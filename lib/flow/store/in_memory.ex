defmodule Ariadne.Flow.Store.InMemory do
  @moduledoc false
  @behaviour Ariadne.Flow.Store.Backend

  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Backend
  alias Ariadne.Flow.Store.Consumption
  alias Ariadne.Flow.Store.InMemory.State
  alias Ariadne.Flow.Store.StoredEventReactor

  @batch_size 100
  @unlocked :"$ariadne_flow_consume_unlocked"

  @doc """
  Starts an empty store, linked to the calling process. It takes no options.
  """
  @impl Backend
  def init(opts \\ []) do
    {:ok, agent} = Agent.start_link(fn -> State.init(opts) end)
    %Store{module: __MODULE__, config: agent}
  end

  @impl Backend
  def read(agent, query, opts) do
    Agent.get(agent, &State.read(&1, query, opts))
  end

  @impl Backend
  def append(agent, events, opts) do
    Agent.get_and_update(agent, fn state ->
      case State.append(state, events, opts) do
        {:ok, new_state, appended_events} ->
          {{:ok, %{events: appended_events}}, new_state}

        {:error, reason} ->
          {{:error, reason}, state}
      end
    end)
  end

  @impl Backend
  def count(agent) do
    Agent.get(agent, &State.count/1)
  end

  # The handler runs in the calling process rather than in the agent callback, so it can
  # reach the store it is consuming from — read it, append to it, dispatch into it. The
  # per-reactor lock around it is the in-memory equivalent of the advisory lock the
  # Postgres backend holds for the length of its consume transaction.
  @impl Backend
  def consume(agent, %StoredEventReactor{name: name, handler: handler} = reactor) do
    holding_consume_lock(agent, name, fn ->
      transaction(agent, fn ->
        {prior_position, events} =
          Agent.get(agent, &State.unconsumed(&1, reactor, @batch_size + 1))

        {batch, more_in_store?} = Consumption.split(events, @batch_size)
        {result, new_position} = Consumption.run(batch, prior_position, more_in_store?, handler)
        Agent.update(agent, &State.put_checkpoint(&1, name, new_position))

        result
      end)
    end)
  end

  @impl Backend
  def checkpoint(agent, name) do
    Agent.get(agent, &State.checkpoint(&1, name))
  end

  @impl Backend
  def init_checkpoints(agent, checkpoints) do
    Agent.update(agent, &State.init_checkpoints(&1, checkpoints))
  end

  @impl Backend
  def transaction(agent, fun) when is_function(fun, 0) do
    if in_transaction?(agent), do: fun.(), else: rolling_back_on_raise(agent, fun)
  end

  # The transaction is the calling process's, so whether one is open is process-local
  # state — the agent holds the events, not who is currently writing them.
  @impl Backend
  def in_transaction?(agent), do: Process.get(transaction_key(agent), false)

  @impl Backend
  def telemetry_metadata(_agent), do: %{}

  # The store's serialized form is the agent itself, so it round-trips only within
  # the same node — for in-process schedulers, not for handing a store to another node.
  @impl Backend
  def dump(agent), do: agent

  @impl Backend
  def load(agent), do: agent

  defp rolling_back_on_raise(agent, fun) do
    snapshot = Agent.get(agent, & &1)
    Process.put(transaction_key(agent), true)

    try do
      fun.()
    catch
      kind, reason ->
        Agent.update(agent, &State.restore(snapshot, &1))
        :erlang.raise(kind, reason, __STACKTRACE__)
    after
      Process.delete(transaction_key(agent))
    end
  end

  # Held per reactor name, and re-entered rather than waited on by the process already
  # holding it — a handler consuming its own reactor again would otherwise wait forever.
  defp holding_consume_lock(agent, name, fun) do
    key = consume_lock_key(agent, name)

    if Process.get(key, false) do
      fun.()
    else
      lock_consume(agent, name)
      Process.put(key, true)

      try do
        fun.()
      after
        Process.delete(key)
        unlock_consume(agent, name)
      end
    end
  end

  defp lock_consume(agent, name) do
    # The callback runs in the agent, so the process taking the lock has to be named here.
    caller = self()

    case Agent.get_and_update(agent, &State.lock_consume(&1, name, caller)) do
      :ok ->
        :ok

      {:wait, holder} ->
        await_unlock(agent, name, Process.monitor(holder))
        lock_consume(agent, name)
    end
  end

  defp await_unlock(agent, name, monitor_ref) do
    receive do
      {@unlocked, ^agent, ^name} -> Process.demonitor(monitor_ref, [:flush])
      {:DOWN, ^monitor_ref, :process, _holder, _reason} -> :ok
    end
  end

  defp unlock_consume(agent, name) do
    agent
    |> Agent.get_and_update(&State.unlock_consume(&1, name))
    |> Enum.each(&send(&1, {@unlocked, agent, name}))
  end

  defp consume_lock_key(agent, name), do: {__MODULE__, :consume, agent, name}

  defp transaction_key(agent), do: {__MODULE__, :transaction, agent}
end
