defmodule Ariadne.Flow.Store.InMemory do
  @moduledoc false
  @behaviour Ariadne.Flow.Store.Backend

  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Backend
  alias Ariadne.Flow.Store.InMemory.State
  alias Ariadne.Flow.Store.StoredEventReactor

  @batch_size 100

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

  @impl Backend
  def consume(agent, %StoredEventReactor{} = reactor) do
    Agent.get_and_update(agent, fn state ->
      {new_state, result} = State.consume(state, reactor, @batch_size)
      {result, new_state}
    end)
  end

  @impl Backend
  def checkpoint(agent, name) do
    Agent.get(agent, &State.checkpoint(&1, name))
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
  # the same node — for in-process engines, not for handing a store to another node.
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
        Agent.update(agent, fn _state -> snapshot end)
        :erlang.raise(kind, reason, __STACKTRACE__)
    after
      Process.delete(transaction_key(agent))
    end
  end

  defp transaction_key(agent), do: {__MODULE__, :transaction, agent}
end
