defmodule Ariadne.Flow.Store.InMemory do
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.InMemory.State
  alias Ariadne.Flow.Store.StoredEventReactor

  @batch_size 100

  def init(opts \\ []) do
    {:ok, agent} = Agent.start_link(fn -> State.init(opts) end)
    %Store{module: __MODULE__, config: agent}
  end

  def read(agent, query, opts) do
    Agent.get(agent, &State.read(&1, query, opts))
  end

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

  def consume(agent, %StoredEventReactor{} = reactor) do
    Agent.get_and_update(agent, fn state ->
      {new_state, result} = State.consume(state, reactor, @batch_size)
      {result, new_state}
    end)
  end

  def transaction(agent, fun) when is_function(fun, 0) do
    snapshot = Agent.get(agent, & &1)

    try do
      fun.()
    catch
      kind, reason ->
        Agent.update(agent, fn _state -> snapshot end)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  def telemetry_metadata(_agent), do: %{}

  # The store's serialized form is the agent itself, so it round-trips only within
  # the same node — for in-process engines, not for handing a store to another node.
  def dump(agent), do: agent

  def load(agent), do: agent
end
