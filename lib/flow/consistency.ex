defmodule Ariadne.Flow.Consistency do
  @moduledoc false
  alias Ariadne.Flow.ConsistencyTimeoutError
  alias Ariadne.Flow.Reactor
  alias Ariadne.Flow.Store

  @default_timeout 5_000
  @poll_interval 10

  @enforce_keys [:reactors, :timeout]
  defstruct @enforce_keys

  @type t :: %__MODULE__{reactors: [%Reactor{}], timeout: non_neg_integer()}

  @doc """
  What a dispatch has to confirm before it returns: the sync reactors among `reactors`,
  and how long to wait for them.

  The reactors are kept as their own declarations rather than as names, because a name
  alone does not say how far that reactor has to get: its filter is what decides which of
  the dispatch's events it will ever process, and so what its checkpoint has to reach.

  A dispatch nested inside an outer transaction confirms nothing. Its events and any job
  row the engine wrote are invisible until the outer commit, so no checkpoint could
  advance while it waits — the engine executes its sync runs inline instead
  (`Ariadne.Flow.ReactorRun.inline?/1`), where the confirmation comes with the execution.

  The wait is bounded by the `:await_timeout` option, in milliseconds.
  """
  def new(reactors, nested, opts) when is_boolean(nested) and is_list(opts) do
    %__MODULE__{
      reactors: awaited(reactors, nested),
      timeout: Keyword.get(opts, :await_timeout, @default_timeout)
    }
  end

  @doc """
  Waits until every awaited reactor has checkpointed at or past the last of `events` it
  will process.

  The target is per reactor, not one position for the whole dispatch: a checkpoint records
  the last *matching* event a reactor consumed and stays put on events its filter skips, so
  a reactor is caught up once its checkpoint reaches the highest appended position matching
  its filter. A reactor matching none of the dispatch's events has nothing to catch up to
  and is not waited on at all.

  Confirmation is the checkpoint, not the state of whatever the engine enqueued: under the
  per-reactor consumption lock a concurrent dispatch's run may be the one that processes
  these events, leaving this dispatch's own run to no-op. The checkpoint is store-observable
  and engine-agnostic — an engine that executed the run inline has already advanced it, so
  the first check confirms and nothing is waited on.

  Returns `{:error, %Ariadne.Flow.ConsistencyTimeoutError{}}` when the timeout runs out
  first. That is not a failure of the dispatch: the events are committed and the runs stay
  scheduled. It is the caller's read-your-writes expectation that went unmet.
  """
  def await(%__MODULE__{reactors: []}, %Store{}, _events), do: :ok

  def await(%__MODULE__{}, %Store{}, []), do: :ok

  def await(%__MODULE__{reactors: reactors, timeout: timeout}, %Store{} = store, events) do
    case Enum.flat_map(reactors, &target(&1, events)) do
      [] -> :ok
      targets -> poll(store, targets, timeout, now() + timeout)
    end
  end

  # The last event of this dispatch the reactor is going to process, as the name its
  # checkpoint is keyed on and the position that checkpoint has to reach.
  defp target(%Reactor{name: name} = reactor, events) do
    case Enum.filter(events, &Reactor.matches?(reactor, &1)) do
      [] -> []
      matching -> [%{name: name, position: highest_position(matching)}]
    end
  end

  defp poll(store, targets, timeout, deadline) do
    pending = Enum.reject(targets, &confirmed?(store, &1))

    cond do
      pending == [] ->
        :ok

      now() >= deadline ->
        {:error, ConsistencyTimeoutError.exception(unconfirmed: pending, timeout: timeout)}

      true ->
        Process.sleep(@poll_interval)
        poll(store, targets, timeout, deadline)
    end
  end

  defp confirmed?(store, %{name: name, position: position}) do
    case Store.checkpoint(store, name) do
      nil -> false
      checkpoint -> checkpoint >= position
    end
  end

  defp awaited(_reactors, true), do: []

  defp awaited(reactors, false) do
    Enum.flat_map(reactors, fn reactor_module ->
      case reactor_module.reactor() do
        %Reactor{sync: true} = reactor -> [reactor]
        %Reactor{} -> []
      end
    end)
  end

  defp highest_position(events) do
    events
    |> Enum.map(& &1.metadata.position)
    |> Enum.max()
  end

  defp now, do: System.monotonic_time(:millisecond)
end
