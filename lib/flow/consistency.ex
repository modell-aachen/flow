defmodule Ariadne.Flow.Consistency do
  @moduledoc false
  alias Ariadne.Flow.ConsistencyTimeoutError
  alias Ariadne.Flow.Reactor
  alias Ariadne.Flow.Store

  @default_timeout 5_000
  @poll_interval 10

  @enforce_keys [:reactors, :timeout]
  defstruct @enforce_keys

  @type t :: %__MODULE__{reactors: [String.t()], timeout: non_neg_integer()}

  @doc """
  What a dispatch has to confirm before it returns: the names of the sync reactors among
  `reactors`, and how long to wait for them.

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
  Waits until every awaited reactor has checkpointed at or past the highest position of
  `events`.

  Confirmation is the checkpoint, not the state of whatever the engine enqueued: under
  the per-reactor consumption lock a concurrent dispatch's run may be the one that
  processes these events, leaving this dispatch's own run to no-op. The checkpoint is
  store-observable and engine-agnostic — an engine that executed the run inline has
  already advanced it, so the first check confirms and nothing is waited on.

  Returns `{:error, %Ariadne.Flow.ConsistencyTimeoutError{}}` when the timeout runs out
  first. That is not a failure of the dispatch: the events are committed and the runs
  stay scheduled. It is the caller's read-your-writes expectation that went unmet.
  """
  def await(%__MODULE__{reactors: []}, %Store{}, _events), do: :ok

  def await(%__MODULE__{}, %Store{}, []), do: :ok

  def await(%__MODULE__{timeout: timeout} = consistency, %Store{} = store, events) do
    poll(consistency, store, highest_position(events), now() + timeout)
  end

  defp poll(%__MODULE__{reactors: reactors} = consistency, store, position, deadline) do
    pending = Enum.reject(reactors, &confirmed?(store, &1, position))

    cond do
      pending == [] ->
        :ok

      now() >= deadline ->
        {:error, timed_out(consistency, pending, position)}

      true ->
        Process.sleep(@poll_interval)
        poll(consistency, store, position, deadline)
    end
  end

  defp confirmed?(store, name, position) do
    case Store.checkpoint(store, name) do
      nil -> false
      checkpoint -> checkpoint >= position
    end
  end

  defp timed_out(%__MODULE__{timeout: timeout}, pending, position) do
    ConsistencyTimeoutError.exception(reactors: pending, position: position, timeout: timeout)
  end

  defp awaited(_reactors, true), do: []

  defp awaited(reactors, false) do
    Enum.flat_map(reactors, fn reactor_module ->
      case reactor_module.reactor() do
        %Reactor{sync: true, name: name} -> [name]
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
