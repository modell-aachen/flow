defmodule Ariadne.Flow.Consistency do
  @moduledoc false
  alias Ariadne.Flow.ConsistencyTimeoutError
  alias Ariadne.Flow.Reactor
  alias Ariadne.Flow.Store

  @default_timeout 5_000
  # Doubling from a couple of milliseconds keeps a confirmation that arrives quickly — the
  # case sync reactors exist for — cheap to notice, while the cap bounds how long a
  # dispatch keeps waiting after its reactor has actually caught up. Waiting out the
  # default timeout costs tens of rounds of reads rather than the five hundred a fixed
  # short interval would, and the extra latency lands only on a dispatch already hundreds
  # of milliseconds into a wait it is about to give up on.
  @first_interval 2
  @max_interval 100

  @enforce_keys [:reactors, :timeout]
  defstruct @enforce_keys

  @type t :: %__MODULE__{reactors: [%Reactor{}], timeout: non_neg_integer()}

  @doc """
  What a dispatch has to confirm before it returns: the sync reactors among `reactors`,
  and how long to wait for them.

  The reactors are kept as their own declarations rather than as names, because a name
  alone does not say how far that reactor has to get: its filter is what decides which of
  the dispatch's events it will ever process, and so what its checkpoint has to reach.

  `dispatch` carries the facts about the dispatch that decide this, `nested` being the one
  that matters here. A dispatch nested inside an outer transaction confirms nothing: its
  events and any job row the engine wrote are invisible until the outer commit, so no
  checkpoint could advance while it waits. The awaited set is therefore the complement of
  `Ariadne.Flow.ReactorRun.inline?/1` — what the engine is not told to run inline is what
  Flow waits for, and what it is told to run inline needs no waiting, the confirmation
  coming with the execution.

  The wait is bounded by the `:await_timeout` option, in milliseconds.
  """
  def new(reactors, dispatch, opts) when is_map(dispatch) and is_list(opts) do
    %__MODULE__{
      reactors: awaited(reactors, Map.get(dispatch, :nested, false)),
      timeout: validated_timeout(Keyword.get(opts, :await_timeout, @default_timeout))
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

  The wait is reported as a `[:ariadne, :flow, :dispatch, :await]` span, measuring how long
  callers block on their sync reactors and how many rounds of checkpoint reads that took.
  """
  def await(%__MODULE__{reactors: []}, %Store{}, _events), do: :ok

  def await(%__MODULE__{}, %Store{}, []), do: :ok

  def await(%__MODULE__{reactors: reactors, timeout: timeout}, %Store{} = store, events) do
    case Enum.flat_map(reactors, &target(&1, events)) do
      [] -> :ok
      targets -> await_targets(store, targets, timeout)
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

  defp await_targets(store, targets, timeout) do
    metadata = %{awaited: targets, timeout: timeout}

    :telemetry.span([:ariadne, :flow, :dispatch, :await], metadata, fn ->
      wait = %{deadline: now() + timeout, interval: @first_interval, polls: 0}

      store
      |> poll(targets, wait)
      |> report(metadata)
    end)
  end

  # Polls the targets not yet confirmed, backing off between rounds. Recursing on `pending`
  # rather than on every target is safe because a checkpoint never moves backwards, and it
  # is what keeps a slow reactor from being paid for by queries about its caught-up peers:
  # a dispatch that has to wait out the default timeout costs a dozen or so rounds, not the
  # hundreds a fixed short interval would.
  defp poll(store, targets, %{deadline: deadline, interval: interval} = wait) do
    pending = Enum.reject(targets, &confirmed?(store, &1))
    wait = %{wait | polls: wait.polls + 1}

    cond do
      pending == [] ->
        {:ok, wait}

      now() >= deadline ->
        {:timeout, pending, wait}

      true ->
        # Never sleeping past the deadline keeps the wait within the timeout the error
        # reports, up to the round of reads that discovers it has run out.
        Process.sleep(min(interval, max(deadline - now(), 0)))
        poll(store, pending, %{wait | interval: min(interval * 2, @max_interval)})
    end
  end

  defp report({:ok, %{polls: polls}}, metadata) do
    {:ok, %{polls: polls}, Map.put(metadata, :result, :confirmed)}
  end

  defp report({:timeout, pending, %{polls: polls}}, %{timeout: timeout} = metadata) do
    error = ConsistencyTimeoutError.exception(unconfirmed: pending, timeout: timeout)

    {{:error, error}, %{polls: polls},
     Map.merge(metadata, %{result: :timeout, unconfirmed: pending})}
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

  # Checked here, before the dispatch opens its transaction, so a bad option fails with
  # nothing written: raising at the wait instead would raise *after* the commit, which is
  # the one thing this contract reserves for outcomes the caller must not retry.
  # `:infinity` is rejected rather than supported — a dispatch that may never return is not
  # a guarantee a caller can do anything with.
  defp validated_timeout(timeout) when is_integer(timeout) and timeout >= 0, do: timeout

  defp validated_timeout(timeout) do
    raise ArgumentError,
          ":await_timeout must be a non-negative number of milliseconds, got: #{inspect(timeout)}"
  end

  defp highest_position(events) do
    events
    |> Enum.map(& &1.metadata.position)
    |> Enum.max()
  end

  defp now, do: System.monotonic_time(:millisecond)
end
