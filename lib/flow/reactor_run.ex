defmodule Ariadne.Flow.ReactorRun do
  @moduledoc false
  alias Ariadne.Flow.ConsumeResult
  alias Ariadne.Flow.Reactor
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Event.Codec
  alias Ariadne.Flow.Store.StoredEventReactor

  @enforce_keys [:reactor, :start_after_position]
  defstruct [:reactor, :start_after_position, metadata: %{}, nested: false]

  @start_hint ":head is a declaration, not a position — the run builder resolves it against what it knows is now, for reactor "

  def new(%{reactor: reactor_module} = attrs) do
    reactor = reactor_module.reactor()

    %__MODULE__{
      reactor: reactor_module,
      start_after_position:
        position(Map.get(attrs, :start_after_position, reactor.start_after_position), reactor),
      metadata: Map.get(attrs, :metadata, %{}),
      nested: Map.get(attrs, :nested, false)
    }
  end

  # "The dispatch will not return before this run's checkpoint has passed the events it
  # appended" — an intent about the dispatch, not about where the run executes. An engine
  # may defer a sync run onto its job system, as long as the run is durably enqueued in
  # the dispatch's transaction: the wait is on the checkpoint, which the job advances.
  def sync?(%__MODULE__{reactor: reactor_module}), do: reactor_module.reactor().sync

  # A sync run in a dispatch nested inside an outer transaction is the one that cannot be
  # deferred: its events and the engine's job row stay invisible until the outer commit, so
  # nothing could advance the checkpoint while the dispatch waits on it. Executing such a
  # run inline is the only place its confirmation can come from.
  def inline?(%__MODULE__{nested: nested} = reactor_run), do: nested and sync?(reactor_run)

  def name(%__MODULE__{reactor: reactor_module}), do: reactor_module.reactor().name

  # The nesting flag is deliberately not dumped: a run being serialised is on its way to a
  # job system, which executes it outside the dispatch's transaction whatever that dispatch
  # was nested in.
  def dump(%__MODULE__{reactor: reactor, start_after_position: position, metadata: metadata}) do
    %{
      "reactor" => Atom.to_string(reactor),
      "start_after_position" => position,
      "metadata" => metadata
    }
  end

  def load(%{"reactor" => reactor, "start_after_position" => position} = payload) do
    new(%{
      reactor: String.to_existing_atom(reactor),
      start_after_position: position,
      metadata: Map.get(payload, "metadata", %{})
    })
  end

  def execute(
        %__MODULE__{reactor: reactor_module, start_after_position: position},
        %Store{} = store
      ) do
    reactor = reactor_module.reactor()

    stored_event_reactor =
      StoredEventReactor.new(%{
        name: reactor.name,
        query: Reactor.query(reactor),
        handler: fn events -> run_handler(reactor, events) end,
        start_after_position: position
      })

    catch_up(store, stored_event_reactor)
  end

  defp position(position, _reactor) when is_integer(position) and position >= 0, do: position

  defp position(:head, %Reactor{name: name}),
    do: raise(ArgumentError, @start_hint <> inspect(name))

  defp catch_up(store, %StoredEventReactor{name: name} = stored_event_reactor) do
    case Store.consume(store, stored_event_reactor) do
      %ConsumeResult{status: :error, last_position: position, failure: %{reason: reason}} ->
        {:error, %ReactorError{failures: [%{name: name, position: position, reason: reason}]}}

      %ConsumeResult{more?: true} ->
        catch_up(store, stored_event_reactor)

      %ConsumeResult{} ->
        :ok
    end
  end

  defp run_handler(reactor, events) do
    Enum.reduce_while(events, {:ok, 0}, fn seq, {:ok, count} ->
      case Reactor.handle(reactor, Codec.deserialize(seq)) do
        :ok ->
          {:cont, {:ok, count + 1}}

        {:error, reason} ->
          {:halt, {:error, count, %{event: seq, reason: reason}}}
      end
    end)
  end
end
