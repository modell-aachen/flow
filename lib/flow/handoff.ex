defmodule Ariadne.Flow.Handoff do
  @moduledoc false
  alias Ariadne.Flow.Reactor
  alias Ariadne.Flow.ReactorEngine
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Store

  @enforce_keys [:reactors]
  defstruct [:reactors, engine: nil, metadata: %{}, nested: false]

  def new(%{reactors: reactors} = attrs) do
    %__MODULE__{
      reactors: reactors,
      engine: ReactorEngine.normalize(Map.get(attrs, :engine)),
      metadata: Map.get(attrs, :metadata, %{}),
      nested: Map.get(attrs, :nested, false)
    }
  end

  def hand_off(%__MODULE__{}, %Store{}, []), do: []
  def hand_off(%__MODULE__{reactors: []}, %Store{}, _events), do: []

  def hand_off(%__MODULE__{nested: true} = handoff, %Store{} = store, events) do
    init_checkpoints(handoff, store, events)

    {awaited, deferrable} = Enum.split_with(runs(handoff), &ReactorRun.sync?/1)
    _dropped = unscheduled(deferrable, handoff, store)

    awaited
  end

  def hand_off(%__MODULE__{} = handoff, %Store{} = store, events) do
    init_checkpoints(handoff, store, events)

    handoff
    |> runs()
    |> unscheduled(handoff, store)
  end

  def catch_up(%__MODULE__{reactors: []}, %Store{}), do: []

  def catch_up(%__MODULE__{reactors: reactors} = handoff, %Store{} = store) do
    Store.init_checkpoints(store, declared_checkpoints(reactors))

    reactors
    |> Enum.filter(&checkpointed?(&1, store))
    |> runs(handoff.metadata)
    |> unscheduled(handoff, store)
  end

  def execute(reactor_runs, %Store{} = store) when is_list(reactor_runs) do
    Enum.flat_map(reactor_runs, &report(failures(&1, store)))
  end

  def summarize(failures), do: Enum.map(failures, &Map.take(&1, [:name, :position, :reason]))

  defp failures(reactor_run, store) do
    case ReactorRun.execute(reactor_run, store) do
      :ok ->
        []

      {:error, %ReactorError{failures: failures}} ->
        Enum.map(failures, &Map.merge(&1, %{run: reactor_run, stacktrace: nil}))
    end
  rescue
    exception -> [raised(reactor_run, exception, __STACKTRACE__)]
  end

  defp raised(reactor_run, exception, stacktrace) do
    %{
      run: reactor_run,
      name: ReactorRun.name(reactor_run),
      position: nil,
      reason: exception,
      stacktrace: stacktrace
    }
  end

  defp report(failures) do
    Enum.each(failures, fn %{name: name, position: position, reason: reason} ->
      :telemetry.execute(
        [:ariadne, :flow, :reactor, :failure],
        %{system_time: System.system_time()},
        %{name: name, position: position, reason: reason}
      )
    end)

    failures
  end

  defp unscheduled([], %__MODULE__{}, %Store{}), do: []
  defp unscheduled(reactor_runs, %__MODULE__{engine: nil}, %Store{}), do: reactor_runs

  defp unscheduled(reactor_runs, %__MODULE__{engine: {engine, opts}}, %Store{} = store) do
    scheduled = engine.schedule(reactor_runs, store, opts)

    Enum.reject(reactor_runs, &(&1 in scheduled))
  end

  defp runs(%__MODULE__{reactors: reactors, metadata: metadata}), do: runs(reactors, metadata)

  defp runs(reactors, metadata) do
    Enum.map(reactors, &ReactorRun.new(%{reactor: &1, metadata: metadata}))
  end

  defp init_checkpoints(%__MODULE__{reactors: reactors}, store, events) do
    before_events = lowest_position(events) - 1

    Store.init_checkpoints(store, Enum.map(reactors, &dispatch_checkpoint(&1, before_events)))
  end

  defp dispatch_checkpoint(reactor_module, before_events) do
    reactor = reactor_module.reactor()

    %{name: reactor.name, position: dispatch_start(reactor, before_events)}
  end

  defp declared_checkpoints(reactors) do
    Enum.flat_map(reactors, fn reactor_module ->
      case reactor_module.reactor() do
        %Reactor{start_after_position: :head} ->
          []

        %Reactor{start_after_position: position, name: name} ->
          [%{name: name, position: position}]
      end
    end)
  end

  defp checkpointed?(reactor_module, store) do
    store
    |> Store.checkpoint(reactor_module.reactor().name)
    |> is_integer()
  end

  defp dispatch_start(%Reactor{start_after_position: :head}, before_events), do: before_events
  defp dispatch_start(%Reactor{start_after_position: position}, _before_events), do: position

  defp lowest_position(events) do
    events
    |> Enum.map(& &1.metadata.position)
    |> Enum.min()
  end
end
