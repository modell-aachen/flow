defmodule Ariadne.Flow.Handoff do
  @moduledoc false
  alias Ariadne.Flow.Reactor
  alias Ariadne.Flow.ReactorEngine
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Store

  @enforce_keys [:reactors, :engine]
  defstruct [:reactors, :engine, metadata: %{}, nested: false]

  def new(%{reactors: reactors, engine: engine} = attrs) do
    %__MODULE__{
      reactors: reactors,
      engine: ReactorEngine.normalize(engine),
      metadata: Map.get(attrs, :metadata, %{}),
      nested: Map.get(attrs, :nested, false)
    }
  end

  def hand_off(%__MODULE__{}, %Store{}, []), do: :ok
  def hand_off(%__MODULE__{reactors: []}, %Store{}, _events), do: :ok

  def hand_off(%__MODULE__{} = handoff, %Store{} = store, events) do
    before_events = lowest_position(events) - 1

    handoff
    |> runs(&dispatch_start(&1, before_events))
    |> drive(handoff, store)
  end

  def catch_up(%__MODULE__{reactors: []}, %Store{}), do: :ok

  def catch_up(%__MODULE__{} = handoff, %Store{} = store) do
    handoff
    |> runs(&catch_up_start(&1, store))
    |> drive(handoff, store)
  end

  defp runs(%__MODULE__{reactors: reactors, metadata: metadata, nested: nested}, start) do
    Enum.flat_map(reactors, fn reactor_module ->
      case start.(reactor_module.reactor()) do
        :none ->
          []

        position ->
          [
            ReactorRun.new(%{
              reactor: reactor_module,
              start_after_position: position,
              metadata: metadata,
              nested: nested
            })
          ]
      end
    end)
  end

  defp drive(reactor_runs, %__MODULE__{engine: {engine, opts}}, store) do
    failures = Enum.flat_map(reactor_runs, &failures(engine.run(&1, store, opts), &1))

    if failures == [], do: :ok, else: {:error, %ReactorError{failures: failures}}
  end

  defp dispatch_start(%Reactor{start_after_position: :head}, before_events), do: before_events
  defp dispatch_start(%Reactor{start_after_position: position}, _before_events), do: position

  defp catch_up_start(%Reactor{start_after_position: :head, name: name}, store),
    do: Store.checkpoint(store, name) || :none

  defp catch_up_start(%Reactor{start_after_position: position}, _store), do: position

  defp failures(:ok, _reactor_run), do: []
  defp failures({:error, %ReactorError{failures: failures}}, _reactor_run), do: failures

  defp failures({:error, reason}, reactor_run),
    do: [%{name: ReactorRun.name(reactor_run), position: nil, reason: reason}]

  defp lowest_position(events) do
    events
    |> Enum.map(& &1.metadata.position)
    |> Enum.min()
  end
end
