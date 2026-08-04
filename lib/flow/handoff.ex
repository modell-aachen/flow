defmodule Ariadne.Flow.Handoff do
  @moduledoc false
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
    %{reactors: reactors, engine: {engine, opts}, metadata: metadata, nested: nested} = handoff
    start_after_position = lowest_position(events) - 1

    failures =
      Enum.flat_map(reactors, fn reactor_module ->
        reactor_run =
          ReactorRun.new(%{
            reactor: reactor_module,
            start_after_position: start_after_position,
            metadata: metadata,
            nested: nested
          })

        failures(engine.run(reactor_run, store, opts), reactor_run)
      end)

    if failures == [], do: :ok, else: {:error, %ReactorError{failures: failures}}
  end

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
