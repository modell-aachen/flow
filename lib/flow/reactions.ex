defmodule Ariadne.Flow.Reactions do
  @moduledoc false
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Store

  def react(_reactors, _store, [], _metadata, _engine), do: :ok
  def react([], _store, _events, _metadata, _engine), do: :ok

  def react(reactors, %Store{} = store, events, metadata, engine) do
    start_after_position = lowest_position(events) - 1
    {engine, opts} = normalize_engine(engine)

    failures =
      Enum.flat_map(reactors, fn reactor_module ->
        reactor_run =
          ReactorRun.new(%{
            reactor: reactor_module,
            start_after_position: start_after_position,
            metadata: metadata
          })

        failures(engine.run(reactor_run, store, opts), reactor_run)
      end)

    if failures == [], do: :ok, else: {:error, %ReactorError{failures: failures}}
  end

  def normalize_engine({module, opts}) when is_atom(module) and is_list(opts), do: {module, opts}
  def normalize_engine(module) when is_atom(module), do: {module, []}

  # The pass runs to the end even after a failure: the dispatch commits either way
  # (Store.transaction commits whenever the closure returns), so stopping early would
  # only withhold the later reactors' runs — and an engine that defers a run enqueues
  # it in this transaction, so a run never handed over is a job never inserted for
  # events that commit regardless.
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
