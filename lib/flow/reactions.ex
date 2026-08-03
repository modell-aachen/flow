defmodule Ariadne.Flow.Reactions do
  @moduledoc false
  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Store

  def react(_reactors, _store, [], _metadata, _engine), do: :ok
  def react([], _store, _events, _metadata, _engine), do: :ok

  def react(reactors, %Store{} = store, events, metadata, engine) do
    start_after_position = lowest_position(events) - 1
    {engine, opts} = normalize_engine(engine)

    Enum.reduce_while(reactors, :ok, fn reactor_module, :ok ->
      reactor_run =
        ReactorRun.new(%{
          reactor: reactor_module,
          start_after_position: start_after_position,
          metadata: metadata
        })

      case engine.run(reactor_run, store, opts) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  def normalize_engine({module, opts}) when is_atom(module) and is_list(opts), do: {module, opts}
  def normalize_engine(module) when is_atom(module), do: {module, []}

  defp lowest_position(events) do
    events
    |> Enum.map(& &1.metadata.position)
    |> Enum.min()
  end
end
