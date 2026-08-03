defmodule Ariadne.Flow.Application do
  @moduledoc false
  alias Ariadne.Flow.CommandHandler
  alias Ariadne.Flow.EventReducer
  alias Ariadne.Flow.Reactions
  alias Ariadne.Flow.ReactorEngine
  alias Ariadne.Flow.Store

  defstruct [:store, reactors: [], engine: {ReactorEngine.Inline, []}]

  def new(%{store: %Store{} = store} = attrs) do
    %__MODULE__{
      store: store,
      reactors: Map.get(attrs, :reactors, []),
      engine: Reactions.normalize_engine(Map.get(attrs, :engine, ReactorEngine.Inline))
    }
  end

  def dispatch(%__MODULE__{store: store, reactors: reactors, engine: engine}, command, opts \\ []) do
    metadata = Keyword.get(opts, :metadata, %{})

    Store.transaction(store, fn ->
      with {:ok, %{events: events} = result} <- CommandHandler.handle(command, store, opts),
           :ok <- Reactions.react(reactors, store, events, metadata, engine) do
        {:ok, result}
      end
    end)
  end

  def dispatch!(%__MODULE__{} = application, command, opts \\ []) do
    case dispatch(application, command, opts) do
      {:error, {:reactor_failed, %{name: name, position: position, reason: reason}}} ->
        raise "reactor #{inspect(name)} failed at position #{position}: #{inspect(reason)}"

      result ->
        result
    end
  end

  def query(%__MODULE__{store: store}, event_reducer) do
    EventReducer.evaluate(event_reducer, store).result
  end
end
