defmodule Ariadne.Flow.EventReducer do
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Event.Codec

  @callback reduce(reducer :: struct(), events :: list()) :: any()

  @callback query(reducer :: struct()) :: list()

  def evaluate(event_reducer, %Store{} = store) do
    {_query, sequenced_events} = read(event_reducer, store)

    fold(event_reducer, sequenced_events)
  end

  def read(%module{} = event_reducer, %Store{} = store) do
    query =
      event_reducer
      |> module.query()
      |> Codec.serialize_query_items()

    %{events: sequenced_events} = Store.read(store, query)

    {query, sequenced_events}
  end

  def fold(%module{} = event_reducer, sequenced_events) do
    module.reduce(event_reducer, Enum.map(sequenced_events, &Codec.deserialize/1))
  end
end
