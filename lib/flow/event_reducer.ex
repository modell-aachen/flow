defmodule Ariadne.Flow.EventReducer do
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Event.Codec

  @callback reduce(reducer :: struct(), events :: list()) :: any()

  @callback query(reducer :: struct()) :: list()

  def evaluate(%module{} = event_reducer, %Store{} = store) do
    query =
      event_reducer
      |> module.query()
      |> Codec.serialize_query_items()

    %{events: sequenced_events} = Store.read(store, query)
    result = module.reduce(event_reducer, Enum.map(sequenced_events, &Codec.deserialize/1))

    %{result: result, query: query, events: sequenced_events}
  end
end
