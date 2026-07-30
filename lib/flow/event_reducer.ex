defmodule Ariadne.Flow.EventReducer do
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Event.Codec

  @callback reduce(reducer :: struct(), events :: list()) :: any()

  @callback query(reducer :: struct()) :: list()

  def evaluate(%module{} = event_reducer, %Store{} = store) do
    store_query =
      event_reducer
      |> module.query()
      |> Codec.serialize_query_items()

    %{events: sequence_events, append_condition: append_condition} =
      Store.read(store, store_query)

    stored_events = Enum.map(sequence_events, &Codec.deserialize/1)
    result = module.reduce(event_reducer, stored_events)

    {result, append_condition}
  end
end
