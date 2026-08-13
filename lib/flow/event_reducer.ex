defmodule Ariadne.Flow.EventReducer do
  alias Ariadne.Flow.Envelope
  alias Ariadne.Flow.Event.Codec
  alias Ariadne.Flow.Store

  @callback reduce(reducer :: struct(), events :: [Envelope.t()]) :: any()

  @callback query(reducer :: struct()) :: list()

  def evaluate(%module{} = event_reducer, %Store{} = store) do
    read = Store.read(store, module.query(event_reducer))
    result = module.reduce(event_reducer, Enum.map(read.events, &Codec.deserialize/1))

    %{result: result, read: read}
  end
end
