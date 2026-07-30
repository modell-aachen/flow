defmodule Ariadne.Flow.Store.Event.Codec do
  alias Ariadne.Flow.Store.Event.Encoder
  alias Ariadne.Flow.Store.SequencedEvent

  def serialize_type(type) when is_atom(type) do
    type
    |> Atom.to_string()
    |> String.trim_leading("Elixir.")
  end

  def deserialize_type(type) when is_binary(type) do
    String.to_existing_atom("Elixir." <> type)
  end

  def serialize_query_items(query_items) when is_list(query_items) do
    Enum.map(query_items, fn %{types: types} = item ->
      %{item | types: Enum.map(types, &serialize_type/1)}
    end)
  end

  def deserialize(%SequencedEvent{
        event: event,
        metadata: metadata,
        created_at: created_at,
        position: position
      }) do
    reconstructed_event =
      event.type
      |> deserialize_type()
      |> struct()
      |> Encoder.decode(event.data, metadata)

    enriched_metadata =
      metadata
      |> Map.put(:created_at, created_at)
      |> Map.put(:position, position)

    %{event: reconstructed_event, metadata: enriched_metadata}
  end
end
