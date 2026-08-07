defmodule Ariadne.Flow.Store.Event.Codec do
  alias Ariadne.Flow.Envelope
  alias Ariadne.Flow.Store.Event.Encoder
  alias Ariadne.Flow.Store.Event.Type
  alias Ariadne.Flow.Store.SequencedEvent

  def deserialize(%SequencedEvent{
        event: event,
        metadata: metadata,
        created_at: created_at,
        position: position
      }) do
    reconstructed_event =
      event.type
      |> Type.module!()
      |> struct()
      |> Encoder.decode(event.data, metadata)

    enriched_metadata =
      metadata
      |> Map.put(:created_at, created_at)
      |> Map.put(:position, position)

    %Envelope{
      event: reconstructed_event,
      metadata: enriched_metadata,
      type: event.type,
      tags: event.tags
    }
  end
end
