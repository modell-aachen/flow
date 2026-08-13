defmodule Ariadne.Flow.Event.Codec do
  alias Ariadne.Flow.Envelope
  alias Ariadne.Flow.Event
  alias Ariadne.Flow.Event.Type
  alias Ariadne.Flow.Store.SequencedRecord

  def deserialize(%SequencedRecord{
        record: record,
        metadata: metadata,
        created_at: created_at,
        position: position
      }) do
    reconstructed_event =
      record.type
      |> Type.module!()
      |> struct()
      |> Event.decode(record.data, metadata)

    enriched_metadata =
      metadata
      |> Map.put(:created_at, created_at)
      |> Map.put(:position, position)

    %Envelope{
      event: reconstructed_event,
      metadata: enriched_metadata,
      type: record.type,
      tags: record.tags
    }
  end
end
