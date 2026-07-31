defmodule Ariadne.Flow.Store.Read do
  @moduledoc """
  What one `Ariadne.Flow.Store.read/3` saw: the events, the normalised query that
  selected them, and the position of the last event seen.

  A read is the unit a dispatch's consistency is built from —
  `Ariadne.Flow.Store.AppendCondition.for_read/1` turns it into the condition that makes
  a write conflict with anything the read missed.
  """
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store.SequencedEvent

  @enforce_keys [:query, :events, :last_position]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          query: Query.t(),
          events: [SequencedEvent.t()],
          last_position: non_neg_integer()
        }

  def new(query, sequenced_events) do
    %__MODULE__{
      query: query,
      events: sequenced_events,
      last_position: last_position(sequenced_events)
    }
  end

  defp last_position(sequenced_events), do: List.last(sequenced_events, %{position: 0}).position
end
