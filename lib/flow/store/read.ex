defmodule Ariadne.Flow.Store.Read do
  @moduledoc false
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store.SequencedRecord

  @enforce_keys [:query, :events, :last_position]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          query: Query.t(),
          events: [SequencedRecord.t()],
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
