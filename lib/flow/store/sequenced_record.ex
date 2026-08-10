defmodule Ariadne.Flow.Store.SequencedRecord do
  alias Ariadne.Flow.Store.Record

  @enforce_keys [:record, :position, :created_at, :metadata]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          record: Record.t(),
          position: pos_integer(),
          created_at: DateTime.t(),
          metadata: map()
        }
end
