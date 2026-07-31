defmodule Ariadne.Flow.Store.SequencedEvent do
  alias Ariadne.Flow.Store.Event

  @enforce_keys [:event, :position, :created_at, :metadata]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          event: Event.t(),
          position: pos_integer(),
          created_at: DateTime.t(),
          metadata: map()
        }
end
