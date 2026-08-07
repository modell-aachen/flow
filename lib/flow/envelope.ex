defmodule Ariadne.Flow.Envelope do
  @enforce_keys [:event, :metadata, :type, :tags]
  defstruct @enforce_keys

  @typedoc "A deserialised event: the decoded struct and its metadata, in the stored form filters match on."
  @type t :: %__MODULE__{
          event: struct(),
          metadata: map(),
          type: String.t(),
          tags: [String.t()]
        }
end
