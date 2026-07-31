defmodule Ariadne.Flow.Store.Event do
  @enforce_keys [:type, :data, :tags]
  defstruct @enforce_keys

  @type t :: %__MODULE__{type: String.t(), data: map(), tags: [String.t()]}
end
