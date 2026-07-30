defmodule Ariadne.Flow.Store.Event do
  @enforce_keys [:type, :data, :tags]
  defstruct @enforce_keys
end
