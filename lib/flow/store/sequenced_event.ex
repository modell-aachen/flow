defmodule Ariadne.Flow.Store.SequencedEvent do
  @enforce_keys [:event, :position, :created_at, :metadata]
  defstruct @enforce_keys
end
