defmodule Ariadne.Flow.ConsumeResult do
  @enforce_keys [:status, :processed, :last_position, :more?]
  defstruct [:status, :processed, :last_position, :more?, failure: nil, duration_ms: 0]
end
