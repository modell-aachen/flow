defmodule Ariadne.Flow.AfterCommit do
  @moduledoc false
  @enforce_keys [:callback]
  defstruct [:callback]

  def new(callback) when is_function(callback, 1), do: %__MODULE__{callback: callback}

  # The callback receives the struct back so later fields (target position,
  # deadline) can be bound onto it without changing its arity.
  def run(%__MODULE__{callback: callback} = after_commit), do: callback.(after_commit)
end
