defmodule Ariadne.Flow.AppendConditionError do
  @moduledoc false
  defexception []

  @impl Exception
  def message(%__MODULE__{}) do
    "the append condition failed: events matching the command's query were appended " <>
      "after its read; re-dispatching decides from the new events"
  end
end
