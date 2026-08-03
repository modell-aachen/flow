defmodule Ariadne.Flow.ConsistencyTimeoutError do
  @moduledoc false
  defexception [:reactors, :position, :timeout]

  @impl Exception
  def message(%__MODULE__{reactors: reactors, position: position, timeout: timeout}) do
    "#{describe(reactors)} did not reach position #{position} within #{timeout}ms: " <>
      "the events are committed and the runs remain durably scheduled, so they will " <>
      "still be reacted to — never re-dispatch the command"
  end

  defp describe([name]), do: "the sync reactor #{inspect(name)}"

  defp describe(names),
    do: "the sync reactors #{Enum.map_join(names, ", ", &inspect/1)}"
end
