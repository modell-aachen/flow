defmodule Ariadne.Flow.ConsistencyTimeoutError do
  @moduledoc false
  defexception [:unconfirmed, :timeout]

  @impl Exception
  def message(%__MODULE__{unconfirmed: unconfirmed, timeout: timeout}) do
    "#{describe(unconfirmed)} did not catch up within #{timeout}ms: the events are " <>
      "committed and the runs remain durably scheduled, so they will still be reacted " <>
      "to — never re-dispatch the command"
  end

  defp describe([reactor]), do: "the sync reactor #{summarize(reactor)}"

  defp describe(reactors),
    do: "the sync reactors #{Enum.map_join(reactors, ", ", &summarize/1)}"

  defp summarize(%{name: name, position: position}),
    do: "#{inspect(name)} (awaited at position #{position})"
end
