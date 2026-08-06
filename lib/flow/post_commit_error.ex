defmodule Ariadne.Flow.PostCommitError do
  @moduledoc false
  defexception [:reason, :timeout, failures: [], unconfirmed: [], nested: false]

  def failure(failures, nested \\ false),
    do: %__MODULE__{reason: :failure, failures: failures, nested: nested}

  def timeout(unconfirmed, timeout),
    do: %__MODULE__{reason: :timeout, unconfirmed: unconfirmed, timeout: timeout}

  @impl Exception
  def message(%__MODULE__{reason: :failure, failures: failures} = error) do
    "#{Enum.map_join(failures, "; ", &describe_failure/1)} — #{outcome(error)}"
  end

  def message(%__MODULE__{reason: :timeout, unconfirmed: unconfirmed} = error) do
    "#{describe_unconfirmed(unconfirmed)} did not catch up within #{error.timeout}ms and " <>
      "the runs remain outstanding — #{outcome(error)}"
  end

  defp outcome(%__MODULE__{nested: false}),
    do: "the events are committed, so never re-dispatch the command"

  defp outcome(%__MODULE__{nested: true}),
    do:
      "the events belong to the transaction you opened, so letting this raise propagate " <>
        "undoes them and the command may be dispatched again"

  defp describe_failure(%{name: name, position: nil, reason: reason}),
    do: "sync reactor #{inspect(name)} failed: #{format(reason)}"

  defp describe_failure(%{name: name, position: position, reason: reason}),
    do: "sync reactor #{inspect(name)} failed at position #{position}: #{format(reason)}"

  defp describe_unconfirmed([reactor]), do: "the sync reactor #{summarize(reactor)}"

  defp describe_unconfirmed(reactors),
    do: "the sync reactors #{Enum.map_join(reactors, ", ", &summarize/1)}"

  defp summarize(%{name: name, position: position}),
    do: "#{inspect(name)} (awaited at position #{position})"

  defp format(%{__exception__: true} = exception), do: Exception.message(exception)
  defp format(reason), do: inspect(reason)
end
