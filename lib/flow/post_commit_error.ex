defmodule Ariadne.Flow.PostCommitError do
  @moduledoc false
  defexception [:reason, :timeout, failures: [], unconfirmed: []]

  @committed "the events are committed, so never re-dispatch the command"

  def failure(failures), do: %__MODULE__{reason: :failure, failures: failures}

  def timeout(unconfirmed, timeout),
    do: %__MODULE__{reason: :timeout, unconfirmed: unconfirmed, timeout: timeout}

  @impl Exception
  def message(%__MODULE__{reason: :failure, failures: failures}) do
    "#{Enum.map_join(failures, "; ", &describe_failure/1)} — #{@committed}"
  end

  def message(%__MODULE__{reason: :timeout, unconfirmed: unconfirmed, timeout: timeout}) do
    "#{describe_unconfirmed(unconfirmed)} did not catch up within #{timeout}ms and " <>
      "the runs remain outstanding — #{@committed}"
  end

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
