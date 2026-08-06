defmodule Ariadne.Flow.ReactorError do
  @moduledoc false
  defexception [:failures]

  @impl Exception
  def message(%__MODULE__{failures: failures}), do: Enum.map_join(failures, "; ", &describe/1)

  defp describe(%{name: name, position: nil, reason: reason}),
    do: "reactor #{inspect(name)} failed: #{format(reason)}"

  defp describe(%{name: name, position: position, reason: reason}),
    do: "reactor #{inspect(name)} failed at position #{position}: #{format(reason)}"

  defp format(%{__exception__: true} = exception), do: Exception.message(exception)
  defp format(reason), do: inspect(reason)
end
