defmodule Ariadne.Flow.ReactorError do
  @moduledoc false
  defexception [:failures]

  @impl Exception
  def message(%__MODULE__{failures: failures}), do: Enum.map_join(failures, "; ", &describe/1)

  defp describe(%{name: name, position: nil, reason: reason}),
    do: "reactor #{inspect(name)} failed: #{inspect(reason)}"

  defp describe(%{name: name, position: position, reason: reason}),
    do: "reactor #{inspect(name)} failed at position #{position}: #{inspect(reason)}"
end
