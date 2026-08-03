defmodule Ariadne.Flow.ReactorError do
  @moduledoc false
  defexception [:name, :position, :reason]

  @impl Exception
  def message(%__MODULE__{name: name, position: nil, reason: reason}),
    do: "reactor #{inspect(name)} failed: #{inspect(reason)}"

  def message(%__MODULE__{name: name, position: position, reason: reason}),
    do: "reactor #{inspect(name)} failed at position #{position}: #{inspect(reason)}"
end
