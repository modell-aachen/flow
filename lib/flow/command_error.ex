defmodule Ariadne.Flow.CommandError do
  @moduledoc false
  defexception [:reason]

  @impl Exception
  def message(%__MODULE__{reason: reason}), do: "the command was refused: #{inspect(reason)}"
end
