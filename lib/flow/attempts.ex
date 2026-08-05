defmodule Ariadne.Flow.Attempts do
  @moduledoc false
  alias Ariadne.Flow.AppendConditionError

  @default_limit 3

  @enforce_keys [:limit]
  defstruct @enforce_keys

  @type t :: %__MODULE__{limit: pos_integer()}

  def new(attrs \\ %{}) do
    limit = validated_limit(Map.get(attrs, :attempts))

    %__MODULE__{limit: limit(limit, Map.get(attrs, :nested, false))}
  end

  def run(%__MODULE__{limit: limit}, fun) when is_function(fun, 0), do: attempt(fun, limit, 1)

  defp attempt(fun, limit, attempt) do
    case fun.() do
      {:error, %AppendConditionError{}} when attempt < limit -> attempt(fun, limit, attempt + 1)
      result -> {result, attempt}
    end
  end

  defp limit(_limit, true), do: 1
  defp limit(limit, false), do: limit

  defp validated_limit(nil), do: @default_limit
  defp validated_limit(limit) when is_integer(limit) and limit >= 1, do: limit

  defp validated_limit(limit) do
    raise ArgumentError, ":attempts must be a positive integer, got: #{inspect(limit)}"
  end
end
