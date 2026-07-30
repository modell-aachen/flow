defmodule Ariadne.Flow.Store.Event.Encoder.Default do
  def encode(%module{} = event) do
    data =
      event
      |> Map.from_struct()
      |> Enum.into(%{}, fn {key, value} ->
        ensure_primitive!(module, key, value)
        {Atom.to_string(key), value}
      end)

    %{
      data: data,
      tags: module.tags(event)
    }
  end

  def decode(%module{}, %{} = store_data, _metadata) do
    data =
      for {key, value} <- store_data, into: %{}, do: {String.to_existing_atom(key), value}

    struct!(module, data)
  end

  defp ensure_primitive!(module, key, value) do
    unless primitive?(value) do
      raise ArgumentError, """
      #{inspect(module)} field #{inspect(key)} holds a value the default encoder \
      cannot store without changing its type: #{inspect(value)}.

      The default encoder only supports JSON-primitive values (strings, numbers, \
      booleans, nil, and lists or string-keyed maps of those). To store events \
      with DateTime, Date, atom, tuple, or struct fields, provide a custom \
      encoder that round-trips them:

          @derive {Ariadne.Flow.Store.Event.Encoder, to: MyEncoder}
      """
    end
  end

  defp primitive?(value) when is_binary(value), do: String.valid?(value)
  defp primitive?(value) when is_number(value), do: true
  defp primitive?(value) when is_boolean(value), do: true
  defp primitive?(nil), do: true
  defp primitive?(value) when is_list(value), do: Enum.all?(value, &primitive?/1)

  defp primitive?(value) when is_map(value) and not is_struct(value) do
    Enum.all?(value, fn {key, value} ->
      is_binary(key) and String.valid?(key) and primitive?(value)
    end)
  end

  defp primitive?(_value), do: false
end
