defprotocol Ariadne.Flow.Store.Event.Encoder do
  defmacro __deriving__(module, options) do
    target = Keyword.get(options, :to, Ariadne.Flow.Store.Event.Encoder.Default)

    declared_type =
      case Keyword.fetch(options, :type) do
        {:ok, type} -> quote(do: def(type, do: unquote(type)))
        :error -> nil
      end

    quote do
      defimpl Ariadne.Flow.Store.Event.Encoder, for: unquote(module) do
        unquote(declared_type)

        defdelegate encode(event), to: unquote(target)
        defdelegate decode(event_struct, store_data, metadata), to: unquote(target)
      end
    end
  end

  def encode(event)
  def decode(event, store_data, metadata)
end
