defmodule Ariadne.Flow.CommandHandler do
  @moduledoc false
  alias Ariadne.Flow.EventReducer
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.AppendCondition
  alias Ariadne.Flow.Store.Event.Codec
  alias Ariadne.Flow.Store.Event.Encoder

  def handle(command, %Store{} = store, opts \\ []) do
    %{result: result, query: query, events: read_events} = EventReducer.evaluate(command, store)

    with {:ok, events} <- result do
      store_events = Enum.map(events, &serialize/1)

      append_opts =
        maybe_put(
          [
            condition: AppendCondition.for_read(query, read_events),
            metadata: Keyword.get(opts, :metadata, %{})
          ],
          :created_at,
          Keyword.get(opts, :created_at)
        )

      with {:ok, %{events: sequenced_events}} <-
             Store.append(store, store_events, append_opts) do
        {:ok, %{events: Enum.map(sequenced_events, &Codec.deserialize/1)}}
      end
    end
  end

  defp serialize(%type{} = event) do
    %{tags: tags, data: data} = Encoder.encode(event)

    %Store.Event{
      type: Codec.serialize_type(type),
      data: data,
      tags: tags
    }
  end

  defp maybe_put(keyword, _key, nil), do: keyword
  defp maybe_put(keyword, key, value), do: Keyword.put(keyword, key, value)
end
