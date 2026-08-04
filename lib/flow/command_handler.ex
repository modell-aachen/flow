defmodule Ariadne.Flow.CommandHandler do
  @moduledoc false
  alias Ariadne.Flow.AppendConditionError
  alias Ariadne.Flow.EventReducer
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.AppendCondition
  alias Ariadne.Flow.Store.Event.Codec
  alias Ariadne.Flow.Store.Event.Encoder

  @enforce_keys [:command]
  defstruct [:command, metadata: %{}, created_at: nil]

  def new(%{command: command} = attrs) do
    %__MODULE__{
      command: command,
      metadata: Map.get(attrs, :metadata, %{}),
      created_at: Map.get(attrs, :created_at)
    }
  end

  def handle(%__MODULE__{command: command} = command_handler, %Store{} = store) do
    %{result: result, read: read} = EventReducer.evaluate(command, store)

    with {:ok, events} <- result do
      store_events = Enum.map(events, &serialize/1)

      append_opts =
        maybe_put(
          [
            condition: AppendCondition.for_read(read),
            metadata: command_handler.metadata
          ],
          :created_at,
          command_handler.created_at
        )

      case Store.append(store, store_events, append_opts) do
        {:ok, %{events: sequenced_events}} ->
          {:ok, %{events: Enum.map(sequenced_events, &Codec.deserialize/1)}}

        {:error, :append_condition_failed} ->
          {:error, %AppendConditionError{}}

        {:error, _} = error ->
          error
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
