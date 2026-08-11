defmodule Ariadne.Flow.Filter do
  alias Ariadne.Flow.Event.Type

  @enforce_keys [:types]
  defstruct [:types, :tags, only_last_event: false]

  @type t :: %__MODULE__{
          types: [String.t(), ...],
          tags: [String.t()] | nil,
          only_last_event: boolean()
        }

  @filter_hint "A filter must contain types and may contain tags and only_last_event"
  @filter_hint_empty_types "A filter's types must be a non-empty list"
  @filter_hint_only_last_event "A filter's only_last_event must be a boolean"
  @filter_hint_tags "A filter's tags must be a list"
  @filter_hint_empty_tags "A filter's tags must be omitted rather than empty"
  @filter_hint_repeated_tags "A filter's tags must not repeat"

  def new(filter) do
    validate!(filter)

    %__MODULE__{
      types: Enum.map(filter.types, &serialize_type/1),
      tags: Map.get(filter, :tags),
      only_last_event: only_last_event?(filter)
    }
  end

  def matches?(%{types: types, tags: tags}, %{type: _, tags: _} = stored) do
    matches_types?(types, stored) and matches_tags?(tags, stored)
  end

  def take_last(events, %__MODULE__{only_last_event: true}), do: Enum.take(events, -1)
  def take_last(events, %__MODULE__{only_last_event: false}), do: events

  defp serialize_type(type) when is_binary(type), do: type
  defp serialize_type(type) when is_atom(type), do: Type.of(type)

  defp only_last_event?(%{only_last_event: only_last_event}) when is_boolean(only_last_event),
    do: only_last_event

  defp only_last_event?(_filter), do: false

  defp matches_types?(types, %{type: type}), do: type in types

  defp matches_tags?(nil, _), do: true

  defp matches_tags?(tags, %{tags: event_tags}) do
    MapSet.subset?(MapSet.new(tags), MapSet.new(event_tags))
  end

  defp validate!(%{types: []}), do: raise(ArgumentError, @filter_hint_empty_types)

  defp validate!(%{types: _} = filter) do
    validate_tags!(Map.get(filter, :tags))
    validate_only_last_event!(Map.get(filter, :only_last_event))
  end

  defp validate!(_), do: raise(ArgumentError, @filter_hint)

  defp validate_tags!(nil), do: :ok
  defp validate_tags!([]), do: raise(ArgumentError, @filter_hint_empty_tags)

  defp validate_tags!(tags) when is_list(tags) do
    if Enum.uniq(tags) == tags,
      do: :ok,
      else: raise(ArgumentError, @filter_hint_repeated_tags)
  end

  defp validate_tags!(_), do: raise(ArgumentError, @filter_hint_tags)

  defp validate_only_last_event!(value) when is_boolean(value) or is_nil(value), do: :ok
  defp validate_only_last_event!(_), do: raise(ArgumentError, @filter_hint_only_last_event)
end
