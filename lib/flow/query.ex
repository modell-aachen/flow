defmodule Ariadne.Flow.Query do
  alias Ariadne.Flow.Query.Optimizer

  defmodule Item do
    alias Ariadne.Flow.Store.Event.Codec

    @enforce_keys [:types]
    defstruct [:types, :tags, only_last_event: false]

    @type t :: %__MODULE__{
            types: [String.t(), ...],
            tags: [String.t()] | nil,
            only_last_event: boolean()
          }

    @query_hint "A query item must contain types and may contain tags and only_last_event"
    @query_hint_empty_types "A query item's types must be a non-empty list"
    @query_hint_only_last_event "A query item's only_last_event must be a boolean"

    def new(item) do
      validate!(item)

      %__MODULE__{
        types: Enum.map(item.types, &serialize_type/1),
        tags: Map.get(item, :tags),
        only_last_event: only_last_event?(item)
      }
    end

    def matches?(%{types: types, tags: tags}, %{type: _, tags: _} = event) do
      matches_types?(types, event) and matches_tags?(tags, event)
    end

    def take_last(events, %__MODULE__{only_last_event: true}), do: Enum.take(events, -1)
    def take_last(events, %__MODULE__{only_last_event: false}), do: events

    defp serialize_type(type) when is_binary(type), do: type
    defp serialize_type(type) when is_atom(type), do: Codec.serialize_type(type)

    defp only_last_event?(%{only_last_event: only_last_event}) when is_boolean(only_last_event),
      do: only_last_event

    defp only_last_event?(_item), do: false

    defp matches_types?(types, %{type: type}), do: type in types

    defp matches_tags?(nil, _), do: true

    defp matches_tags?(tags, %{tags: event_tags}) do
      MapSet.subset?(MapSet.new(tags), MapSet.new(event_tags))
    end

    defp validate!(%{types: []}), do: raise(@query_hint_empty_types)

    defp validate!(%{types: _} = item),
      do: validate_only_last_event!(Map.get(item, :only_last_event))

    defp validate!(_), do: raise(@query_hint)

    defp validate_only_last_event!(value) when is_boolean(value) or is_nil(value), do: :ok
    defp validate_only_last_event!(_), do: raise(@query_hint_only_last_event)
  end

  @enforce_keys [:items]
  defstruct @enforce_keys

  @typedoc "A normalised query: every event, or the items an event has to match one of."
  @type t :: %__MODULE__{items: :all | [Item.t()]}

  def new(%__MODULE__{} = query), do: query

  def new(:all), do: %__MODULE__{items: :all}

  def new(items) when is_list(items) do
    items = Enum.map(items, &Item.new/1)

    %__MODULE__{items: Optimizer.optimize(items)}
  end
end
