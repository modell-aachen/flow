defmodule Ariadne.Flow.Query do
  alias Ariadne.Flow.Query.Optimizer

  defmodule Item do
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
        types: item.types,
        tags: Map.get(item, :tags),
        only_last_event: Map.get(item, :only_last_event, false)
      }
    end

    def matches?(%{types: types, tags: tags}, %{type: _, tags: _} = event) do
      matches_types?(types, event) and matches_tags?(tags, event)
    end

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

  @typedoc "A normalised query: every event, or the items an event has to match one of."
  @type t :: :all | [Item.t()]

  def new(:all), do: :all

  def new(query) when is_list(query) do
    query
    |> Enum.map(&Item.new/1)
    |> Optimizer.optimize()
  end
end
