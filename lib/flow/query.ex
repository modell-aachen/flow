defmodule Ariadne.Flow.Query do
  alias Ariadne.Flow.Query.Optimizer

  defmodule Item do
    @enforce_keys [:types]
    defstruct [:types, :tags]

    @type t :: %__MODULE__{types: [String.t(), ...], tags: [String.t()] | nil}

    @query_hint "A query item must contain types and may contain tags"
    @query_hint_empty_types "A query item's types must be a non-empty list"

    def new(item) do
      validate!(item)
      %__MODULE__{types: item.types, tags: Map.get(item, :tags)}
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
    defp validate!(%{types: _}), do: :ok
    defp validate!(_), do: raise(@query_hint)
  end

  @typedoc "A normalised query: every event, or the items an event has to match one of."
  @type t :: :all | [Item.t()]

  def matches?(:all, _event), do: true

  def matches?(query, %{type: _, tags: _} = event) when is_list(query) do
    Enum.any?(query, fn query_item ->
      Item.matches?(query_item, event)
    end)
  end

  def new(:all), do: :all

  def new(query) when is_list(query) do
    query
    |> Enum.map(&Item.new/1)
    |> Optimizer.optimize()
  end
end
