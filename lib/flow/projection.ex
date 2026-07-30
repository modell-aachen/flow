defmodule Ariadne.Flow.Projection do
  @behaviour Ariadne.Flow.EventReducer
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store
  @enforce_keys [:state, :filter, :handler]
  defstruct @enforce_keys

  def new(%{initial_state: state, filter: filter}, handler) when is_function(handler, 3) do
    %__MODULE__{state: state, filter: Query.Item.new(filter), handler: handler}
  end

  @impl Ariadne.Flow.EventReducer
  def reduce(%__MODULE__{state: state, filter: filter, handler: handler}, events) do
    events
    |> Enum.filter(&matches_query_item?(&1, filter))
    |> Enum.reduce(state, &build_state(&1, &2, handler))
  end

  defp matches_query_item?(%{event: %type{} = event}, filter) do
    %{tags: tags} = Store.Event.Encoder.encode(event)
    Query.Item.matches?(filter, %{type: type, tags: tags})
  end

  defp build_state(%{event: event, metadata: metadata}, state, handler) do
    handler.(state, event, metadata)
  end

  @impl Ariadne.Flow.EventReducer
  def query(%__MODULE__{filter: filter}), do: [filter]
end
