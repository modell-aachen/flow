defmodule Ariadne.Flow.Store.AppendCondition do
  alias Ariadne.Flow.Query

  @append_condition_hint "An append condition must have 'fail_if_events_match' and may have 'after'"

  @enforce_keys [:fail_if_events_match, :after]
  defstruct @enforce_keys

  def new(%{fail_if_events_match: query} = condition) do
    after_condition = Map.get(condition, :after, 0)
    %__MODULE__{fail_if_events_match: Query.new(query), after: after_condition}
  end

  def new(_), do: raise(@append_condition_hint)

  def for_read(query, sequenced_events) do
    new(%{fail_if_events_match: query, after: last_position(sequenced_events)})
  end

  defp last_position(sequenced_events), do: List.last(sequenced_events, %{position: 0}).position
end
