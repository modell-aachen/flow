defmodule Ariadne.Flow.Store.AppendCondition do
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store.Read

  @append_condition_hint "An append condition must have 'fail_if_events_match' and may have 'after'"

  @enforce_keys [:fail_if_events_match, :after]
  defstruct @enforce_keys

  @type t :: %__MODULE__{fail_if_events_match: Query.t(), after: non_neg_integer()}

  def new(%{fail_if_events_match: query} = condition) do
    after_condition = Map.get(condition, :after, 0)
    %__MODULE__{fail_if_events_match: Query.new(query), after: after_condition}
  end

  def new(_), do: raise(ArgumentError, @append_condition_hint)

  @doc "Conflicts on everything the read's own query matches after the last position it saw."
  def for_read(%Read{query: query, last_position: last_position}) do
    %__MODULE__{fail_if_events_match: query, after: last_position}
  end
end
