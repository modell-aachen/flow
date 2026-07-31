defmodule Ariadne.Flow.Store.StoredEventReactor do
  @moduledoc """
  A reactor as the store sees it: a name to checkpoint under, a query to select the
  events it reacts to, and a handler to hand them to.

  `Ariadne.Flow.Store.consume/2` takes one of these. The name is what a backend stores
  its position against, so two reactors sharing a name share a position;
  `start_after_position` only decides where a reactor with no stored position begins.

  The query has to want every event it matches, so `new/1` rejects one whose items ask
  for `only_last_event`: consumption checkpoints past the whole batch it was handed, and
  an event the query left out is not delivered later either.
  """
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store.SequencedEvent

  @enforce_keys [:name, :query, :handler]
  defstruct [:name, :query, :handler, start_after_position: 0]

  @query_hint_only_last_event "A reactor's query cannot ask for only_last_event — every event it leaves out is checkpointed past and never delivered"

  @typedoc """
  Processes a batch of events and reports how many of them it got through, so the
  store knows where to set the reactor's checkpoint.
  """
  @type handler ::
          ([SequencedEvent.t()] ->
             {:ok, non_neg_integer()} | {:error, non_neg_integer(), term()})

  @type t :: %__MODULE__{
          name: String.t(),
          query: Query.t(),
          handler: handler(),
          start_after_position: non_neg_integer()
        }

  def new(%{name: name, query: query, handler: handler} = attrs)
      when is_binary(name) and is_list(query) and is_function(handler, 1) do
    %__MODULE__{
      name: name,
      query: new_query(query),
      handler: handler,
      start_after_position: Map.get(attrs, :start_after_position, 0)
    }
  end

  defp new_query(query) do
    normalised = Query.new(query)

    if Enum.any?(normalised, & &1.only_last_event),
      do: raise(@query_hint_only_last_event),
      else: normalised
  end
end
