defmodule Ariadne.Flow.Store.StoredEventReactor do
  @moduledoc false
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store.SequencedRecord

  @enforce_keys [:name, :query, :handler]
  defstruct @enforce_keys

  @query_hint_only_last_event "A reactor's query cannot ask for only_last_event — every event it leaves out is checkpointed past and never delivered"

  @typedoc """
  Processes a batch of events and reports how many of them it got through, so the
  store knows where to set the reactor's checkpoint.
  """
  @type handler ::
          ([SequencedRecord.t()] ->
             {:ok, non_neg_integer()} | {:error, non_neg_integer(), term()})

  @type t :: %__MODULE__{name: String.t(), query: Query.t(), handler: handler()}

  def new(%{name: name, query: query, handler: handler})
      when is_binary(name) and is_list(query) and is_function(handler, 1) do
    %__MODULE__{name: name, query: new_query(query), handler: handler}
  end

  defp new_query(query) do
    %Query{filters: filters} = normalised = Query.new(query)

    if Enum.any?(filters, & &1.only_last_event),
      do: raise(ArgumentError, @query_hint_only_last_event),
      else: normalised
  end
end
