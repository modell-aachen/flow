defmodule Ariadne.Flow.Store.StoredEventReactor do
  @moduledoc false
  alias Ariadne.Flow.Query

  @enforce_keys [:name, :query, :handler]
  defstruct [:name, :query, :handler, start_after_position: 0]

  def new(%{name: name, query: query, handler: handler} = attrs)
      when is_binary(name) and is_list(query) and is_function(handler, 1) do
    %__MODULE__{
      name: name,
      query: Query.new(query),
      handler: handler,
      start_after_position: Map.get(attrs, :start_after_position, 0)
    }
  end
end
