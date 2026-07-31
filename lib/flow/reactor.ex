defmodule Ariadne.Flow.Reactor do
  alias Ariadne.Flow.Query

  @enforce_keys [:name, :filter, :handler]
  defstruct [:name, :filter, :handler, start_after_position: 0, sync: false]

  @filter_hint_only_last_event "A reactor's filter cannot ask for only_last_event — a reactor reacts to every event it matches"

  def new(%{name: name, filter: filter} = attrs, handler)
      when is_binary(name) and is_function(handler, 2) do
    %__MODULE__{
      name: name,
      filter: new_filter(filter),
      handler: handler,
      start_after_position: Map.get(attrs, :start_after_position, 0),
      sync: Map.get(attrs, :sync, false)
    }
  end

  def query(%__MODULE__{filter: filter}), do: [filter]

  def handle(
        %__MODULE__{filter: filter, handler: handler},
        %{event: event, metadata: metadata} = envelope
      ) do
    if Query.Item.matches?(filter, envelope), do: handler.(event, metadata), else: :ok
  end

  defp new_filter(filter) do
    case Query.Item.new(filter) do
      %Query.Item{only_last_event: true} -> raise(@filter_hint_only_last_event)
      %Query.Item{} = item -> item
    end
  end
end
