defmodule Ariadne.Flow.Reactor do
  alias Ariadne.Flow.Query

  @enforce_keys [:name, :filter, :handler]
  defstruct [:name, :filter, :handler, start_after_position: :head, sync: false]

  @filter_hint_only_last_event "A reactor's filter cannot ask for only_last_event — a reactor reacts to every event it matches"
  @start_hint ":start_after_position must be :head or a non-negative position, got: "

  def new(%{name: name, filter: filter} = attrs, handler)
      when is_binary(name) and is_function(handler, 2) do
    %__MODULE__{
      name: name,
      filter: new_filter(filter),
      handler: handler,
      start_after_position:
        new_start_after_position(Map.get(attrs, :start_after_position, :head)),
      sync: Map.get(attrs, :sync, false)
    }
  end

  def query(%__MODULE__{filter: filter}), do: [filter]

  def handle(
        %__MODULE__{handler: handler} = reactor,
        %{event: event, metadata: metadata} = envelope
      ) do
    if matches?(reactor, envelope), do: handler.(event, metadata), else: :ok
  end

  # Whether this reactor reacts to the event at all. The stored `type` and `tags` the
  # envelope carries are what the filter matches on, so an event can be tested against a
  # reactor without going back to the store — which is how a caller works out which of a
  # dispatch's events a reactor is ever going to process.
  def matches?(%__MODULE__{filter: filter}, envelope), do: Query.Item.matches?(filter, envelope)

  defp new_start_after_position(:head), do: :head

  defp new_start_after_position(position) when is_integer(position) and position >= 0,
    do: position

  defp new_start_after_position(position),
    do: raise(ArgumentError, @start_hint <> inspect(position))

  defp new_filter(filter) do
    case Query.Item.new(filter) do
      %Query.Item{only_last_event: true} -> raise(@filter_hint_only_last_event)
      %Query.Item{} = item -> item
    end
  end
end
