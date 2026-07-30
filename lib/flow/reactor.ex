defmodule Ariadne.Flow.Reactor do
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store.Event.Encoder

  @enforce_keys [:name, :filter, :handler]
  defstruct [:name, :filter, :handler, start_after_position: 0, sync: false]

  def new(%{name: name, filter: filter} = attrs, handler)
      when is_binary(name) and is_function(handler, 2) do
    %__MODULE__{
      name: name,
      filter: Query.Item.new(filter),
      handler: handler,
      start_after_position: Map.get(attrs, :start_after_position, 0),
      sync: Map.get(attrs, :sync, false)
    }
  end

  def query(%__MODULE__{filter: filter}), do: [filter]

  def handle(%__MODULE__{filter: filter, handler: handler}, %{event: event, metadata: metadata}) do
    if matches?(filter, event), do: handler.(event, metadata), else: :ok
  end

  defp matches?(filter, %module{} = event) do
    %{tags: tags} = Encoder.encode(event)
    Query.Item.matches?(filter, %{type: module, tags: tags})
  end
end
