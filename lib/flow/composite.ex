defmodule Ariadne.Flow.Composite do
  @behaviour Ariadne.Flow.EventReducer

  @enforce_keys [:read_model, :map_fn]
  defstruct @enforce_keys

  def new(read_model, map_fn) when is_non_struct_map(read_model) and is_function(map_fn, 1) do
    %__MODULE__{read_model: read_model, map_fn: map_fn}
  end

  @impl Ariadne.Flow.EventReducer
  def reduce(%__MODULE__{read_model: read_model, map_fn: map_fn}, events) do
    read_model
    |> reduce_to_state(events)
    |> map_fn.()
  end

  defp reduce_to_state(read_model, events) do
    for {key, %module{} = projection} <- read_model, into: %{} do
      {key, module.reduce(projection, events)}
    end
  end

  @impl Ariadne.Flow.EventReducer
  def query(%__MODULE__{read_model: read_model}) do
    read_model
    |> Map.values()
    |> Enum.flat_map(fn %module{} = m -> module.query(m) end)
  end
end
