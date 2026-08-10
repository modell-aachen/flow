defmodule Ariadne.Flow.Composite do
  @behaviour Ariadne.Flow.EventReducer

  @enforce_keys [:reducers, :mapper]
  defstruct @enforce_keys

  def new(reducers, mapper) when is_non_struct_map(reducers) and is_function(mapper, 1) do
    %__MODULE__{reducers: reducers, mapper: mapper}
  end

  @impl Ariadne.Flow.EventReducer
  def reduce(%__MODULE__{reducers: reducers, mapper: mapper}, events) do
    reducers
    |> reduce_to_state(events)
    |> mapper.()
  end

  defp reduce_to_state(reducers, events) do
    for {key, %module{} = reducer} <- reducers, into: %{} do
      {key, module.reduce(reducer, events)}
    end
  end

  @impl Ariadne.Flow.EventReducer
  def query(%__MODULE__{reducers: reducers}) do
    reducers
    |> Map.values()
    |> Enum.flat_map(fn %module{} = reducer -> module.query(reducer) end)
  end
end
