defmodule Ariadne.Flow.Query do
  alias Ariadne.Flow.Filter
  alias Ariadne.Flow.Query.Optimizer

  @enforce_keys [:filters]
  defstruct @enforce_keys

  @typedoc "A normalised query: every event, or the filters an event has to match one of."
  @type t :: %__MODULE__{filters: :all | [Filter.t()]}

  def new(%__MODULE__{} = query), do: query

  def new(:all), do: %__MODULE__{filters: :all}

  def new(filters) when is_list(filters) do
    filters = Enum.map(filters, &Filter.new/1)

    %__MODULE__{filters: Optimizer.optimize(filters)}
  end
end
