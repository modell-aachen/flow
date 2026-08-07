defmodule Ariadne.Flow.ReactorEngine do
  @moduledoc false
  @callback schedule(
              reactor_runs :: [%Ariadne.Flow.ReactorRun{}],
              store :: %Ariadne.Flow.Store{},
              opts :: keyword()
            ) :: [%Ariadne.Flow.ReactorRun{}]

  def normalize(nil), do: nil
  def normalize({module, opts}) when is_atom(module) and is_list(opts), do: {module, opts}
  def normalize(module) when is_atom(module), do: {module, []}
end
