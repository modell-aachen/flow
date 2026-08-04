defmodule Ariadne.Flow.ReactorEngine do
  @moduledoc false
  @callback run(
              reactor_run :: %Ariadne.Flow.ReactorRun{},
              store :: %Ariadne.Flow.Store{},
              opts :: keyword()
            ) :: :ok | {:error, term()}

  def normalize({module, opts}) when is_atom(module) and is_list(opts), do: {module, opts}
  def normalize(module) when is_atom(module), do: {module, []}
end
