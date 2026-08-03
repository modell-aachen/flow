defmodule Ariadne.Flow.ReactorEngine do
  @moduledoc false
  @callback run(
              reactor_run :: %Ariadne.Flow.ReactorRun{},
              store :: %Ariadne.Flow.Store{},
              opts :: keyword()
            ) :: :ok | {:error, term()}
end
