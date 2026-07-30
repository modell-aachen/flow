defmodule Ariadne.Flow.ReactorEngine do
  @moduledoc false
  @callback run(
              reactor_run :: %Ariadne.Flow.ReactorRun{},
              store :: %Ariadne.Flow.Store{},
              opts :: keyword()
            ) :: :ok | {:ok, %Ariadne.Flow.AfterCommit{}} | {:error, term()}
end
