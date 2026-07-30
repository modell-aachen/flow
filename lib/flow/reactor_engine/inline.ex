defmodule Ariadne.Flow.ReactorEngine.Inline do
  @moduledoc false
  @behaviour Ariadne.Flow.ReactorEngine

  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Store

  @impl Ariadne.Flow.ReactorEngine
  def run(%ReactorRun{} = reactor_run, %Store{} = store, _opts),
    do: ReactorRun.execute(reactor_run, store)
end
