defmodule Ariadne.Flow.Store.Speedrun.ReportingStore do
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Speedrun.Reporter

  def read(pid, query \\ :all, opts \\ []) do
    {time, result} = :timer.tc(fn -> Store.read(pid, query, opts) end)
    Reporter.record_read(time)
    result
  end

  def append(pid, events, opts \\ []) do
    {time, result} = :timer.tc(fn -> Store.append(pid, events, opts) end)
    Reporter.record_append(time)
    result
  end
end
