defmodule Ariadne.Flow.Store.Postgres.ConsistencyRun.TagExclusivity do
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Record

  def run(:a, store, id) do
    tick(store, id)
  end

  def run(:b, store, id) do
    boom(store, id)
  end

  defp tick(store, id) do
    Store.append(store, tick_event(id),
      condition: %{
        fail_if_events_match: [boom_filter(id)]
      }
    )
  end

  defp boom(store, id) do
    Store.append(store, boom_event(id))
  end

  defp tick_type do
    "Tick"
  end

  defp boom_type do
    "Boom"
  end

  defp tick_event(id) do
    %Record{
      type: tick_type(),
      data: %{},
      tags: ["id:#{id}"]
    }
  end

  defp boom_event(id) do
    %Record{
      type: boom_type(),
      data: %{},
      tags: ["id:#{id}"]
    }
  end

  defp tick_filter(id) do
    %{types: [tick_type()], tags: ["id:#{id}"]}
  end

  defp boom_filter(id) do
    %{types: [boom_type()], tags: ["id:#{id}"]}
  end

  def evaluate(store, id, _a_result, _b_result) do
    %{events: events} =
      Store.read(store, [
        tick_filter(id),
        boom_filter(id)
      ])

    tick_type = tick_type()
    boom_type = boom_type()

    case events do
      [%{record: %{type: ^boom_type}}, %{record: %{type: ^tick_type}}] ->
        {:error, "Consistency violation: Tick after Boom"}

      [%{record: %{type: ^tick_type}}, %{record: %{type: ^boom_type}}] ->
        :ok

      [%{record: %{type: ^boom_type}}] ->
        :ok

      _ ->
        {:error, "Unexpected events: #{inspect(events)}"}
    end
  end
end
