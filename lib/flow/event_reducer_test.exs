defmodule Ariadne.Flow.EventReducerTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.EventReducer
  alias Ariadne.Flow.Projection
  alias Ariadne.Flow.Store

  defmodule CountEvent do
    @derive Ariadne.Flow.Event
    defstruct count: 1

    def tags(%{count: count}), do: ["count:#{count}"]
  end

  defp num_counts_projection do
    Projection.new(
      %{initial_state: 0, filter: %{types: [CountEvent]}},
      fn state, %CountEvent{}, _ -> state + 1 end
    )
  end

  defp last_count_projection do
    Projection.new(
      %{initial_state: 0, filter: %{types: [CountEvent], only_last_event: true}},
      fn _state, %CountEvent{count: count}, _ -> count end
    )
  end

  defp count_event(count) do
    %Store.Record{
      type: "Ariadne.Flow.EventReducerTest.CountEvent",
      data: %{"count" => count},
      tags: []
    }
  end

  defp append(store, count) do
    {:ok, _} = Store.append(store, [count_event(count)])
  end

  describe "evaluate/2" do
    test "folds the query's matching events into the reducer's result" do
      store = Store.InMemory.init()
      append(store, 1)
      append(store, 2)

      assert %{result: 2} = EventReducer.evaluate(num_counts_projection(), store)
    end

    test "returns the read it reduced, carrying the normalised query" do
      store = Store.InMemory.init()
      append(store, 1)

      assert %{
               read: %Store.Read{
                 query: %{filters: [%{types: ["Ariadne.Flow.EventReducerTest.CountEvent"]}]},
                 events: [%Store.SequencedRecord{position: 1}],
                 last_position: 1
               }
             } = EventReducer.evaluate(num_counts_projection(), store)
    end

    test "reads the last matching event only when the reducer's query asks for it" do
      store = Store.InMemory.init()
      append(store, 1)
      append(store, 2)

      assert %{
               result: 2,
               read: %Store.Read{
                 query: %{filters: [%{only_last_event: true}]},
                 events: [%Store.SequencedRecord{position: 2}]
               }
             } = EventReducer.evaluate(last_count_projection(), store)
    end

    test "returns the result of an empty event stream" do
      store = Store.InMemory.init()

      assert %{result: 0, read: %Store.Read{events: [], last_position: 0}} =
               EventReducer.evaluate(num_counts_projection(), store)
    end
  end
end
