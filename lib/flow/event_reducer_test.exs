defmodule Ariadne.Flow.EventReducerTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.EventReducer
  alias Ariadne.Flow.Projection
  alias Ariadne.Flow.Store

  defmodule CountEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct count: 1

    def tags(%{count: count}), do: ["count:#{count}"]
  end

  defp num_counts_projection do
    Projection.new(
      %{initial_state: 0, filter: %{types: [CountEvent]}},
      fn state, %CountEvent{}, _ -> state + 1 end
    )
  end

  defp count_event(count) do
    %Store.Event{
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

    test "returns the reducer's serialized query and the events it read" do
      store = Store.InMemory.init()
      append(store, 1)

      assert %{
               query: [%{types: ["Ariadne.Flow.EventReducerTest.CountEvent"]}],
               events: [%Store.SequencedEvent{position: 1}]
             } = EventReducer.evaluate(num_counts_projection(), store)
    end

    test "returns the result of an empty event stream" do
      store = Store.InMemory.init()

      assert %{result: 0, events: []} = EventReducer.evaluate(num_counts_projection(), store)
    end
  end
end
