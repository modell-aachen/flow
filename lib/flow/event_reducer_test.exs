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

      assert EventReducer.evaluate(num_counts_projection(), store) == 2
    end

    test "returns the result of an empty event stream" do
      store = Store.InMemory.init()

      assert EventReducer.evaluate(num_counts_projection(), store) == 0
    end
  end

  describe "read/2" do
    test "returns the reducer's serialized query and the events matching it" do
      store = Store.InMemory.init()
      append(store, 1)

      assert {[%{types: ["Ariadne.Flow.EventReducerTest.CountEvent"]}],
              [%Store.SequencedEvent{position: 1}]} =
               EventReducer.read(num_counts_projection(), store)
    end

    test "returns no events when nothing matches the query" do
      store = Store.InMemory.init()

      assert {_query, []} = EventReducer.read(num_counts_projection(), store)
    end
  end

  describe "fold/2" do
    test "folds read events into the reducer's result" do
      store = Store.InMemory.init()
      append(store, 1)
      append(store, 2)
      {_query, sequenced_events} = EventReducer.read(num_counts_projection(), store)

      assert EventReducer.fold(num_counts_projection(), sequenced_events) == 2
    end
  end
end
