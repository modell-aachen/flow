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

  describe "evaluate_for_append/2" do
    test "folds the query's matching events into the reducer's result" do
      store = Store.InMemory.init()
      append(store, 1)
      append(store, 2)

      assert {2, _append_condition} =
               EventReducer.evaluate_for_append(num_counts_projection(), store)
    end

    test "returns an append condition scoping a later append to the folded events" do
      store = Store.InMemory.init()
      append(store, 1)

      {_result, append_condition} =
        EventReducer.evaluate_for_append(num_counts_projection(), store)

      assert {:ok, _} = Store.append(store, [count_event(2)], condition: append_condition)

      assert {:error, :append_condition_failed} =
               Store.append(store, [count_event(3)], condition: append_condition)
    end

    test "returns an append condition scoping a later append when nothing was read" do
      store = Store.InMemory.init()

      {_result, append_condition} =
        EventReducer.evaluate_for_append(num_counts_projection(), store)

      assert {:ok, _} = Store.append(store, [count_event(1)], condition: append_condition)

      assert {:error, :append_condition_failed} =
               Store.append(store, [count_event(2)], condition: append_condition)
    end
  end
end
