defmodule Ariadne.Flow.Store.AppendConditionTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.AppendCondition
  alias Ariadne.Flow.Store.Event

  test "new/1 normalises a raw condition and defaults the position to append after" do
    assert %AppendCondition{
             fail_if_events_match: [%Query.Item{types: ["ItemAdded"]}],
             after: 0
           } = AppendCondition.new(%{fail_if_events_match: [%{types: ["ItemAdded"]}]})
  end

  test "new/1 returns an already built condition instead of rebuilding it" do
    condition = AppendCondition.new(%{fail_if_events_match: [%{types: ["ItemAdded"]}], after: 7})

    assert condition == AppendCondition.new(condition)
  end

  test "new/1 rejects anything that is not a condition" do
    assert_raise RuntimeError, fn -> AppendCondition.new(%{after: 1}) end
  end

  test "for_read/1 conflicts on the read's own query after the last position it saw" do
    read = Store.read(store_with_two_items(), [%{types: ["ItemAdded"]}])

    assert %AppendCondition{fail_if_events_match: read.query, after: 2} ==
             AppendCondition.for_read(read)
  end

  test "for_read/1 conflicts on the whole store when the read saw nothing" do
    read = Store.read(Store.InMemory.init())

    assert %AppendCondition{fail_if_events_match: :all, after: 0} ==
             AppendCondition.for_read(read)
  end

  defp store_with_two_items do
    store = Store.InMemory.init()
    {:ok, _} = Store.append(store, [item_added(), item_added()])
    store
  end

  defp item_added, do: %Event{type: "ItemAdded", data: %{}, tags: []}
end
