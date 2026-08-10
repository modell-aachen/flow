defmodule Ariadne.Flow.QueryTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.Query

  defmodule PinnedEvent do
    @derive {Ariadne.Flow.Store.Event.Encoder, type: "query-test-pinned"}
    defstruct [:label]

    def tags(_), do: []
  end

  test "Queries over every event normalise to a query over every event" do
    assert %Query{items: :all} == Query.new(:all)
  end

  test "A normalised query normalises to itself" do
    query = Query.new([%{types: ["TestEvent"], tags: ["test_tag"]}])

    assert query == Query.new(query)
    assert Query.new(:all) == Query.new(Query.new(:all))
  end

  test "Queries normalise their items and optimize them" do
    assert %Query{
             items: [
               %Query.Item{types: ["TestEvent"], tags: ["test_tag"], only_last_event: false}
             ]
           } =
             Query.new([
               %{types: ["TestEvent"], tags: ["test_tag"]},
               %{types: ["TestEvent"], tags: ["test_tag", "other_tag"]}
             ])
  end

  test "Query items serialize event types given as modules" do
    assert %Query.Item{types: ["Ariadne.Flow.QueryTest"]} =
             Query.Item.new(%{types: [Ariadne.Flow.QueryTest]})

    assert %Query{items: [%Query.Item{types: ["Ariadne.Flow.QueryTest"]}]} =
             Query.new([%{types: [Ariadne.Flow.QueryTest]}])
  end

  test "Query items serialize an event module to the type the event declares" do
    assert %Query.Item{types: ["query-test-pinned"]} = Query.Item.new(%{types: [PinnedEvent]})

    assert %Query{items: [%Query.Item{types: ["query-test-pinned"]}]} =
             Query.new([%{types: [PinnedEvent]}])
  end

  test "Query items want every match unless they ask for only the last event" do
    assert %Query{items: [%Query.Item{only_last_event: false}]} =
             Query.new([%{types: ["TestEvent"]}])

    assert %Query{items: [%Query.Item{only_last_event: true}]} =
             Query.new([%{types: ["TestEvent"], only_last_event: true}])
  end

  test "Query items take an unset only_last_event for wanting every match" do
    assert %Query.Item{only_last_event: false} =
             Query.Item.new(%{types: ["TestEvent"], only_last_event: nil})

    assert %Query{items: [%Query.Item{only_last_event: false}]} =
             Query.new([%{types: ["TestEvent"], only_last_event: nil}])
  end

  test "Query items narrow a list of matches to the last one only when asked to" do
    every_match = Query.Item.new(%{types: ["TestEvent"]})
    last_match = Query.Item.new(%{types: ["TestEvent"], only_last_event: true})

    assert [:first, :second] == Query.Item.take_last([:first, :second], every_match)
    assert [:second] == Query.Item.take_last([:first, :second], last_match)
    assert [] == Query.Item.take_last([], last_match)
  end

  test "Query items are validated" do
    assert_raise ArgumentError, ~r/non-empty list/, fn -> Query.new([%{types: []}]) end

    assert_raise ArgumentError, ~r/must contain types/, fn ->
      Query.new([%{tags: ["test_tag"]}])
    end

    assert_raise ArgumentError, ~r/only_last_event must be a boolean/, fn ->
      Query.new([%{types: ["TestEvent"], only_last_event: "yes"}])
    end
  end

  test "An event matches a query item by type and by every tag the item asks for" do
    item = Query.Item.new(%{types: ["TestEvent"], tags: ["test_tag"]})

    assert Query.Item.matches?(item, %{type: "TestEvent", tags: ["test_tag", "other_tag"]})
    refute Query.Item.matches?(item, %{type: "TestEvent", tags: ["other_tag"]})
    refute Query.Item.matches?(item, %{type: "OtherEvent", tags: ["test_tag"]})

    unrestricted = Query.Item.new(%{types: ["TestEvent"]})

    assert Query.Item.matches?(unrestricted, %{type: "TestEvent", tags: []})
  end
end
