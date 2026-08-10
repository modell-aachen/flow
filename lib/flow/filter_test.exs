defmodule Ariadne.Flow.FilterTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.Filter

  defmodule PinnedEvent do
    @derive {Ariadne.Flow.Event, type: "filter-test-pinned"}
    defstruct [:label]

    def tags(_), do: []
  end

  test "Filters serialize event types given as modules" do
    assert %Filter{types: ["Ariadne.Flow.FilterTest"]} =
             Filter.new(%{types: [Ariadne.Flow.FilterTest]})
  end

  test "Filters serialize an event module to the type the event declares" do
    assert %Filter{types: ["filter-test-pinned"]} = Filter.new(%{types: [PinnedEvent]})
  end

  test "Filters want every match unless they ask for only the last event" do
    assert %Filter{only_last_event: false} = Filter.new(%{types: ["TestEvent"]})

    assert %Filter{only_last_event: true} =
             Filter.new(%{types: ["TestEvent"], only_last_event: true})
  end

  test "Filters take an unset only_last_event for wanting every match" do
    assert %Filter{only_last_event: false} =
             Filter.new(%{types: ["TestEvent"], only_last_event: nil})
  end

  test "Filters narrow a list of matches to the last one only when asked to" do
    every_match = Filter.new(%{types: ["TestEvent"]})
    last_match = Filter.new(%{types: ["TestEvent"], only_last_event: true})

    assert [:first, :second] == Filter.take_last([:first, :second], every_match)
    assert [:second] == Filter.take_last([:first, :second], last_match)
    assert [] == Filter.take_last([], last_match)
  end

  test "Filters are validated" do
    assert_raise ArgumentError, ~r/non-empty list/, fn -> Filter.new(%{types: []}) end

    assert_raise ArgumentError, ~r/only_last_event must be a boolean/, fn ->
      Filter.new(%{types: ["TestEvent"], only_last_event: "yes"})
    end
  end

  test "An event matches a filter by type and by every tag the filter asks for" do
    filter = Filter.new(%{types: ["TestEvent"], tags: ["test_tag"]})

    assert Filter.matches?(filter, %{type: "TestEvent", tags: ["test_tag", "other_tag"]})
    refute Filter.matches?(filter, %{type: "TestEvent", tags: ["other_tag"]})
    refute Filter.matches?(filter, %{type: "OtherEvent", tags: ["test_tag"]})

    unrestricted = Filter.new(%{types: ["TestEvent"]})

    assert Filter.matches?(unrestricted, %{type: "TestEvent", tags: []})
  end
end
