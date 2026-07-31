defmodule Ariadne.Flow.QueryTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.Query

  test "Queries are optimized" do
    assert [
             %{types: ["TestEvent"], tags: nil},
             %{types: ["TestEvent2"], tags: ["test_tag"]},
             %{types: ["TestEvent6"], tags: ["test_tag2"]},
             %{types: ["TestEvent5"], tags: ["test_tag3"]},
             %{types: ["TestEvent3"], tags: ["test_tag4"]},
             %{
               types: ["TestEvent3", "TestEvent4"],
               tags: ["test_tag2", "test_tag3"]
             }
           ] =
             Query.new([
               %{types: ["TestEvent"], tags: ["test_tag"]},
               %{types: ["TestEvent"], tags: ["test_tag2"]},
               %{types: ["TestEvent"]},
               %{types: ["TestEvent3"], tags: ["test_tag2", "test_tag3"]},
               %{types: ["TestEvent3"], tags: ["test_tag2", "test_tag3"]},
               %{types: ["TestEvent3"], tags: ["test_tag4"]},
               %{types: ["TestEvent2"], tags: ["test_tag"]},
               %{types: ["TestEvent4", "TestEvent5"], tags: ["test_tag3", "test_tag2"]},
               %{types: ["TestEvent5"], tags: ["test_tag3"]},
               %{types: ["TestEvent6"], tags: ["test_tag2"]}
             ])
  end

  test "Query items want every match unless they ask for only the last event" do
    assert [%Query.Item{only_last_event: false}] = Query.new([%{types: ["TestEvent"]}])

    assert [%Query.Item{only_last_event: true}] =
             Query.new([%{types: ["TestEvent"], only_last_event: true}])

    assert_raise RuntimeError, fn ->
      Query.new([%{types: ["TestEvent"], only_last_event: "yes"}])
    end
  end

  test "Items asking for only the last event are deduplicated but never merged" do
    assert [
             %{types: ["TestEvent"], tags: nil, only_last_event: false},
             %{types: ["TestEvent"], tags: nil, only_last_event: true},
             %{types: ["TestEvent"], tags: ["test_tag"], only_last_event: true},
             %{types: ["TestEvent2"], tags: nil, only_last_event: true}
           ] =
             Query.new([
               %{types: ["TestEvent"], only_last_event: true},
               %{types: ["TestEvent"], only_last_event: true},
               %{types: ["TestEvent"], tags: ["test_tag"], only_last_event: true},
               %{types: ["TestEvent2"], only_last_event: true},
               %{types: ["TestEvent"]}
             ])
  end
end
