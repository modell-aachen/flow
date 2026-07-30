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
end
