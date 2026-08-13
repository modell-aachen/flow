defmodule Ariadne.Flow.QueryTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.Filter
  alias Ariadne.Flow.Query

  test "Queries over every event normalise to a query over every event" do
    assert %Query{filters: :all} == Query.new(:all)
  end

  test "A normalised query normalises to itself" do
    query = Query.new([%{types: ["TestEvent"], tags: ["test_tag"]}])

    assert query == Query.new(query)
    assert Query.new(:all) == Query.new(Query.new(:all))
  end

  test "Queries normalise their filters and optimize them" do
    assert %Query{
             filters: [
               %Filter{types: ["TestEvent"], tags: ["test_tag"], only_last_event: false}
             ]
           } =
             Query.new([
               %{types: ["TestEvent"], tags: ["test_tag"]},
               %{types: ["TestEvent"], tags: ["test_tag", "other_tag"]}
             ])
  end

  test "Queries validate the filters they normalise" do
    assert_raise ArgumentError, ~r/non-empty list/, fn -> Query.new([%{types: []}]) end

    assert_raise ArgumentError, ~r/must contain types/, fn ->
      Query.new([%{tags: ["test_tag"]}])
    end
  end
end
