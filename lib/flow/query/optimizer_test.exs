defmodule Ariadne.Flow.Query.OptimizerTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.Query.Item
  alias Ariadne.Flow.Query.Optimizer

  describe "optimize/1 on items wanting every match" do
    test "keeps a single item as it is" do
      assert [%Item{types: ["TestEvent"], tags: ["test_tag"], only_last_event: false}] =
               optimize([%{types: ["TestEvent"], tags: ["test_tag"]}])
    end

    test "drops duplicate items" do
      assert [%{types: ["TestEvent"], tags: ["test_tag"]}] =
               optimize([
                 %{types: ["TestEvent"], tags: ["test_tag"]},
                 %{types: ["TestEvent"], tags: ["test_tag"]}
               ])
    end

    test "drops a tag superset covered by a subset of it for the same type" do
      assert [%{types: ["TestEvent"], tags: ["test_tag"]}] =
               optimize([
                 %{types: ["TestEvent"], tags: ["test_tag"]},
                 %{types: ["TestEvent"], tags: ["test_tag", "other_tag"]}
               ])
    end

    test "keeps tag constraints neither of which covers the other" do
      assert [
               %{types: ["TestEvent"], tags: ["other_tag"]},
               %{types: ["TestEvent"], tags: ["test_tag"]}
             ] =
               optimize([
                 %{types: ["TestEvent"], tags: ["test_tag"]},
                 %{types: ["TestEvent"], tags: ["other_tag"]}
               ])
    end

    test "lets an unrestricted item absorb the tag-restricted ones for its type" do
      assert [%{types: ["TestEvent"], tags: nil}] =
               optimize([
                 %{types: ["TestEvent"], tags: ["test_tag"]},
                 %{types: ["TestEvent"]},
                 %{types: ["TestEvent"], tags: ["other_tag"]}
               ])
    end

    test "keeps the tag-restricted items of the types the unrestricted one leaves out" do
      assert [
               %{types: ["OtherEvent"], tags: ["test_tag"]},
               %{types: ["TestEvent"], tags: nil}
             ] =
               optimize([
                 %{types: ["TestEvent"]},
                 %{types: ["OtherEvent"], tags: ["test_tag"]}
               ])
    end

    test "collapses types sharing a tag constraint into one item" do
      assert [%{types: types, tags: ["test_tag"]}] =
               optimize([
                 %{types: ["TestEvent"], tags: ["test_tag"]},
                 %{types: ["OtherEvent"], tags: ["test_tag"]}
               ])

      assert Enum.sort(types) == ["OtherEvent", "TestEvent"]
    end

    test "expands an item over several types into one per type" do
      assert [
               %{types: ["OtherEvent"], tags: nil},
               %{types: ["TestEvent"], tags: ["test_tag"]}
             ] =
               optimize([
                 %{types: ["TestEvent", "OtherEvent"], tags: ["test_tag"]},
                 %{types: ["OtherEvent"]}
               ])
    end
  end

  describe "optimize/1 on items wanting their last match only" do
    test "drops duplicate items" do
      assert [%{types: ["TestEvent"], tags: ["test_tag"], only_last_event: true}] =
               optimize([
                 %{types: ["TestEvent"], tags: ["test_tag"], only_last_event: true},
                 %{types: ["TestEvent"], tags: ["test_tag"], only_last_event: true}
               ])
    end

    test "drops items differing only in the order of their types and tags" do
      assert [%{tags: ["other_tag", "test_tag"], only_last_event: true}] =
               optimize([
                 %{
                   types: ["TestEvent", "OtherEvent"],
                   tags: ["other_tag", "test_tag"],
                   only_last_event: true
                 },
                 %{
                   types: ["OtherEvent", "TestEvent"],
                   tags: ["test_tag", "other_tag"],
                   only_last_event: true
                 }
               ])
    end

    test "keeps a tag superset, whose last event is not the subset's last event" do
      assert [
               %{types: ["TestEvent"], tags: ["test_tag"], only_last_event: true},
               %{types: ["TestEvent"], tags: ["test_tag", "other_tag"], only_last_event: true}
             ] =
               optimize([
                 %{types: ["TestEvent"], tags: ["test_tag"], only_last_event: true},
                 %{types: ["TestEvent"], tags: ["test_tag", "other_tag"], only_last_event: true}
               ])
    end

    test "keeps a tag-restricted item beside an unrestricted one for the same type" do
      assert [
               %{types: ["TestEvent"], tags: nil, only_last_event: true},
               %{types: ["TestEvent"], tags: ["test_tag"], only_last_event: true}
             ] =
               optimize([
                 %{types: ["TestEvent"], only_last_event: true},
                 %{types: ["TestEvent"], tags: ["test_tag"], only_last_event: true}
               ])
    end

    test "keeps types sharing a tag constraint apart, each with its own last event" do
      assert [
               %{types: ["OtherEvent"], tags: ["test_tag"], only_last_event: true},
               %{types: ["TestEvent"], tags: ["test_tag"], only_last_event: true}
             ] =
               optimize([
                 %{types: ["TestEvent"], tags: ["test_tag"], only_last_event: true},
                 %{types: ["OtherEvent"], tags: ["test_tag"], only_last_event: true}
               ])
    end

    test "keeps an item apart from the one that wants every match of the same events" do
      assert [
               %{types: ["TestEvent"], tags: nil, only_last_event: false},
               %{types: ["TestEvent"], tags: nil, only_last_event: true}
             ] =
               optimize([
                 %{types: ["TestEvent"], only_last_event: true},
                 %{types: ["TestEvent"]}
               ])
    end

    test "optimizes the items wanting every match beside them" do
      assert [
               %{types: ["OtherEvent"], tags: ["test_tag"], only_last_event: true},
               %{types: ["TestEvent"], tags: nil, only_last_event: false}
             ] =
               optimize([
                 %{types: ["TestEvent"], tags: ["test_tag"]},
                 %{types: ["TestEvent"]},
                 %{types: ["OtherEvent"], tags: ["test_tag"], only_last_event: true}
               ])
    end
  end

  test "optimize/1 returns nothing for no items" do
    assert [] == optimize([])
  end

  defp optimize(items) do
    items
    |> Enum.map(&Item.new/1)
    |> Optimizer.optimize()
    |> Enum.sort_by(&{&1.types, &1.tags, &1.only_last_event})
  end
end
