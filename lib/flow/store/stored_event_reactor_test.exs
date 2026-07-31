defmodule Ariadne.Flow.Store.StoredEventReactorTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store.StoredEventReactor

  test "new/1 normalises the query and defaults the position to start after" do
    assert %StoredEventReactor{
             name: "echo",
             query: [%Query.Item{types: ["ItemAdded"], only_last_event: false}],
             start_after_position: 0
           } = new(%{query: [%{types: ["ItemAdded"]}]})
  end

  test "new/1 keeps the given position to start after" do
    assert %StoredEventReactor{start_after_position: 42} =
             new(%{query: [%{types: ["ItemAdded"]}], start_after_position: 42})
  end

  test "new/1 rejects a query asking for only the last event" do
    assert_raise RuntimeError, ~r/only_last_event/, fn ->
      new(%{query: [%{types: ["ItemAdded"], only_last_event: true}]})
    end

    assert_raise RuntimeError, ~r/only_last_event/, fn ->
      new(%{
        query: [
          %{types: ["ItemAdded"]},
          %{types: ["ItemRemoved"], only_last_event: true}
        ]
      })
    end
  end

  defp new(attrs) do
    attrs
    |> Map.put_new(:name, "echo")
    |> Map.put_new(:handler, fn _events -> {:ok, 0} end)
    |> StoredEventReactor.new()
  end
end
