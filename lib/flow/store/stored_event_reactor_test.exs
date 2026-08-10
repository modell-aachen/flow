defmodule Ariadne.Flow.Store.StoredEventReactorTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store.StoredEventReactor

  test "new/1 normalises the query" do
    assert %StoredEventReactor{
             name: "echo",
             query: %Query{items: [%Query.Item{types: ["ItemAdded"], only_last_event: false}]}
           } = new(%{query: [%{types: ["ItemAdded"]}]})
  end

  test "new/1 rejects a query asking for only the last event" do
    assert_raise ArgumentError, ~r/only_last_event/, fn ->
      new(%{query: [%{types: ["ItemAdded"], only_last_event: true}]})
    end

    assert_raise ArgumentError, ~r/only_last_event/, fn ->
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
