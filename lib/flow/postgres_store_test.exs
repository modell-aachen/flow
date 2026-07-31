defmodule Ariadne.Flow.PostgresStoreTest do
  use ExUnit.Case,
    async: true

  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Event
  alias Ecto.Adapters.SQL.Sandbox

  setup do
    repo = Ariadne.Flow.Test.Repo
    :ok = Sandbox.checkout(repo)

    store = Store.Postgres.init(repo: repo, prefix: "postgres_store_test_schema")

    context_store =
      Store.Postgres.init(
        repo: repo,
        prefix: "postgres_store_test_schema",
        context: "context"
      )

    %{store: store, context_store: context_store}
  end

  describe "The postgres event store" do
    test "reads all events ordered by position", %{store: store} do
      Store.append(store, [
        %Event{type: "First", data: %{}, tags: []},
        %Event{type: "Second", data: %{}, tags: []},
        %Event{type: "Third", data: %{}, tags: []}
      ])

      %{events: events} = Store.read(store)

      assert ["First", "Second", "Third"] == Enum.map(events, & &1.event.type)

      positions = Enum.map(events, & &1.position)
      assert positions == Enum.sort(positions)
    end

    test "partitions events by context", %{store: store, context_store: context_store} do
      Store.append(store, [
        %Event{type: "SomeEvent", data: %{}, tags: []},
        %Event{type: "SomeEvent", data: %{}, tags: []}
      ])

      Store.append(context_store, [
        %Event{type: "SomeContextStoreEvent", data: %{}, tags: []}
      ])

      assert 2 == Store.count(store)

      assert %{events: [%{event: %{type: "SomeEvent"}}, %{event: %{type: "SomeEvent"}}]} =
               Store.read(store)

      assert %{events: [%{event: %{type: "SomeEvent"}}, %{event: %{type: "SomeEvent"}}]} =
               Store.read(store, [%{types: ["SomeEvent"]}])

      assert 1 == Store.count(context_store)
      assert %{events: [%{event: %{type: "SomeContextStoreEvent"}}]} = Store.read(context_store)

      assert %{events: [%{event: %{type: "SomeContextStoreEvent"}}]} =
               Store.read(context_store, [%{types: ["SomeContextStoreEvent"]}])
    end
  end
end
