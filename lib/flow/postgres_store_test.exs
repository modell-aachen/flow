defmodule Ariadne.Flow.PostgresStoreTest do
  use ExUnit.Case,
    async: true

  import Ecto.Query

  alias Ariadne.Flow.Application
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Projection
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Postgres
  alias Ariadne.Flow.Store.Record
  alias Ariadne.Flow.Test.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @prefix "postgres_store_test_schema"

  defmodule LockEvent do
    @derive Ariadne.Flow.Event
    defstruct [:worker]

    def tags(%{worker: worker}), do: [worker]
  end

  defmodule HeadReactor do
    alias Ariadne.Flow.PostgresStoreTest.LockEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(%{name: "lock-head", filter: %{types: [LockEvent]}}, fn _event, _metadata ->
        :ok
      end)
    end
  end

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
        %Record{type: "First", data: %{}, tags: []},
        %Record{type: "Second", data: %{}, tags: []},
        %Record{type: "Third", data: %{}, tags: []}
      ])

      %{events: events} = Store.read(store)

      assert ["First", "Second", "Third"] == Enum.map(events, & &1.record.type)

      positions = Enum.map(events, & &1.position)
      assert positions == Enum.sort(positions)
    end

    test "partitions events by context", %{store: store, context_store: context_store} do
      Store.append(store, [
        %Record{type: "SomeEvent", data: %{}, tags: []},
        %Record{type: "SomeEvent", data: %{}, tags: []}
      ])

      Store.append(context_store, [
        %Record{type: "SomeContextStoreEvent", data: %{}, tags: []}
      ])

      assert 2 == Store.count(store)

      assert %{events: [%{record: %{type: "SomeEvent"}}, %{record: %{type: "SomeEvent"}}]} =
               Store.read(store)

      assert %{events: [%{record: %{type: "SomeEvent"}}, %{record: %{type: "SomeEvent"}}]} =
               Store.read(store, [%{types: ["SomeEvent"]}])

      assert 1 == Store.count(context_store)
      assert %{events: [%{record: %{type: "SomeContextStoreEvent"}}]} = Store.read(context_store)

      assert %{events: [%{record: %{type: "SomeContextStoreEvent"}}]} =
               Store.read(context_store, [%{types: ["SomeContextStoreEvent"]}])
    end
  end

  # Real connections rather than the sandbox's: the point is two dispatches contending on
  # the store's append lock, which one connection cannot show.
  describe "initializing a checkpoint inside the append's critical section" do
    test "pins a reactor starting from now to the events of whichever dispatch appends first" do
      context = "init_lock_#{System.unique_integer([:positive])}"
      on_exit(fn -> unboxed(fn -> purge(context) end) end)

      test_pid = self()

      first =
        Task.async(fn ->
          unboxed(fn ->
            holding_open(context, "worker:a", test_pid)
          end)
        end)

      assert_receive {:appended, first_position}, 5_000

      second = Task.async(fn -> unboxed(fn -> nested_dispatch(context, "worker:b") end) end)

      Process.sleep(100)

      assert Task.yield(second, 0) == nil,
             "the second dispatch appended without waiting for the first to commit"

      send(first.pid, :commit)

      assert {:ok, _} = Task.await(first, 5_000)
      assert {:ok, _} = Task.await(second, 5_000)

      assert unboxed(fn -> Store.checkpoint(store(context), "lock-head") end) ==
               first_position - 1
    end
  end

  # Nested, so neither dispatch executes the reactor and the checkpoint is what the append
  # initialized it to and nothing else.
  defp holding_open(context, worker, test_pid) do
    Store.transaction(store(context), fn ->
      {:ok, %{events: [%{metadata: %{position: position}}]}} = result = dispatch(context, worker)

      send(test_pid, {:appended, position})

      receive do
        :commit -> result
      after
        5_000 -> flunk("never told to commit")
      end
    end)
  end

  defp nested_dispatch(context, worker) do
    Store.transaction(store(context), fn -> dispatch(context, worker) end)
  end

  defp dispatch(context, worker) do
    %{store: store(context), reactors: [HeadReactor]}
    |> Application.new()
    |> Application.dispatch(lock_command(worker))
  end

  defp lock_command(worker) do
    Composite.new(
      %{
        seen:
          Projection.new(
            %{initial_state: 0, filter: %{types: [LockEvent], tags: [worker]}},
            fn state, %LockEvent{}, _ -> state + 1 end
          )
      },
      fn _ -> {:ok, [%LockEvent{worker: worker}]} end
    )
  end

  defp store(context), do: Postgres.init(repo: Repo, prefix: @prefix, context: context)

  defp unboxed(fun), do: Sandbox.unboxed_run(Repo, fun)

  defp purge(context) do
    positions =
      Repo.all(from(e in "ariadne_flow_store", where: e.context == ^context, select: e.position),
        prefix: @prefix
      )

    delete_all(from(t in "ariadne_flow_store_tags", where: t.position in ^positions))
    delete_all(from(e in "ariadne_flow_store", where: e.context == ^context))

    delete_all(from(c in "ariadne_flow_store_reactor_checkpoints", where: c.context == ^context))
  end

  defp delete_all(query), do: Repo.delete_all(query, prefix: @prefix)
end
