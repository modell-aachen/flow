defmodule Ariadne.Flow.ConsistencyTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.Consistency
  alias Ariadne.Flow.ConsistencyTimeoutError
  alias Ariadne.Flow.ConsumeResult
  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Event.Codec
  alias Ariadne.Flow.Store.StoredEventReactor

  defmodule CountEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct count: 1

    def tags(%{count: count}), do: ["count:#{count}"]
  end

  defmodule Recorder do
    alias Ariadne.Flow.ConsistencyTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor(name, attrs \\ %{}) do
      Reactor.new(
        Map.merge(%{name: name, filter: %{types: [CountEvent]}}, attrs),
        fn _event, _metadata -> :ok end
      )
    end
  end

  defmodule SyncReactor do
    alias Ariadne.Flow.ConsistencyTest.Recorder
    def reactor, do: Recorder.reactor("sync", %{sync: true})
  end

  defmodule OtherSyncReactor do
    alias Ariadne.Flow.ConsistencyTest.Recorder
    def reactor, do: Recorder.reactor("other-sync", %{sync: true})
  end

  defmodule AsyncReactor do
    alias Ariadne.Flow.ConsistencyTest.Recorder
    def reactor, do: Recorder.reactor("async")
  end

  @impatient [await_timeout: 50]

  defp store_with_events(count) do
    store = Store.InMemory.init()

    {:ok, %{events: events}} =
      Store.append(store, for(n <- 1..count, do: count_store_event(n)))

    {store, Enum.map(events, &Codec.deserialize/1)}
  end

  defp count_store_event(count) do
    %Store.Event{
      type: "Ariadne.Flow.ConsistencyTest.CountEvent",
      data: %{"count" => count},
      tags: ["count:#{count}"]
    }
  end

  defp execute(store, reactor_module) do
    :ok = ReactorRun.execute(ReactorRun.new(%{reactor: reactor_module}), store)
  end

  describe "new/3" do
    test "awaits the sync reactors, named by the name their checkpoint is keyed on" do
      assert %Consistency{reactors: ["sync", "other-sync"]} =
               Consistency.new([SyncReactor, AsyncReactor, OtherSyncReactor], false, [])
    end

    test "awaits nothing when no reactor asked to be awaited" do
      assert %Consistency{reactors: []} = Consistency.new([AsyncReactor], false, [])
      assert %Consistency{reactors: []} = Consistency.new([], false, [])
    end

    test "awaits nothing when the dispatch is nested in an outer transaction" do
      assert %Consistency{reactors: []} = Consistency.new([SyncReactor], true, [])
    end

    test "defaults the timeout and honors an explicit await_timeout" do
      assert %Consistency{timeout: 5_000} = Consistency.new([SyncReactor], false, [])

      assert %Consistency{timeout: 250} =
               Consistency.new([SyncReactor], false, await_timeout: 250)
    end
  end

  describe "await/3" do
    test "confirms at once when the reactor already checkpointed past the events" do
      {store, events} = store_with_events(2)
      execute(store, SyncReactor)

      assert :ok = Consistency.await(Consistency.new([SyncReactor], false, []), store, events)
    end

    test "confirms without reading the store when there is nothing to await" do
      {store, events} = store_with_events(1)

      assert :ok = Consistency.await(Consistency.new([AsyncReactor], false, []), store, events)
    end

    test "confirms when the dispatch appended no events" do
      {store, _events} = store_with_events(1)

      assert :ok = Consistency.await(Consistency.new([SyncReactor], false, []), store, [])
    end

    test "confirms once a deferred run advances the checkpoint" do
      {store, events} = store_with_events(1)
      test_pid = self()

      spawn_link(fn ->
        Process.sleep(20)
        execute(store, SyncReactor)
        send(test_pid, :executed)
      end)

      assert :ok =
               Consistency.await(
                 Consistency.new([SyncReactor], false, await_timeout: 2_000),
                 store,
                 events
               )

      assert_received :executed
    end

    test "times out with the reactors it never confirmed and the position it awaited" do
      {store, events} = store_with_events(2)

      assert {:error, %ConsistencyTimeoutError{} = error} =
               Consistency.await(
                 Consistency.new([SyncReactor, OtherSyncReactor], false, @impatient),
                 store,
                 events
               )

      assert error.reactors == ["sync", "other-sync"]
      assert error.position == 2
      assert error.timeout == 50
    end

    test "times out reporting only the reactors that did not confirm" do
      {store, events} = store_with_events(1)
      execute(store, SyncReactor)

      assert {:error, %ConsistencyTimeoutError{reactors: ["other-sync"]}} =
               Consistency.await(
                 Consistency.new([SyncReactor, OtherSyncReactor], false, @impatient),
                 store,
                 events
               )
    end

    test "awaits the highest position the dispatch appended, not the lowest" do
      {store, events} = store_with_events(3)
      checkpoint_after_first_event(store, "sync")

      assert Store.checkpoint(store, "sync") == 1

      assert {:error, %ConsistencyTimeoutError{reactors: ["sync"], position: 3}} =
               Consistency.await(
                 Consistency.new([SyncReactor], false, @impatient),
                 store,
                 events
               )
    end
  end

  describe "the timeout exception" do
    test "says the events are committed and must not be dispatched again" do
      message =
        Exception.message(%ConsistencyTimeoutError{
          reactors: ["sync"],
          position: 7,
          timeout: 5_000
        })

      assert message =~ ~s(the sync reactor "sync" did not reach position 7 within 5000ms)
      assert message =~ "committed"
      assert message =~ "durably scheduled"
      assert message =~ "never re-dispatch"
    end

    test "names every unconfirmed reactor" do
      message =
        Exception.message(%ConsistencyTimeoutError{
          reactors: ["sync", "other-sync"],
          position: 7,
          timeout: 5_000
        })

      assert message =~ ~s(the sync reactors "sync", "other-sync")
    end
  end

  # A checkpoint that exists but sits behind the dispatch's last event — treating a
  # present checkpoint as confirmation would slip past a "no checkpoint yet" test.
  defp checkpoint_after_first_event(store, name) do
    %ConsumeResult{processed: 1} =
      Store.consume(
        store,
        StoredEventReactor.new(%{
          name: name,
          query: [%{types: [CountEvent]}],
          handler: fn _events -> {:ok, 1} end
        })
      )
  end
end
