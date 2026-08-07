defmodule Ariadne.Flow.ConsistencyTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.Consistency
  alias Ariadne.Flow.ConsumeResult
  alias Ariadne.Flow.PostCommitError
  alias Ariadne.Flow.Reactor
  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Event.Codec
  alias Ariadne.Flow.Store.StoredEventReactor

  defmodule CountEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct count: 1

    def tags(%{count: count}), do: ["count:#{count}"]
  end

  defmodule OtherEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct []

    def tags(_), do: []
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

  defmodule TaggedSyncReactor do
    alias Ariadne.Flow.ConsistencyTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(
        %{name: "tagged-sync", filter: %{types: [CountEvent], tags: ["only:me"]}, sync: true},
        fn _event, _metadata -> :ok end
      )
    end
  end

  defmodule AsyncReactor do
    alias Ariadne.Flow.ConsistencyTest.Recorder
    def reactor, do: Recorder.reactor("async")
  end

  # Not a whole backend: the await only ever asks a store for checkpoints, so recording
  # which reactor each poll asked about is enough to see what it stopped asking about.
  defmodule CountingCheckpoints do
    alias Ariadne.Flow.Store.InMemory

    def checkpoint({agent, reporter}, name) do
      send(reporter, {:checkpoint_query, name})
      InMemory.checkpoint(agent, name)
    end
  end

  @impatient 50

  defp store_with_counts(count),
    do: store_with_events(for(n <- 1..count, do: count_store_event(n)))

  defp store_with_events(store_events) do
    store = Store.InMemory.init()
    {:ok, %{events: events}} = Store.append(store, store_events)

    {store, Enum.map(events, &Codec.deserialize/1)}
  end

  defp count_store_event(count) do
    %Store.Event{
      type: "Ariadne.Flow.ConsistencyTest.CountEvent",
      data: %{"count" => count},
      tags: ["count:#{count}"]
    }
  end

  defp tagged_store_event do
    %Store.Event{
      type: "Ariadne.Flow.ConsistencyTest.CountEvent",
      data: %{"count" => 9},
      tags: ["only:me"]
    }
  end

  defp other_store_event do
    %Store.Event{
      type: "Ariadne.Flow.ConsistencyTest.OtherEvent",
      data: %{},
      tags: []
    }
  end

  defp execute(store, reactor_module) do
    :ok = ReactorRun.execute(ReactorRun.new(%{reactor: reactor_module}), store)
  end

  describe "new/1" do
    test "awaits the sync reactors, keeping the declaration that says how far each must get" do
      assert %Consistency{reactors: [%Reactor{name: "sync"}, %Reactor{name: "other-sync"}]} =
               Consistency.new(%{reactors: [SyncReactor, AsyncReactor, OtherSyncReactor]})
    end

    test "awaits nothing when no reactor asked to be awaited" do
      assert %Consistency{reactors: []} = Consistency.new(%{reactors: [AsyncReactor]})
      assert %Consistency{reactors: []} = Consistency.new(%{reactors: []})
    end

    test "awaits nothing when the dispatch is nested in an outer transaction" do
      assert %Consistency{reactors: []} =
               Consistency.new(%{reactors: [SyncReactor], nested: true})
    end

    test "defaults the timeout when none is given and honors an explicit one" do
      assert %Consistency{timeout: 5_000} = Consistency.new(%{reactors: [SyncReactor]})

      assert %Consistency{timeout: 5_000} =
               Consistency.new(%{reactors: [SyncReactor], await_timeout: nil})

      assert %Consistency{timeout: 250} =
               Consistency.new(%{reactors: [SyncReactor], await_timeout: 250})
    end

    # Rejected here rather than at the wait: `new/1` runs before the dispatch's transaction,
    # so a bad option fails with nothing written instead of raising after the commit.
    test "rejects a timeout that is not a non-negative integer of milliseconds" do
      for timeout <- [:infinity, -1, 1.5, "5000"] do
        assert_raise ArgumentError, ~r/:await_timeout/, fn ->
          Consistency.new(%{reactors: [SyncReactor], await_timeout: timeout})
        end
      end
    end

    test "accepts a zero timeout, which confirms or gives up on the first look" do
      assert %Consistency{timeout: 0} =
               Consistency.new(%{reactors: [SyncReactor], await_timeout: 0})
    end
  end

  describe "await/3" do
    test "confirms at once when the reactor already checkpointed past the events" do
      {store, events} = store_with_counts(2)
      execute(store, SyncReactor)

      assert :ok = Consistency.await(Consistency.new(%{reactors: [SyncReactor]}), store, events)
    end

    test "confirms without reading the store when there is nothing to await" do
      {store, events} = store_with_counts(1)

      assert :ok = Consistency.await(Consistency.new(%{reactors: [AsyncReactor]}), store, events)
    end

    test "confirms when the dispatch appended no events" do
      {store, _events} = store_with_counts(1)

      assert :ok = Consistency.await(Consistency.new(%{reactors: [SyncReactor]}), store, [])
    end

    test "confirms once a deferred run advances the checkpoint" do
      {store, events} = store_with_counts(1)
      test_pid = self()

      spawn_link(fn ->
        Process.sleep(20)
        execute(store, SyncReactor)
        send(test_pid, :executed)
      end)

      assert :ok =
               Consistency.await(
                 Consistency.new(%{reactors: [SyncReactor], await_timeout: 2_000}),
                 store,
                 events
               )

      assert_received :executed
    end

    test "times out with the reactors it never confirmed and the position it awaited" do
      {store, events} = store_with_counts(2)

      assert {:error, %PostCommitError{} = error} =
               Consistency.await(
                 Consistency.new(%{
                   reactors: [SyncReactor, OtherSyncReactor],
                   await_timeout: @impatient
                 }),
                 store,
                 events
               )

      assert error.unconfirmed == [
               %{name: "sync", position: 2},
               %{name: "other-sync", position: 2}
             ]

      assert error.timeout == 50
    end

    test "times out reporting only the reactors that did not confirm" do
      {store, events} = store_with_counts(1)
      execute(store, SyncReactor)

      assert {:error, %PostCommitError{unconfirmed: [%{name: "other-sync"}]}} =
               Consistency.await(
                 Consistency.new(%{
                   reactors: [SyncReactor, OtherSyncReactor],
                   await_timeout: @impatient
                 }),
                 store,
                 events
               )
    end

    test "awaits the highest position the dispatch appended, not the lowest" do
      {store, events} = store_with_counts(3)
      checkpoint_after_first_event(store, "sync")

      assert Store.checkpoint(store, "sync") == 1

      assert {:error, %PostCommitError{unconfirmed: [%{name: "sync", position: 3}]}} =
               Consistency.await(
                 Consistency.new(%{reactors: [SyncReactor], await_timeout: @impatient}),
                 store,
                 events
               )
    end

    test "confirms a caught-up reactor whose filter misses the dispatch's last event" do
      {store, events} = store_with_events([count_store_event(1), other_store_event()])
      execute(store, SyncReactor)

      # The checkpoint stays put on the event the filter skipped, so awaiting the highest
      # position the dispatch appended would burn the whole timeout on a caught-up reactor.
      assert Store.checkpoint(store, "sync") == 1

      assert :ok =
               Consistency.await(
                 Consistency.new(%{reactors: [SyncReactor], await_timeout: @impatient}),
                 store,
                 events
               )
    end

    test "does not await a reactor that matches none of the dispatch's events" do
      {store, events} = store_with_events([other_store_event()])

      assert Store.checkpoint(store, "sync") == nil

      assert :ok =
               Consistency.await(
                 Consistency.new(%{reactors: [SyncReactor], await_timeout: @impatient}),
                 store,
                 events
               )
    end

    test "awaits only the reactors the dispatch's events concern" do
      {store, events} = store_with_events([count_store_event(1)])

      assert {:error, %PostCommitError{unconfirmed: [%{name: "sync", position: 1}]}} =
               Consistency.await(
                 Consistency.new(%{
                   reactors: [SyncReactor, TaggedSyncReactor],
                   await_timeout: @impatient
                 }),
                 store,
                 events
               )
    end

    test "awaits each reactor at its own last matching event" do
      {store, events} =
        store_with_events([tagged_store_event(), count_store_event(1), count_store_event(2)])

      assert {:error, %PostCommitError{unconfirmed: unconfirmed}} =
               Consistency.await(
                 Consistency.new(%{
                   reactors: [SyncReactor, TaggedSyncReactor],
                   await_timeout: @impatient
                 }),
                 store,
                 events
               )

      assert unconfirmed == [%{name: "sync", position: 3}, %{name: "tagged-sync", position: 1}]
    end
  end

  describe "await/3 telemetry" do
    setup do
      handler = "consistency-await-test-#{inspect(self())}"

      :ok =
        :telemetry.attach_many(
          handler,
          [
            [:ariadne, :flow, :dispatch, :await, :start],
            [:ariadne, :flow, :dispatch, :await, :stop]
          ],
          fn event, measurements, metadata, pid ->
            send(pid, {:telemetry, List.last(event), measurements, metadata})
          end,
          self()
        )

      on_exit(fn -> :telemetry.detach(handler) end)
    end

    # The migration property inline engines rely on: their checkpoint committed with the
    # events, so the await costs one round of reads and never sleeps.
    test "a reactor already caught up is confirmed in a single poll" do
      {store, events} = store_with_counts(1)
      execute(store, SyncReactor)

      assert :ok = Consistency.await(Consistency.new(%{reactors: [SyncReactor]}), store, events)

      assert_received {:telemetry, :start, _, %{awaited: [%{name: "sync", position: 1}]}}
      assert_received {:telemetry, :stop, %{polls: 1}, %{result: :confirmed}}
    end

    # Every round is a checkpoint read per unconfirmed reactor, so the interval has to grow:
    # a 200ms wait is 8 rounds while doubling from 2ms, against 20 at a fixed 10ms — and the
    # gap widens with the timeout, the default 5s being tens of rounds against five hundred.
    test "backs off between polls instead of querying at a fixed short interval" do
      {store, events} = store_with_counts(1)

      assert {:error, %PostCommitError{}} =
               Consistency.await(
                 Consistency.new(%{reactors: [SyncReactor], await_timeout: 200}),
                 store,
                 events
               )

      assert_received {:telemetry, :stop, %{polls: polls}, %{result: :timeout}}
      assert polls <= 10
    end

    test "reports the duration and the reactors that were still unconfirmed" do
      {store, events} = store_with_counts(1)

      assert {:error, _} =
               Consistency.await(
                 Consistency.new(%{
                   reactors: [SyncReactor, OtherSyncReactor],
                   await_timeout: @impatient
                 }),
                 store,
                 events
               )

      assert_received {:telemetry, :stop, %{duration: duration},
                       %{unconfirmed: [%{name: "sync"}, %{name: "other-sync"}], timeout: 50}}

      assert duration > 0
    end

    test "does not report a wait it never had to make" do
      {store, events} = store_with_events([other_store_event()])

      assert :ok = Consistency.await(Consistency.new(%{reactors: [SyncReactor]}), store, events)

      refute_received {:telemetry, _, _, _}
    end
  end

  describe "await/3 stops polling a reactor once it confirms" do
    test "asks about a confirmed reactor once, and keeps asking only about the lagging one" do
      {store, events} = store_with_events([tagged_store_event(), count_store_event(1)])

      # "tagged-sync" matches only the first event, so consuming it leaves that reactor
      # confirmed at position 1 while "sync" is still awaited at position 2.
      execute(store, TaggedSyncReactor)

      assert {:error, %PostCommitError{unconfirmed: [%{name: "sync"}]}} =
               Consistency.await(
                 Consistency.new(%{
                   reactors: [TaggedSyncReactor, SyncReactor],
                   await_timeout: @impatient
                 }),
                 counting_store(store),
                 events
               )

      assert queries_for("tagged-sync") == 1
      assert queries_for("sync") > 1
    end
  end

  describe "the timeout it gives up with" do
    test "is the post-commit error, telling the caller the events are already in the store" do
      {store, events} = store_with_counts(1)

      assert {:error, %PostCommitError{reason: :timeout} = error} =
               Consistency.await(
                 Consistency.new(%{reactors: [SyncReactor], await_timeout: @impatient}),
                 store,
                 events
               )

      assert Exception.message(error) =~ "never re-dispatch"
    end
  end

  defp counting_store(%Store{config: agent}) do
    %Store{module: CountingCheckpoints, config: {agent, self()}}
  end

  defp queries_for(name) do
    {:messages, messages} = Process.info(self(), :messages)

    Enum.count(messages, &(&1 == {:checkpoint_query, name}))
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
