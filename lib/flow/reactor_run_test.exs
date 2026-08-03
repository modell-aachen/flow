defmodule Ariadne.Flow.ReactorRunTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Store

  defmodule CountEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct count: 1

    def tags(%{count: count}), do: ["count:#{count}"]
  end

  defmodule UnrelatedEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct []

    def tags(_), do: []
  end

  defmodule Recorder do
    alias Ariadne.Flow.Reactor
    alias Ariadne.Flow.ReactorRunTest.CountEvent

    def reactor(name, attrs \\ %{}) do
      Reactor.new(
        Map.merge(%{name: name, filter: %{types: [CountEvent]}}, attrs),
        fn event, metadata ->
          send(Process.get(:inbox), {:got, name, event, metadata})
          :ok
        end
      )
    end
  end

  defmodule CountsReactor do
    alias Ariadne.Flow.ReactorRunTest.Recorder
    def reactor, do: Recorder.reactor("counts")
  end

  defmodule AlphaReactor do
    alias Ariadne.Flow.ReactorRunTest.Recorder
    def reactor, do: Recorder.reactor("alpha")
  end

  defmodule BetaReactor do
    alias Ariadne.Flow.ReactorRunTest.Recorder
    def reactor, do: Recorder.reactor("beta")
  end

  defmodule SyncReactor do
    alias Ariadne.Flow.ReactorRunTest.Recorder
    def reactor, do: Recorder.reactor("sync", %{sync: true})
  end

  defmodule SeededReactor do
    alias Ariadne.Flow.ReactorRunTest.Recorder
    def reactor, do: Recorder.reactor("seeded", %{start_after_position: 1})
  end

  defmodule RejectsZeroReactor do
    alias Ariadne.Flow.Reactor
    alias Ariadne.Flow.ReactorRunTest.CountEvent

    def reactor do
      Reactor.new(%{name: "rejects-zero", filter: %{types: [CountEvent]}}, fn
        %CountEvent{count: 0}, _metadata -> {:error, :zero_not_allowed}
        _event, _metadata -> :ok
      end)
    end
  end

  defmodule AcceptsAllReactor do
    alias Ariadne.Flow.Reactor
    alias Ariadne.Flow.ReactorRunTest.CountEvent

    def reactor do
      Reactor.new(
        %{name: "rejects-zero", filter: %{types: [CountEvent]}},
        fn _event, _metadata -> :ok end
      )
    end
  end

  # An InMemory store whose agent process knows the test pid, so module reactors
  # (whose handlers run inside that agent) can report back via Process.get(:inbox).
  defp inbox_store do
    store = Store.InMemory.init()
    test_pid = self()

    Agent.update(store.config, fn state ->
      Process.put(:inbox, test_pid)
      state
    end)

    store
  end

  defp run(reactor_module, attrs \\ %{}) do
    ReactorRun.new(Map.merge(%{reactor: reactor_module}, attrs))
  end

  describe "new/1" do
    test "builds a storeless run from a reactor module, seeding from the reactor's defaults" do
      assert %ReactorRun{
               reactor: CountsReactor,
               start_after_position: 0,
               metadata: %{}
             } = ReactorRun.new(%{reactor: CountsReactor})
    end

    test "defaults start_after_position to the reactor's own declaration" do
      assert %ReactorRun{start_after_position: 1} = ReactorRun.new(%{reactor: SeededReactor})
    end

    test "honors an explicit start_after_position" do
      assert %ReactorRun{start_after_position: 5} =
               ReactorRun.new(%{reactor: CountsReactor, start_after_position: 5})
    end

    test "carries the dispatch metadata it is given" do
      assert %ReactorRun{metadata: %{"tenant_id" => "acme"}} =
               ReactorRun.new(%{reactor: CountsReactor, metadata: %{"tenant_id" => "acme"}})
    end

    test "is not nested unless the dispatch says it is" do
      assert %ReactorRun{nested: false} = ReactorRun.new(%{reactor: CountsReactor})
      assert %ReactorRun{nested: true} = ReactorRun.new(%{reactor: CountsReactor, nested: true})
    end
  end

  describe "sync?/1" do
    test "reads the sync intent off the run's reactor" do
      assert ReactorRun.sync?(run(SyncReactor))
      refute ReactorRun.sync?(run(CountsReactor))
    end

    test "does not depend on the run being nested" do
      assert ReactorRun.sync?(run(SyncReactor, %{nested: true}))
    end
  end

  describe "inline?/1" do
    test "is true only for a sync run of a nested dispatch, the one that cannot be deferred" do
      assert ReactorRun.inline?(run(SyncReactor, %{nested: true}))
    end

    test "is false for a sync run the engine may defer and await" do
      refute ReactorRun.inline?(run(SyncReactor))
    end

    test "is false for an async run, nested or not" do
      refute ReactorRun.inline?(run(CountsReactor))
      refute ReactorRun.inline?(run(CountsReactor, %{nested: true}))
    end
  end

  describe "dump/1 and load/1" do
    test "dumps a run to a store-free, string-keyed payload and loads it back" do
      reactor_run =
        run(CountsReactor, %{start_after_position: 3, metadata: %{"tenant_id" => "acme"}})

      assert %{
               "reactor" => "Elixir.Ariadne.Flow.ReactorRunTest.CountsReactor",
               "start_after_position" => 3,
               "metadata" => %{"tenant_id" => "acme"}
             } = payload = ReactorRun.dump(reactor_run)

      refute Map.has_key?(payload, "store")
      assert ReactorRun.load(payload) == reactor_run
    end

    test "loads a payload carrying extra keys, so a worker can pass its whole job args" do
      payload =
        CountsReactor
        |> run()
        |> ReactorRun.dump()
        |> Map.merge(%{"store" => %{"module" => "x"}, "sync" => false, "tenant_id" => "acme"})

      assert %ReactorRun{reactor: CountsReactor} = ReactorRun.load(payload)
    end

    test "drops the nesting flag, which belongs to the dispatch and not to the execution" do
      payload = ReactorRun.dump(run(SyncReactor, %{nested: true}))

      refute Map.has_key?(payload, "nested")
      assert %ReactorRun{nested: false} = loaded = ReactorRun.load(payload)
      refute ReactorRun.inline?(loaded)
    end

    test "loads a metadata-less payload with empty metadata" do
      payload =
        CountsReactor
        |> run()
        |> ReactorRun.dump()
        |> Map.delete("metadata")

      assert %ReactorRun{reactor: CountsReactor, metadata: %{}} = ReactorRun.load(payload)
    end
  end

  describe "execute/2" do
    test "drives the reactor against the store supplied at execution time" do
      store = inbox_store()
      Store.append(store, [count_store_event(1)])

      assert :ok = ReactorRun.execute(run(CountsReactor), store)

      assert_received {:got, "counts", %CountEvent{count: 1}, _}
    end

    test "delivers deserialized events to the reactor handler in order" do
      store = inbox_store()
      Store.append(store, [count_store_event(1), count_store_event(2)])

      assert :ok = ReactorRun.execute(run(CountsReactor), store)

      assert_received {:got, "counts", %CountEvent{count: 1},
                       %{created_at: %DateTime{}, position: first_position}}

      assert_received {:got, "counts", %CountEvent{count: 2},
                       %{created_at: %DateTime{}, position: second_position}}

      assert first_position < second_position
    end

    test "skips events that do not match the reactor filter" do
      store = inbox_store()

      Store.append(store, [
        unrelated_store_event(),
        count_store_event(7),
        unrelated_store_event()
      ])

      assert :ok = ReactorRun.execute(run(CountsReactor), store)

      assert_received {:got, "counts", %CountEvent{count: 7}, _}
      refute_received {:got, "counts", %UnrelatedEvent{}, _}
    end

    test "processes newly appended events on a subsequent run" do
      store = inbox_store()
      Store.append(store, [count_store_event(1)])

      assert :ok = ReactorRun.execute(run(CountsReactor), store)
      assert_received {:got, "counts", %CountEvent{count: 1}, _}

      Store.append(store, [count_store_event(2)])

      assert :ok = ReactorRun.execute(run(CountsReactor), store)
      assert_received {:got, "counts", %CountEvent{count: 2}, _}
    end

    test "does not reprocess events after draining" do
      store = inbox_store()
      Store.append(store, [count_store_event(1)])

      assert :ok = ReactorRun.execute(run(CountsReactor), store)
      assert_received {:got, "counts", %CountEvent{count: 1}, _}

      assert :ok = ReactorRun.execute(run(CountsReactor), store)
      refute_received {:got, "counts", _, _}
    end

    test "isolates positions across reactor names" do
      store = inbox_store()
      Store.append(store, [count_store_event(1)])

      assert :ok = ReactorRun.execute(run(AlphaReactor), store)
      assert :ok = ReactorRun.execute(run(BetaReactor), store)

      assert_received {:got, "alpha", %CountEvent{count: 1}, _}
      assert_received {:got, "beta", %CountEvent{count: 1}, _}
    end

    test "halts at the first handler error and resumes once the failure is fixed" do
      store = inbox_store()

      Store.append(store, [
        count_store_event(1),
        count_store_event(0),
        count_store_event(2)
      ])

      assert {:error,
              %ReactorError{
                failures: [%{name: "rejects-zero", position: position, reason: :zero_not_allowed}]
              }} =
               ReactorRun.execute(run(RejectsZeroReactor), store)

      assert is_integer(position)

      assert :ok = ReactorRun.execute(run(AcceptsAllReactor), store)
    end

    test "starts after start_after_position, skipping earlier events" do
      store = inbox_store()

      Store.append(store, [
        count_store_event(1),
        count_store_event(2),
        count_store_event(3)
      ])

      assert :ok = ReactorRun.execute(run(CountsReactor, %{start_after_position: 2}), store)

      assert_received {:got, "counts", %CountEvent{count: 3}, _}
      refute_received {:got, "counts", _, _}
    end

    test "start_after_position only seeds the first run; later runs resume from the checkpoint" do
      store = inbox_store()
      Store.append(store, [count_store_event(1), count_store_event(2)])

      reactor_run = run(CountsReactor, %{start_after_position: 1})

      assert :ok = ReactorRun.execute(reactor_run, store)
      assert_received {:got, "counts", %CountEvent{count: 2}, _}
      refute_received {:got, "counts", %CountEvent{count: 1}, _}

      Store.append(store, [count_store_event(3)])

      assert :ok = ReactorRun.execute(reactor_run, store)
      assert_received {:got, "counts", %CountEvent{count: 3}, _}
      refute_received {:got, "counts", %CountEvent{count: 2}, _}
    end

    test "drives across batch boundaries until fully drained" do
      store = inbox_store()
      total = 105
      Store.append(store, for(n <- 1..total, do: count_store_event(n)))

      assert :ok = ReactorRun.execute(run(CountsReactor), store)

      Enum.each(1..total, fn _ -> assert_received {:got, "counts", _, _} end)
      refute_received {:got, "counts", _, _}
    end

    test "executes a run rebuilt from its payload" do
      store = inbox_store()
      Store.append(store, [count_store_event(1), count_store_event(2)])

      reactor_run =
        CountsReactor
        |> run()
        |> ReactorRun.dump()
        |> ReactorRun.load()

      assert :ok = ReactorRun.execute(reactor_run, store)

      assert_received {:got, "counts", %CountEvent{count: 1}, _}
      assert_received {:got, "counts", %CountEvent{count: 2}, _}
    end
  end

  defp count_store_event(count) do
    %Store.Event{
      type: "Ariadne.Flow.ReactorRunTest.CountEvent",
      data: %{"count" => count},
      tags: ["count:#{count}"]
    }
  end

  defp unrelated_store_event do
    %Store.Event{
      type: "Ariadne.Flow.ReactorRunTest.UnrelatedEvent",
      data: %{},
      tags: []
    }
  end
end
