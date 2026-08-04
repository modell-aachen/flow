defmodule Ariadne.Flow.HandoffTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.CommandHandler
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Handoff
  alias Ariadne.Flow.Projection
  alias Ariadne.Flow.ReactorEngine
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Store

  defmodule CountEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct count: 1

    def tags(%{count: count}), do: ["count:#{count}"]
  end

  defmodule Recorder do
    alias Ariadne.Flow.HandoffTest.CountEvent
    alias Ariadne.Flow.Reactor

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
    alias Ariadne.Flow.HandoffTest.Recorder
    def reactor, do: Recorder.reactor("counts")
  end

  defmodule AlphaReactor do
    alias Ariadne.Flow.HandoffTest.Recorder
    def reactor, do: Recorder.reactor("alpha")
  end

  defmodule BetaReactor do
    alias Ariadne.Flow.HandoffTest.Recorder
    def reactor, do: Recorder.reactor("beta")
  end

  defmodule FromOriginReactor do
    alias Ariadne.Flow.HandoffTest.Recorder
    def reactor, do: Recorder.reactor("from-origin", %{start_after_position: 0})
  end

  defmodule FromPositionReactor do
    alias Ariadne.Flow.HandoffTest.Recorder
    def reactor, do: Recorder.reactor("from-position", %{start_after_position: 1})
  end

  defmodule BoomFromOriginReactor do
    alias Ariadne.Flow.HandoffTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(
        %{name: "boom-from-origin", filter: %{types: [CountEvent]}, start_after_position: 0},
        fn _event, _metadata -> {:error, :kaboom} end
      )
    end
  end

  defmodule SyncReactor do
    alias Ariadne.Flow.HandoffTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(
        %{name: "sync", filter: %{types: [CountEvent]}, sync: true},
        fn event, metadata ->
          send(Process.get(:inbox), {:got, "sync", event, metadata})
          :ok
        end
      )
    end
  end

  defmodule BoomSyncReactor do
    alias Ariadne.Flow.HandoffTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(
        %{name: "boom-sync", filter: %{types: [CountEvent]}, sync: true},
        fn _event, _metadata -> {:error, :kaboom} end
      )
    end
  end

  defmodule BoomReactor do
    alias Ariadne.Flow.HandoffTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(%{name: "boom", filter: %{types: [CountEvent]}}, fn _event, _metadata ->
        {:error, :kaboom}
      end)
    end
  end

  defmodule RecordingEngine do
    @behaviour ReactorEngine

    @impl ReactorEngine
    def run(payload, store, opts) do
      send(self(), {:engine_run, payload, store, opts})
      Keyword.get(opts, :result, :ok)
    end
  end

  defmodule DeferringEngine do
    @behaviour ReactorEngine

    @impl ReactorEngine
    def run(reactor_run, store, _opts) do
      if ReactorRun.sync?(reactor_run) do
        ReactorRun.execute(reactor_run, store)
      else
        send(self(), {:deferred, reactor_run})
        :ok
      end
    end
  end

  @inline {ReactorEngine.Inline, []}

  defp num_counts_projection do
    Projection.new(
      %{initial_state: 0, filter: %{types: [CountEvent]}},
      fn state, %CountEvent{}, _ -> state + 1 end
    )
  end

  defp count_command(increase) do
    Composite.new(
      %{count: num_counts_projection()},
      fn %{count: count} -> {:ok, [%CountEvent{count: count + increase}]} end
    )
  end

  defp inbox_store do
    store = Store.InMemory.init()
    test_pid = self()

    Agent.update(store.config, fn state ->
      Process.put(:inbox, test_pid)
      state
    end)

    store
  end

  defp append(store, increase \\ 1) do
    {:ok, %{events: events}} =
      CommandHandler.handle(CommandHandler.new(%{command: count_command(increase)}), store)

    events
  end

  defp hand_off(reactors, store, events, engine \\ @inline, attrs \\ %{}) do
    attrs
    |> Map.merge(%{reactors: reactors, engine: engine})
    |> Handoff.new()
    |> Handoff.hand_off(store, events)
  end

  defp catch_up(reactors, store, engine \\ @inline, attrs \\ %{}) do
    attrs
    |> Map.merge(%{reactors: reactors, engine: engine})
    |> Handoff.new()
    |> Handoff.catch_up(store)
  end

  describe "new/1" do
    test "normalizes a bare engine module to a {module, opts} pair" do
      assert %Handoff{engine: {RecordingEngine, []}} =
               Handoff.new(%{reactors: [CountsReactor], engine: RecordingEngine})
    end

    test "keeps an explicit {module, opts} engine, defaulting metadata and nested" do
      assert %Handoff{engine: {RecordingEngine, [foo: :bar]}, metadata: %{}, nested: false} =
               Handoff.new(%{reactors: [CountsReactor], engine: {RecordingEngine, foo: :bar}})
    end
  end

  describe "hand_off/3 short-circuits" do
    test "returns :ok without touching reactors when there are no events" do
      store = inbox_store()

      assert :ok = hand_off([BoomReactor], store, [])
    end

    test "returns :ok when there are no reactors" do
      store = inbox_store()
      events = append(store)

      assert :ok = hand_off([], store, events)
    end
  end

  describe "hand_off/3 drives reactors with only a store, reactors and events" do
    test "runs every reactor inline under the default engine over the given events" do
      store = inbox_store()
      events = append(store)

      assert :ok =
               hand_off([AlphaReactor, BetaReactor], store, events)

      assert_received {:got, "alpha", %CountEvent{count: 1}, _}
      assert_received {:got, "beta", %CountEvent{count: 1}, _}
    end

    test "drives reactors only over the events it is given, not earlier ones" do
      store = inbox_store()
      _earlier = append(store)
      events = append(store)

      assert :ok = hand_off([CountsReactor], store, events)

      assert_received {:got, "counts", %CountEvent{count: 2}, _}
      refute_received {:got, "counts", %CountEvent{count: 1}, _}
    end

    test "drives reactors across batch boundaries until fully drained" do
      batch_size = 100
      total = batch_size + 5
      store = inbox_store()

      bulk_command =
        Composite.new(
          %{count: num_counts_projection()},
          fn _ -> {:ok, for(_ <- 1..total, do: %CountEvent{count: 1})} end
        )

      {:ok, %{events: events}} =
        CommandHandler.handle(CommandHandler.new(%{command: bulk_command}), store)

      assert :ok = hand_off([CountsReactor], store, events)

      Enum.each(1..total, fn _ -> assert_received {:got, "counts", _, _} end)
      refute_received {:got, "counts", _, _}
    end
  end

  describe "hand_off/3 resolves the reactor's start_after_position" do
    test "starts a :head reactor right before the events it is handed" do
      store = Store.InMemory.init()
      _earlier = append(store)
      events = append(store)

      assert :ok = hand_off([CountsReactor], store, events, {RecordingEngine, result: :ok})

      assert_received {:engine_run, %ReactorRun{start_after_position: 1}, _store, _opts}
    end

    test "passes a declared position through, so a first run picks up the history it asked for" do
      store = inbox_store()
      _earlier = append(store)
      events = append(store)

      assert :ok = hand_off([FromOriginReactor], store, events)

      assert_received {:got, "from-origin", %CountEvent{count: 1}, _}
      assert_received {:got, "from-origin", %CountEvent{count: 2}, _}
    end

    test "starts a declared position where the reactor asked, not where the dispatch begins" do
      store = inbox_store()
      _earlier = append(store)
      _earlier = append(store)
      events = append(store)

      assert :ok = hand_off([FromPositionReactor], store, events)

      assert_received {:got, "from-position", %CountEvent{count: 2}, _}
      assert_received {:got, "from-position", %CountEvent{count: 3}, _}
      refute_received {:got, "from-position", %CountEvent{count: 1}, _}
    end
  end

  describe "catch_up/2" do
    test "returns :ok when there are no reactors" do
      store = inbox_store()
      _events = append(store)

      assert :ok = catch_up([], store)
    end

    test "drives a reactor over the history it declared, with no dispatch to hand it events" do
      store = inbox_store()
      _events = append(store)
      _events = append(store)

      assert :ok = catch_up([FromOriginReactor], store)

      assert_received {:got, "from-origin", %CountEvent{count: 1}, _}
      assert_received {:got, "from-origin", %CountEvent{count: 2}, _}
    end

    test "is a no-op for a reactor that is already up to date" do
      store = inbox_store()
      events = append(store)

      assert :ok = hand_off([FromOriginReactor], store, events)
      assert_received {:got, "from-origin", %CountEvent{count: 1}, _}

      assert :ok = catch_up([FromOriginReactor], store)

      refute_received {:got, "from-origin", _, _}
    end

    test "resumes a :head reactor from its checkpoint once a dispatch has run it" do
      store = inbox_store()
      events = append(store)

      assert :ok = hand_off([CountsReactor], store, events)
      assert_received {:got, "counts", %CountEvent{count: 1}, _}

      _out_of_band = append(store)

      assert :ok = catch_up([CountsReactor], store)

      assert_received {:got, "counts", %CountEvent{count: 2}, _}
    end

    test "skips a checkpoint-less :head reactor, which has nothing to catch up on yet" do
      store = inbox_store()
      _events = append(store)

      assert :ok = catch_up([CountsReactor], store, {RecordingEngine, result: :ok})

      refute_received {:engine_run, _reactor_run, _store, _opts}
      assert nil == Store.checkpoint(store, "counts")
    end

    test "collects failures into an error value rather than raising" do
      store = inbox_store()
      _events = append(store)

      assert {:error,
              %ReactorError{
                failures: [%{name: "boom-from-origin", position: position, reason: :kaboom}]
              }} = catch_up([BoomFromOriginReactor, FromOriginReactor], store)

      assert is_integer(position)
      assert_received {:got, "from-origin", %CountEvent{count: 1}, _}
    end

    test "hands the engine one run per reactor, carrying the metadata and nesting it is given" do
      store = Store.InMemory.init()
      _events = append(store)

      assert :ok =
               catch_up(
                 [FromOriginReactor],
                 store,
                 {RecordingEngine, result: :ok, foo: :bar},
                 %{metadata: %{"tenant_id" => "acme"}, nested: true}
               )

      assert_received {:engine_run, reactor_run, ^store, opts}

      assert %ReactorRun{
               reactor: FromOriginReactor,
               start_after_position: 0,
               metadata: %{"tenant_id" => "acme"},
               nested: true
             } = reactor_run

      assert opts == [result: :ok, foo: :bar]
    end

    test "delivers each event exactly once when a hand-off and catch-ups race the same reactor" do
      store = inbox_store()
      total = 20
      events = Enum.flat_map(1..total, fn _ -> append(store) end)

      drivers = [
        fn -> hand_off([FromOriginReactor], store, events) end,
        fn -> catch_up([FromOriginReactor], store) end,
        fn -> catch_up([FromOriginReactor], store) end
      ]

      results =
        drivers
        |> Enum.map(&Task.async/1)
        |> Task.await_many()

      assert Enum.all?(results, &(&1 == :ok))

      Enum.each(1..total, fn count ->
        assert_receive {:got, "from-origin", %CountEvent{count: ^count}, _}
      end)

      refute_receive {:got, "from-origin", _, _}
      assert total == Store.checkpoint(store, "from-origin")
    end
  end

  describe "hand_off/3 continues past a failure" do
    test "runs the reactors after a failing one, in declaration order" do
      store = inbox_store()
      events = append(store)

      assert {:error, %ReactorError{failures: [%{name: "boom"}]}} =
               hand_off(
                 [BoomReactor, AlphaReactor, BetaReactor],
                 store,
                 events
               )

      assert_received {:got, "alpha", _, _}
      assert_received {:got, "beta", _, _}
    end

    test "collects every failure of the pass into one error, in declaration order" do
      store = inbox_store()
      events = append(store)

      assert {:error, %ReactorError{failures: failures}} =
               hand_off(
                 [BoomReactor, AlphaReactor, BoomSyncReactor],
                 store,
                 events
               )

      assert [%{name: "boom", reason: :kaboom}, %{name: "boom-sync", reason: :kaboom}] = failures
      assert_received {:got, "alpha", _, _}
    end

    test "reports a reactor failure with its name, position and reason" do
      store = inbox_store()
      events = append(store)

      assert {:error,
              %ReactorError{failures: [%{name: "boom", position: position, reason: :kaboom}]}} =
               hand_off([BoomReactor], store, events)

      assert is_integer(position)
    end
  end

  describe "hand_off/3 leaves the sync/async decision to the engine" do
    test "hands sync and async reactors alike to the engine" do
      store = inbox_store()
      events = append(store)

      assert :ok =
               hand_off(
                 [SyncReactor, CountsReactor],
                 store,
                 events,
                 {RecordingEngine, result: :ok}
               )

      assert_received {:engine_run, %ReactorRun{reactor: SyncReactor}, _, _}
      assert_received {:engine_run, %ReactorRun{reactor: CountsReactor}, _, _}
      refute_received {:got, "sync", _, _}
    end

    test "a sync reactor's failure fails the pass under an engine that would defer it" do
      store = inbox_store()
      events = append(store)

      assert {:error, %ReactorError{failures: [%{name: "boom-sync", reason: :kaboom}]}} =
               hand_off(
                 [BoomSyncReactor, CountsReactor],
                 store,
                 events,
                 DeferringEngine
               )
    end

    test "still defers the async reactors declared after a failing sync one" do
      store = inbox_store()
      events = append(store)

      assert {:error, %ReactorError{}} =
               hand_off(
                 [BoomSyncReactor, CountsReactor],
                 store,
                 events,
                 DeferringEngine
               )

      assert_received {:deferred, %ReactorRun{reactor: CountsReactor}}
    end
  end

  describe "hand_off/3 engine contract" do
    test "accepts a bare engine module, normalizing it to a {module, opts} pair" do
      store = Store.InMemory.init()
      events = append(store)

      assert :ok = hand_off([CountsReactor], store, events, RecordingEngine)

      assert_received {:engine_run, %ReactorRun{reactor: CountsReactor}, ^store, []}
    end

    test "hands the engine one run on the store per reactor plus its configured opts" do
      store = Store.InMemory.init()
      events = append(store)

      assert :ok =
               hand_off(
                 [CountsReactor],
                 store,
                 events,
                 {RecordingEngine, result: :ok, foo: :bar}
               )

      assert_received {:engine_run, reactor_run, ^store, opts}
      assert %ReactorRun{reactor: CountsReactor, start_after_position: 0} = reactor_run
      assert opts == [result: :ok, foo: :bar]
    end

    test "puts the metadata on the run handed to the engine" do
      store = Store.InMemory.init()
      events = append(store)

      assert :ok =
               hand_off(
                 [CountsReactor],
                 store,
                 events,
                 {RecordingEngine, result: :ok},
                 %{metadata: %{"tenant_id" => "acme", "trace_id" => "abc123"}}
               )

      assert_received {:engine_run,
                       %ReactorRun{metadata: %{"tenant_id" => "acme", "trace_id" => "abc123"}},
                       _store, _opts}
    end

    test "an engine that returns :ok without running the reactor does not fail the pass" do
      store = inbox_store()
      events = append(store)

      assert :ok =
               hand_off(
                 [CountsReactor],
                 store,
                 events,
                 {RecordingEngine, result: :ok}
               )

      assert_received {:engine_run, _payload, _store, _opts}
      refute_received {:got, "counts", _, _}
    end

    test "an engine error fails the pass as a typed reactor failure, one per failing run" do
      store = Store.InMemory.init()
      events = append(store)

      assert {:error,
              %ReactorError{
                failures: [
                  %{name: "counts", position: nil, reason: :engine_boom},
                  %{name: "alpha", position: nil, reason: :engine_boom}
                ]
              }} =
               hand_off(
                 [CountsReactor, AlphaReactor],
                 store,
                 events,
                 {RecordingEngine, result: {:error, :engine_boom}}
               )

      assert_received {:engine_run, %ReactorRun{reactor: CountsReactor}, _, _}
      assert_received {:engine_run, %ReactorRun{reactor: AlphaReactor}, _, _}
    end
  end
end
