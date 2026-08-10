defmodule Ariadne.Flow.HandoffTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.CommandHandler
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Handoff
  alias Ariadne.Flow.Projection
  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Scheduler
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Test.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true

  defmodule CountEvent do
    @derive Ariadne.Flow.Event
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

  defmodule SyncReactor do
    alias Ariadne.Flow.HandoffTest.Recorder
    def reactor, do: Recorder.reactor("sync", %{sync: true})
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

  defmodule BoomReactor do
    alias Ariadne.Flow.HandoffTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(%{name: "boom", filter: %{types: [CountEvent]}}, fn _event, _metadata ->
        {:error, :kaboom}
      end)
    end
  end

  defmodule RaisingReactor do
    alias Ariadne.Flow.HandoffTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(%{name: "raising", filter: %{types: [CountEvent]}}, fn _event, _metadata ->
        raise "kaboom"
      end)
    end
  end

  # What a handler that talks to a dead process does — an exit rather than an exception.
  defmodule ExitingReactor do
    alias Ariadne.Flow.HandoffTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(%{name: "exiting", filter: %{types: [CountEvent]}}, fn _event, _metadata ->
        exit(:noproc)
      end)
    end
  end

  defmodule ThrowingReactor do
    alias Ariadne.Flow.HandoffTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(%{name: "throwing", filter: %{types: [CountEvent]}}, fn _event, _metadata ->
        throw(:nope)
      end)
    end
  end

  # A scheduler that reports every run it is handed as durably enqueued without doing
  # anything about it — what Flow does with a run it believes is somebody else's.
  defmodule ClaimingScheduler do
    @behaviour Scheduler

    @impl Scheduler
    def schedule(reactor_runs, store, opts) do
      send(self(), {:scheduled, reactor_runs, store, opts})

      Keyword.get(opts, :schedules, reactor_runs)
    end
  end

  defmodule DecliningScheduler do
    @behaviour Scheduler

    @impl Scheduler
    def schedule(reactor_runs, _store, _opts) do
      send(self(), {:offered, reactor_runs})

      []
    end
  end

  # What an scheduler that dumps its job args and hands back what it would enqueue looks like:
  # the round trip stringifies metadata keys, so the runs come back as equal-but-not-identical
  # terms.
  defmodule RoundTrippingScheduler do
    @behaviour Scheduler

    @impl Scheduler
    def schedule(reactor_runs, _store, _opts) do
      Enum.map(reactor_runs, fn reactor_run ->
        reactor_run
        |> ReactorRun.dump()
        |> Jason.encode!()
        |> Jason.decode!()
        |> ReactorRun.load()
      end)
    end
  end

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

  # A raising reactor takes the InMemory agent down with it, so a raise can only be
  # observed on a store whose reactors run in the calling process.
  defp postgres_store do
    :ok = Sandbox.checkout(Repo)
    Process.put(:inbox, self())

    Store.Postgres.init(repo: Repo, prefix: "postgres_store_test_schema")
  end

  defp append(store, increase \\ 1) do
    {:ok, %{events: events}} =
      CommandHandler.handle(CommandHandler.new(%{command: count_command(increase)}), store)

    events
  end

  defp hand_off(reactors, store, events, attrs \\ %{}) do
    attrs
    |> Map.merge(%{reactors: reactors})
    |> Handoff.new()
    |> Handoff.hand_off(store, events)
  end

  defp drive(reactors, store, events, attrs \\ %{}) do
    reactors
    |> hand_off(store, events, attrs)
    |> Handoff.execute(store)
  end

  defp catch_up(reactors, store, attrs \\ %{}) do
    attrs
    |> Map.merge(%{reactors: reactors})
    |> Handoff.new()
    |> Handoff.catch_up(store)
    |> Handoff.execute(store)
  end

  describe "new/1" do
    test "defaults to no scheduler at all, so every run is Flow's to execute" do
      assert %Handoff{scheduler: nil, metadata: %{}, nested: false} =
               Handoff.new(%{reactors: [CountsReactor]})
    end

    test "normalizes a bare scheduler module to a {module, opts} pair" do
      assert %Handoff{scheduler: {ClaimingScheduler, []}} =
               Handoff.new(%{reactors: [CountsReactor], scheduler: ClaimingScheduler})
    end

    test "keeps an explicit {module, opts} scheduler" do
      assert %Handoff{scheduler: {ClaimingScheduler, [foo: :bar]}} =
               Handoff.new(%{
                 reactors: [CountsReactor],
                 scheduler: {ClaimingScheduler, foo: :bar}
               })
    end
  end

  describe "hand_off/3 short-circuits" do
    test "returns no runs when there are no events" do
      store = inbox_store()

      assert [] = hand_off([BoomReactor], store, [])
    end

    test "returns no runs when there are no reactors" do
      store = inbox_store()
      events = append(store)

      assert [] = hand_off([], store, events)
    end
  end

  describe "hand_off/3 with no scheduler" do
    test "hands back one run per reactor, in declaration order" do
      store = inbox_store()
      events = append(store)

      assert [%ReactorRun{reactor: AlphaReactor}, %ReactorRun{reactor: BetaReactor}] =
               hand_off([AlphaReactor, BetaReactor], store, events)
    end

    test "puts the metadata it was given on every run" do
      store = inbox_store()
      events = append(store)

      assert [%ReactorRun{metadata: %{"tenant_id" => "acme"}}] =
               hand_off([CountsReactor], store, events, %{metadata: %{"tenant_id" => "acme"}})
    end
  end

  describe "hand_off/3 initializes checkpoints" do
    test "starts a reactor that declared no position right before the dispatch's events" do
      store = inbox_store()
      _earlier = append(store)
      events = append(store)

      hand_off([CountsReactor], store, events)

      assert Store.checkpoint(store, "counts") == 1
    end

    test "starts a reactor that declared a position exactly there" do
      store = inbox_store()
      _earlier = append(store)
      events = append(store)

      hand_off([FromOriginReactor, FromPositionReactor], store, events)

      assert Store.checkpoint(store, "from-origin") == 0
      assert Store.checkpoint(store, "from-position") == 1
    end

    test "leaves a reactor that already has a checkpoint where it stands" do
      store = inbox_store()
      first = append(store)

      drive([CountsReactor], store, first)
      assert Store.checkpoint(store, "counts") == 1

      Store.append(store, [%Store.Record{type: "Unrelated", data: %{}, tags: []}])
      hand_off([CountsReactor], store, append(store))

      assert Store.checkpoint(store, "counts") == 1
    end

    # A run lost between the commit and its execution costs promptness, not events: the
    # checkpoint that says where the reactor starts was written with the events themselves.
    test "starts a reactor even when nothing ever executes its run" do
      store = inbox_store()
      _earlier = append(store)
      events = append(store)

      assert [] = drive([CountsReactor], store, events, %{scheduler: ClaimingScheduler})

      assert Store.checkpoint(store, "counts") == 1
      refute_received {:got, "counts", _, _}

      assert [] = catch_up([CountsReactor], store)

      assert_received {:got, "counts", %CountEvent{count: 2}, _}
    end
  end

  describe "execute/2" do
    test "drives every run over the events its reactor has not seen" do
      store = inbox_store()
      events = append(store)

      assert [] = drive([AlphaReactor, BetaReactor], store, events)

      assert_received {:got, "alpha", %CountEvent{count: 1}, _}
      assert_received {:got, "beta", %CountEvent{count: 1}, _}
    end

    test "drives a reactor only over the events after its checkpoint" do
      store = inbox_store()
      _earlier = append(store)
      events = append(store)

      assert [] = drive([CountsReactor], store, events)

      assert_received {:got, "counts", %CountEvent{count: 2}, _}
      refute_received {:got, "counts", %CountEvent{count: 1}, _}
    end

    test "drives a reactor that declared history over all of it" do
      store = inbox_store()
      _earlier = append(store)
      events = append(store)

      assert [] = drive([FromOriginReactor], store, events)

      assert_received {:got, "from-origin", %CountEvent{count: 1}, _}
      assert_received {:got, "from-origin", %CountEvent{count: 2}, _}
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

      assert [] = drive([CountsReactor], store, events)

      Enum.each(1..total, fn _ -> assert_received {:got, "counts", _, _} end)
      refute_received {:got, "counts", _, _}
    end

    test "reports a failed reactor with its run, name, position and reason" do
      store = inbox_store()
      events = append(store)

      assert [failure] = drive([BoomReactor], store, events)

      assert %{
               run: %ReactorRun{reactor: BoomReactor},
               name: "boom",
               position: position,
               reason: :kaboom,
               stacktrace: nil
             } = failure

      assert is_integer(position)
    end

    test "reports a raising reactor as a failure carrying the exception and its stacktrace" do
      store = postgres_store()
      events = append(store)

      assert [%{name: "raising", position: nil, kind: :error, reason: reason} = failure] =
               drive([RaisingReactor], store, events)

      assert %RuntimeError{message: "kaboom"} = reason
      assert is_list(failure.stacktrace)
    end

    # A handler talking to a dead process exits rather than raises, and an exit escaping
    # here would take an already-committed dispatch down with it.
    test "contains an exiting reactor, the way it contains a raising one" do
      store = postgres_store()
      events = append(store)

      assert [%{name: "exiting", kind: :exit, reason: :noproc, stacktrace: stacktrace}] =
               drive([ExitingReactor], store, events)

      assert is_list(stacktrace)
    end

    test "contains a throwing reactor" do
      store = postgres_store()
      events = append(store)

      assert [%{name: "throwing", kind: :throw, reason: :nope}] =
               drive([ThrowingReactor], store, events)
    end

    test "runs the reactors after one that exited" do
      store = postgres_store()
      events = append(store)

      assert [%{name: "exiting"}] = drive([ExitingReactor, AlphaReactor], store, events)

      assert_received {:got, "alpha", _, _}
    end

    test "runs every reactor declared after a failing one, in declaration order" do
      store = postgres_store()
      events = append(store)

      assert [%{name: "boom"}, %{name: "raising"}] =
               drive([BoomReactor, AlphaReactor, RaisingReactor, BetaReactor], store, events)

      assert_received {:got, "alpha", _, _}
      assert_received {:got, "beta", _, _}
    end

    test "emits telemetry for every failure it collects" do
      handler = "reactor-failure-test-#{inspect(self())}"
      test_pid = self()

      :ok =
        :telemetry.attach(
          handler,
          [:ariadne, :flow, :reactor, :failure],
          fn _event, measurements, metadata, _ ->
            send(test_pid, {:telemetry, measurements, metadata})
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(handler) end)

      store = inbox_store()
      events = append(store)

      assert [_] = drive([BoomReactor], store, events)

      assert_received {:telemetry, %{system_time: _},
                       %{name: "boom", position: position, reason: :kaboom}}

      assert is_integer(position)
    end
  end

  describe "summarize/1" do
    test "keeps only what an error carries about a failure" do
      store = inbox_store()
      events = append(store)

      assert [summary] =
               [BoomReactor]
               |> drive(store, events)
               |> Handoff.summarize()

      assert Map.keys(summary) == [:name, :position, :reason]
    end
  end

  describe "hand_off/3 offers the runs to the scheduler" do
    test "hands it every run at once, together with the store and its configured opts" do
      store = Store.InMemory.init()
      events = append(store)

      assert [] =
               hand_off([CountsReactor, AlphaReactor], store, events, %{
                 scheduler: {ClaimingScheduler, foo: :bar}
               })

      assert_received {:scheduled, runs, ^store, opts}
      assert [%ReactorRun{reactor: CountsReactor}, %ReactorRun{reactor: AlphaReactor}] = runs
      assert opts == [foo: :bar]
    end

    test "keeps the runs the scheduler did not report as scheduled" do
      store = inbox_store()
      events = append(store)

      assert [] = hand_off([CountsReactor], store, events, %{scheduler: ClaimingScheduler})

      assert [%ReactorRun{reactor: CountsReactor}] =
               hand_off([CountsReactor], store, events, %{scheduler: DecliningScheduler})

      assert_received {:offered, [%ReactorRun{reactor: CountsReactor}]}
    end

    # Reported as scheduled is reported as scheduled, however the scheduler got the value
    # back — a job scheduler that returns what it dumped and reloaded means the same thing.
    test "recognizes a claimed run the scheduler rebuilt rather than handed back" do
      store = inbox_store()
      events = append(store)

      assert [] =
               drive([CountsReactor], store, events, %{scheduler: RoundTrippingScheduler})

      refute_received {:got, "counts", _, _}
    end

    test "keeps the runs a partial scheduler left behind" do
      store = inbox_store()
      events = append(store)

      scheduled = [ReactorRun.new(%{reactor: AlphaReactor})]

      assert [%ReactorRun{reactor: CountsReactor}] =
               hand_off([CountsReactor, AlphaReactor], store, events, %{
                 scheduler: {ClaimingScheduler, schedules: scheduled}
               })
    end
  end

  describe "hand_off/3 nested in an outer transaction" do
    test "keeps the sync runs and never offers them to the scheduler" do
      store = inbox_store()
      events = append(store)

      assert [%ReactorRun{reactor: SyncReactor}] =
               hand_off([SyncReactor, CountsReactor], store, events, %{
                 scheduler: ClaimingScheduler,
                 nested: true
               })

      assert_received {:scheduled, [%ReactorRun{reactor: CountsReactor}], _store, _opts}
    end

    # The async run's work lands after the outer commit either way — through a job row the
    # scheduler inserted, or through the next dispatch or catch-up.
    test "drops an async run the scheduler did not schedule rather than executing it" do
      store = inbox_store()
      events = append(store)

      assert [] =
               drive([CountsReactor], store, events, %{
                 scheduler: DecliningScheduler,
                 nested: true
               })

      assert_received {:offered, [%ReactorRun{reactor: CountsReactor}]}
      refute_received {:got, "counts", _, _}
      assert Store.checkpoint(store, "counts") == 0
    end

    test "drops an async run with no scheduler to offer it to" do
      store = inbox_store()
      events = append(store)

      assert [] = drive([CountsReactor], store, events, %{nested: true})

      refute_received {:got, "counts", _, _}
    end
  end

  describe "catch_up/2" do
    test "returns no runs when there are no reactors" do
      store = inbox_store()
      _events = append(store)

      assert [] = catch_up([], store)
    end

    test "drives a reactor over the history it declared, with no dispatch to hand it events" do
      store = inbox_store()
      _events = append(store)
      _events = append(store)

      assert [] = catch_up([FromOriginReactor], store)

      assert_received {:got, "from-origin", %CountEvent{count: 1}, _}
      assert_received {:got, "from-origin", %CountEvent{count: 2}, _}
    end

    test "starts a declared reactor exactly where it asked, once" do
      store = inbox_store()
      _first = append(store)
      _second = append(store)

      assert [] = catch_up([FromPositionReactor], store)

      assert Store.checkpoint(store, "from-position") == 2
      assert_received {:got, "from-position", %CountEvent{count: 2}, _}
      refute_received {:got, "from-position", %CountEvent{count: 1}, _}
    end

    test "is a no-op for a reactor that is already up to date" do
      store = inbox_store()
      events = append(store)

      assert [] = drive([FromOriginReactor], store, events)
      assert_received {:got, "from-origin", %CountEvent{count: 1}, _}

      assert [] = catch_up([FromOriginReactor], store)

      refute_received {:got, "from-origin", _, _}
    end

    test "resumes a reactor started by a dispatch from the checkpoint that dispatch gave it" do
      store = inbox_store()
      events = append(store)

      assert [] = drive([CountsReactor], store, events)
      assert_received {:got, "counts", %CountEvent{count: 1}, _}

      _out_of_band = append(store)

      assert [] = catch_up([CountsReactor], store)

      assert_received {:got, "counts", %CountEvent{count: 2}, _}
    end

    test "skips a reactor no dispatch has ever started, having no position to invent" do
      store = inbox_store()
      _events = append(store)

      assert [] = catch_up([CountsReactor], store, %{scheduler: DecliningScheduler})

      refute_received {:offered, _runs}
      assert nil == Store.checkpoint(store, "counts")
    end

    test "collects the failures of the runs it executed" do
      store = inbox_store()
      _events = append(store)

      assert [%{name: "boom-from-origin", position: position, reason: :kaboom}] =
               catch_up([BoomFromOriginReactor, FromOriginReactor], store)

      assert is_integer(position)
      assert_received {:got, "from-origin", %CountEvent{count: 1}, _}
    end

    test "offers its runs to the scheduler like a dispatch does, metadata and all" do
      store = Store.InMemory.init()
      _events = append(store)

      assert [] =
               catch_up([FromOriginReactor], store, %{
                 scheduler: {ClaimingScheduler, foo: :bar},
                 metadata: %{"tenant_id" => "acme"}
               })

      assert_received {:scheduled, runs, ^store, opts}

      assert [%ReactorRun{reactor: FromOriginReactor, metadata: %{"tenant_id" => "acme"}}] = runs
      assert opts == [foo: :bar]
    end

    test "delivers each event exactly once when a hand-off and catch-ups race the same reactor" do
      store = inbox_store()
      total = 20
      events = Enum.flat_map(1..total, fn _ -> append(store) end)

      drivers = [
        fn -> drive([FromOriginReactor], store, events) end,
        fn -> catch_up([FromOriginReactor], store) end,
        fn -> catch_up([FromOriginReactor], store) end
      ]

      results =
        drivers
        |> Enum.map(&Task.async/1)
        |> Task.await_many()

      assert Enum.all?(results, &(&1 == []))

      Enum.each(1..total, fn count ->
        assert_receive {:got, "from-origin", %CountEvent{count: ^count}, _}
      end)

      refute_receive {:got, "from-origin", _, _}
      assert total == Store.checkpoint(store, "from-origin")
    end
  end
end
