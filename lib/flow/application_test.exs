defmodule Ariadne.Flow.ApplicationTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.AppendConditionError
  alias Ariadne.Flow.Application
  alias Ariadne.Flow.CommandError
  alias Ariadne.Flow.CommandHandler
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.ConsistencyTimeoutError
  alias Ariadne.Flow.Projection
  alias Ariadne.Flow.ReactorEngine
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Test.Repo
  alias Ecto.Adapters.SQL.Sandbox

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

  defmodule CountsReactor do
    alias Ariadne.Flow.ApplicationTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(%{name: "counts", filter: %{types: [CountEvent]}}, fn event, metadata ->
        send(Process.get(:inbox), {:got, "counts", event, metadata})
        :ok
      end)
    end
  end

  defmodule BoomReactor do
    alias Ariadne.Flow.ApplicationTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(%{name: "boom", filter: %{types: [CountEvent]}}, fn _event, _metadata ->
        {:error, :kaboom}
      end)
    end
  end

  defmodule HistoryReactor do
    alias Ariadne.Flow.ApplicationTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(
        %{name: "history", filter: %{types: [CountEvent]}, start_after_position: 0},
        fn event, metadata ->
          send(Process.get(:inbox), {:got, "history", event, metadata})
          :ok
        end
      )
    end
  end

  defmodule BoomHistoryReactor do
    alias Ariadne.Flow.ApplicationTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(
        %{name: "boom-history", filter: %{types: [CountEvent]}, start_after_position: 0},
        fn _event, _metadata -> {:error, :kaboom} end
      )
    end
  end

  defmodule AlsoBoomReactor do
    alias Ariadne.Flow.ApplicationTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(%{name: "also-boom", filter: %{types: [CountEvent]}}, fn _event, _metadata ->
        {:error, :kaboom_again}
      end)
    end
  end

  defmodule RaisingReactor do
    alias Ariadne.Flow.ApplicationTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(%{name: "raising", filter: %{types: [CountEvent]}}, fn _event, _metadata ->
        raise "kaboom"
      end)
    end
  end

  defmodule SyncCountsReactor do
    alias Ariadne.Flow.ApplicationTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(
        %{name: "sync-counts", filter: %{types: [CountEvent]}, sync: true},
        fn event, metadata ->
          send(Process.get(:inbox), {:got, "sync-counts", event, metadata})
          :ok
        end
      )
    end
  end

  defmodule BoomSyncReactor do
    alias Ariadne.Flow.ApplicationTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(
        %{name: "boom-sync", filter: %{types: [CountEvent]}, sync: true},
        fn _event, _metadata -> {:error, :kaboom} end
      )
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

  # An enqueue-first engine: it executes only the runs that could never be confirmed
  # otherwise and defers the rest onto another process, standing in for a job system.
  # `delay: :never` is the job that never gets around to running.
  defmodule DeferringEngine do
    @behaviour ReactorEngine

    @impl ReactorEngine
    def run(reactor_run, store, opts) do
      cond do
        ReactorRun.inline?(reactor_run) -> ReactorRun.execute(reactor_run, store)
        Keyword.get(opts, :delay, :never) == :never -> :ok
        true -> defer(reactor_run, store, Keyword.fetch!(opts, :delay))
      end
    end

    defp defer(reactor_run, store, delay) do
      spawn(fn ->
        Process.sleep(delay)
        ReactorRun.execute(reactor_run, store)
      end)

      :ok
    end
  end

  defp num_counts_projection do
    Projection.new(
      %{initial_state: 0, filter: %{types: [CountEvent]}},
      fn state, %CountEvent{}, _ -> state + 1 end
    )
  end

  defp total_count_projection do
    Projection.new(
      %{initial_state: 0, filter: %{types: [CountEvent]}},
      fn state, %CountEvent{count: count}, _ -> state + count end
    )
  end

  defp count_command(increase) do
    Composite.new(%{count: num_counts_projection()}, &decide_count(&1, increase))
  end

  defp decide_count(%{count: 3}, _increase), do: {:error, :count_too_high}
  defp decide_count(%{count: count}, increase), do: {:ok, [%CountEvent{count: count + increase}]}

  # The last event this appends is one no reactor here reacts to, so the dispatch's highest
  # position is past every sync reactor's own last matching event.
  defp count_then_other_command do
    Composite.new(
      %{count: num_counts_projection()},
      fn %{count: count} -> {:ok, [%CountEvent{count: count + 1}, %OtherEvent{}]} end
    )
  end

  defp count_stats_query do
    Composite.new(
      %{num_counts: num_counts_projection(), total_count: total_count_projection()},
      &{:ok, &1}
    )
  end

  defp application(reactors \\ []) do
    Application.new(%{store: inbox_store(), reactors: reactors})
  end

  # A raising reactor takes the InMemory agent down with it, so the store has to
  # be one whose reactors run in the calling process to observe a rollback.
  defp postgres_store do
    :ok = Sandbox.checkout(Repo)
    Store.Postgres.init(repo: Repo, prefix: "postgres_store_test_schema")
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

  # Appending from inside the decide function lands between the command's read and
  # its append, which is the conflict the append condition guards against.
  defp conflicting_count_command(store), do: conflicting_count_command(store, :always)

  # The same conflict, but only for the first `conflicts` attempts — the ones after that
  # read the events those appends left behind and decide from them.
  defp conflicting_count_command(store, conflicts) do
    {:ok, remaining} = Agent.start_link(fn -> conflicts end)

    Composite.new(%{count: num_counts_projection()}, fn state ->
      if conflict?(remaining) do
        {:ok, _} = CommandHandler.handle(CommandHandler.new(%{command: count_command(1)}), store)
      end

      decide_count(state, 1)
    end)
  end

  defp conflict?(remaining) do
    Agent.get_and_update(remaining, fn
      :always -> {true, :always}
      0 -> {false, 0}
      count -> {true, count - 1}
    end)
  end

  describe "new/1" do
    test "bundles a store and reactor modules into an application struct" do
      store = Store.InMemory.init()

      assert %Application{store: ^store, reactors: [CountsReactor]} =
               Application.new(%{store: store, reactors: [CountsReactor]})
    end

    test "defaults reactors to an empty list and the engine to the inline engine" do
      store = Store.InMemory.init()

      assert %Application{store: ^store, reactors: [], engine: {ReactorEngine.Inline, []}} =
               Application.new(%{store: store})
    end

    test "normalizes a bare engine module to a {module, opts} pair" do
      store = Store.InMemory.init()

      assert %Application{engine: {RecordingEngine, []}} =
               Application.new(%{store: store, engine: RecordingEngine})
    end

    test "keeps an explicit {module, opts} engine" do
      store = Store.InMemory.init()

      assert %Application{engine: {RecordingEngine, [foo: :bar]}} =
               Application.new(%{store: store, engine: {RecordingEngine, foo: :bar}})
    end
  end

  describe "dispatch/3" do
    test "appends the command's events through the handler and returns them" do
      application = application()

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               Application.dispatch(application, count_command(1))

      assert {:ok, %{events: [%{event: %CountEvent{count: 2}}]}} =
               Application.dispatch(application, count_command(1))
    end

    test "drives the configured reactors over the events the dispatch produced" do
      application = application([CountsReactor])

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               Application.dispatch(application, count_command(1))

      assert_received {:got, "counts", %CountEvent{count: 1}, _}
    end

    test "returns the command error without driving reactors when the command fails" do
      store = inbox_store()
      base = Application.new(%{store: store})
      {:ok, _} = Application.dispatch(base, count_command(1))
      {:ok, _} = Application.dispatch(base, count_command(1))
      {:ok, _} = Application.dispatch(base, count_command(1))

      application = Application.new(%{store: store, reactors: [CountsReactor]})

      assert {:error, :count_too_high} = Application.dispatch(application, count_command(1))

      refute_received {:got, "counts", _, _}
    end

    test "returns a typed error when the append condition fails" do
      store = inbox_store()
      application = Application.new(%{store: store})

      assert {:error, %AppendConditionError{}} =
               Application.dispatch(application, conflicting_count_command(store))
    end

    test "raises a reactor failure with its name, position and reason" do
      application = application([BoomReactor])

      error =
        assert_raise ReactorError, fn ->
          Application.dispatch(application, count_command(1))
        end

      assert %ReactorError{failures: [%{name: "boom", position: position, reason: :kaboom}]} =
               error

      assert is_integer(position)
    end

    test "raises one error carrying every failure, having driven the reactors after a failing one" do
      application = application([BoomReactor, CountsReactor, AlsoBoomReactor])

      error =
        assert_raise ReactorError, fn ->
          Application.dispatch(application, count_command(1))
        end

      assert %ReactorError{
               failures: [
                 %{name: "boom", reason: :kaboom},
                 %{name: "also-boom", reason: :kaboom_again}
               ]
             } = error

      assert_received {:got, "counts", %CountEvent{count: 1}, _}
    end

    test "keeps the command's events when a reactor returns an error" do
      store = postgres_store()
      application = Application.new(%{store: store, reactors: [BoomReactor]})

      assert_raise ReactorError, fn -> Application.dispatch(application, count_command(1)) end

      assert %{events: [%{event: %Store.Event{}}]} = Store.read(store)
    end

    test "rolls back the command's events when a reactor raises" do
      store = postgres_store()
      application = Application.new(%{store: store, reactors: [RaisingReactor]})

      assert_raise RuntimeError, "kaboom", fn ->
        Application.dispatch(application, count_command(1))
      end

      assert %{events: []} = Store.read(store)
    end

    test "keeps the command's events when every reactor succeeds" do
      store = inbox_store()
      application = Application.new(%{store: store, reactors: [CountsReactor]})

      assert {:ok, _} = Application.dispatch(application, count_command(1))

      assert %{events: [%{event: %Store.Event{data: %{"count" => 1}}}]} = Store.read(store)
    end

    test "puts the dispatch metadata on the run, defaulting to an empty map" do
      application =
        Application.new(%{
          store: Store.InMemory.init(),
          reactors: [CountsReactor],
          engine: {RecordingEngine, result: :ok}
        })

      assert {:ok, _} = Application.dispatch(application, count_command(1))

      assert_received {:engine_run, %ReactorRun{reactor: CountsReactor, metadata: %{}}, _store,
                       _opts}

      assert {:ok, _} =
               Application.dispatch(application, count_command(1),
                 metadata: %{"tenant_id" => "acme", "trace_id" => "abc123"}
               )

      assert_received {:engine_run,
                       %ReactorRun{metadata: %{"tenant_id" => "acme", "trace_id" => "abc123"}},
                       _store, _opts}
    end
  end

  describe "dispatch/3 retries an append conflict" do
    test "appends the events the retry decided on, not the ones the conflicted attempt did" do
      store = inbox_store()
      application = Application.new(%{store: store})

      assert {:ok, %{events: [%{event: %CountEvent{count: 2}}]}} =
               Application.dispatch(application, conflicting_count_command(store, 1))
    end

    test "returns the command's own refusal when the retry decides against the new events" do
      store = inbox_store()
      application = Application.new(%{store: store})
      {:ok, _} = Application.dispatch(application, count_command(1))
      {:ok, _} = Application.dispatch(application, count_command(1))

      assert {:error, :count_too_high} =
               Application.dispatch(application, conflicting_count_command(store, 1))
    end

    # A reactor starting from now starts at the head the *winning* attempt saw, so the
    # events this dispatch lost to belong to whoever appended them, not to the retry.
    test "hands the reactors the retry's events and nothing it conflicted with" do
      store = inbox_store()
      application = Application.new(%{store: store, reactors: [CountsReactor]})

      assert {:ok, %{events: [%{event: %CountEvent{count: 2}}]}} =
               Application.dispatch(application, conflicting_count_command(store, 1))

      assert_received {:got, "counts", %CountEvent{count: 2}, _}
      refute_received {:got, "counts", _, _}
    end

    test "gives up after the bound the caller asked for" do
      store = inbox_store()
      application = Application.new(%{store: store})

      assert {:error, %AppendConditionError{}} =
               Application.dispatch(application, conflicting_count_command(store, 1), attempts: 1)
    end

    test "raises before anything is written when the bound is not a positive integer" do
      store = inbox_store()
      application = Application.new(%{store: store})

      assert_raise ArgumentError, ~r/:attempts must be a positive integer/, fn ->
        Application.dispatch(application, count_command(1), attempts: 0)
      end

      assert %{events: []} = Store.read(store)
    end

    test "makes a single attempt when nested in the caller's transaction" do
      store = inbox_store()
      application = Application.new(%{store: store})

      assert {:error, %AppendConditionError{}} =
               Store.transaction(store, fn ->
                 Application.dispatch(application, conflicting_count_command(store, 1),
                   attempts: 3
                 )
               end)

      assert %{events: [%{event: %Store.Event{data: %{"count" => 1}}}]} = Store.read(store)
    end
  end

  describe "dispatch/3 telemetry" do
    setup do
      handler = "dispatch-telemetry-test-#{inspect(self())}"

      :ok =
        :telemetry.attach_many(
          handler,
          [
            [:ariadne, :flow, :dispatch, :start],
            [:ariadne, :flow, :dispatch, :stop],
            [:ariadne, :flow, :dispatch, :exception]
          ],
          fn event, measurements, metadata, pid ->
            send(pid, {:telemetry, List.last(event), measurements, metadata})
          end,
          self()
        )

      on_exit(fn -> :telemetry.detach(handler) end)
    end

    test "reports the one attempt an uncontended dispatch took" do
      assert {:ok, _} = Application.dispatch(application(), count_command(1))

      assert_received {:telemetry, :start, _, %{}}
      assert_received {:telemetry, :stop, %{attempts: 1}, %{result: :ok}}
    end

    test "reports what a conflict cost, so a retry cannot hide the contention" do
      store = inbox_store()

      assert {:ok, _} =
               Application.dispatch(
                 Application.new(%{store: store}),
                 conflicting_count_command(store, 1)
               )

      assert_received {:telemetry, :stop, %{attempts: 2}, %{result: :ok}}
    end

    test "reports a conflict the attempts could not resolve" do
      store = inbox_store()

      assert {:error, %AppendConditionError{}} =
               Application.dispatch(
                 Application.new(%{store: store}),
                 conflicting_count_command(store)
               )

      assert_received {:telemetry, :stop, %{attempts: 3}, %{result: :conflict}}
    end

    test "reports a refused command as an error, having only attempted it once" do
      store = inbox_store()
      application = Application.new(%{store: store})
      for _ <- 1..3, do: {:ok, _} = Application.dispatch(application, count_command(1))

      assert {:error, :count_too_high} = Application.dispatch(application, count_command(1))

      assert_received {:telemetry, :stop, %{attempts: 1}, %{result: :error}}
    end

    test "reports a reactor failure as an exception, the events being committed" do
      assert_raise ReactorError, fn ->
        Application.dispatch(application([BoomReactor]), count_command(1))
      end

      assert_received {:telemetry, :exception, _, %{kind: :error, reason: %ReactorError{}}}
    end
  end

  describe "dispatch/3 awaits its sync reactors" do
    test "returns once a deferred sync run has advanced the reactor's checkpoint" do
      application =
        Application.new(%{
          store: inbox_store(),
          reactors: [SyncCountsReactor],
          engine: {DeferringEngine, delay: 20}
        })

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               Application.dispatch(application, count_command(1), await_timeout: 2_000)

      assert_received {:got, "sync-counts", %CountEvent{count: 1}, _}
    end

    test "confirms instantly when the engine ran the reactor inline" do
      application = Application.new(%{store: inbox_store(), reactors: [SyncCountsReactor]})

      assert {:ok, _} = Application.dispatch(application, count_command(1), await_timeout: 0)

      assert_received {:got, "sync-counts", %CountEvent{count: 1}, _}
    end

    test "raises a consistency timeout naming the reactor and the position it awaited" do
      application =
        Application.new(%{
          store: inbox_store(),
          reactors: [SyncCountsReactor],
          engine: DeferringEngine
        })

      error =
        assert_raise ConsistencyTimeoutError, fn ->
          Application.dispatch(application, count_command(1), await_timeout: 50)
        end

      assert %ConsistencyTimeoutError{
               unconfirmed: [%{name: "sync-counts", position: 1}],
               timeout: 50
             } = error

      refute_received {:got, "sync-counts", _, _}
    end

    test "keeps the committed events when the await times out" do
      store = inbox_store()

      application =
        Application.new(%{store: store, reactors: [SyncCountsReactor], engine: DeferringEngine})

      assert_raise ConsistencyTimeoutError, fn ->
        Application.dispatch(application, count_command(1), await_timeout: 50)
      end

      assert %{events: [%{event: %Store.Event{}}]} = Store.read(store)
    end

    test "does not await an async reactor the engine deferred" do
      application =
        Application.new(%{
          store: inbox_store(),
          reactors: [CountsReactor],
          engine: DeferringEngine
        })

      assert {:ok, _} = Application.dispatch(application, count_command(1), await_timeout: 0)
    end

    test "confirms a caught-up reactor when the dispatch's last event misses its filter" do
      application = Application.new(%{store: inbox_store(), reactors: [SyncCountsReactor]})

      assert {:ok, %{events: [_count, _other]}} =
               Application.dispatch(application, count_then_other_command(), await_timeout: 50)

      assert_received {:got, "sync-counts", %CountEvent{count: 1}, _}
    end

    test "raises the reactor's failure rather than waiting the timeout out" do
      application = Application.new(%{store: inbox_store(), reactors: [BoomSyncReactor]})

      assert_raise ReactorError, fn ->
        Application.dispatch(application, count_command(1), await_timeout: 10_000)
      end
    end
  end

  describe "dispatch/3 nested in an outer transaction" do
    test "tells the engine the run is nested, so it can see it must not defer it" do
      store = inbox_store()

      application =
        Application.new(%{
          store: store,
          reactors: [SyncCountsReactor],
          engine: {RecordingEngine, result: :ok}
        })

      Store.transaction(store, fn -> Application.dispatch(application, count_command(1)) end)

      assert_received {:engine_run, %ReactorRun{reactor: SyncCountsReactor, nested: true} = run,
                       _store, _opts}

      assert ReactorRun.inline?(run)
    end

    test "executes the sync run inline instead of awaiting a confirmation that cannot come" do
      store = inbox_store()

      application =
        Application.new(%{store: store, reactors: [SyncCountsReactor], engine: DeferringEngine})

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               Store.transaction(store, fn ->
                 Application.dispatch(application, count_command(1), await_timeout: 0)
               end)

      assert_received {:got, "sync-counts", %CountEvent{count: 1}, _}
    end

    test "runs inline under a repo transaction the dispatch knows nothing about" do
      store = postgres_store()
      Process.put(:inbox, self())

      application =
        Application.new(%{store: store, reactors: [SyncCountsReactor], engine: DeferringEngine})

      assert {:ok, _} =
               Repo.transaction(fn ->
                 Application.dispatch(application, count_command(1), await_timeout: 0)
               end)

      assert_received {:got, "sync-counts", %CountEvent{count: 1}, _}
    end

    test "is not treated as nested when the caller opened no transaction" do
      store = postgres_store()

      application =
        Application.new(%{
          store: store,
          reactors: [SyncCountsReactor],
          engine: {RecordingEngine, result: :ok}
        })

      assert_raise ConsistencyTimeoutError, fn ->
        Application.dispatch(application, count_command(1), await_timeout: 50)
      end

      assert_received {:engine_run, %ReactorRun{nested: false}, _store, _opts}
    end
  end

  describe "dispatch!/3" do
    test "returns the result when the dispatch succeeds" do
      application = application([CountsReactor])

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               Application.dispatch!(application, count_command(1))

      assert_received {:got, "counts", %CountEvent{count: 1}, _}
    end

    test "raises when the command is refused, carrying the refusal reason" do
      store = inbox_store()
      base = Application.new(%{store: store})
      {:ok, _} = Application.dispatch(base, count_command(1))
      {:ok, _} = Application.dispatch(base, count_command(1))
      {:ok, _} = Application.dispatch(base, count_command(1))

      application = Application.new(%{store: store, reactors: [CountsReactor]})

      error =
        assert_raise CommandError, fn -> Application.dispatch!(application, count_command(1)) end

      assert error.reason == :count_too_high
    end

    test "raises when the append condition fails" do
      store = inbox_store()
      application = Application.new(%{store: store})

      assert_raise AppendConditionError, fn ->
        Application.dispatch!(application, conflicting_count_command(store))
      end
    end

    test "raises when a reactor fails" do
      application = application([BoomReactor])

      assert_raise ReactorError, ~r/reactor "boom" failed at position \d+: :kaboom/, fn ->
        Application.dispatch!(application, count_command(1))
      end
    end

    test "raises when a sync reactor does not confirm in time" do
      application =
        Application.new(%{
          store: inbox_store(),
          reactors: [SyncCountsReactor],
          engine: DeferringEngine
        })

      assert_raise ConsistencyTimeoutError, ~r/never re-dispatch/, fn ->
        Application.dispatch!(application, count_command(1), await_timeout: 50)
      end
    end
  end

  describe "catch_up/2" do
    test "drives a reactor over events no dispatch of its own handed it" do
      store = inbox_store()
      writer = Application.new(%{store: store})
      {:ok, _} = Application.dispatch(writer, count_command(1))
      {:ok, _} = Application.dispatch(writer, count_command(1))

      assert :ok =
               Application.catch_up(Application.new(%{store: store, reactors: [HistoryReactor]}))

      assert_received {:got, "history", %CountEvent{count: 1}, _}
      assert_received {:got, "history", %CountEvent{count: 2}, _}
    end

    test "is a no-op for a reactor that is already up to date" do
      application = Application.new(%{store: inbox_store(), reactors: [HistoryReactor]})
      {:ok, _} = Application.dispatch(application, count_command(1))
      assert_received {:got, "history", %CountEvent{count: 1}, _}

      assert :ok = Application.catch_up(application)

      refute_received {:got, "history", _, _}
    end

    test "skips a reactor that starts from now and has never run" do
      store = inbox_store()
      writer = Application.new(%{store: store})
      {:ok, _} = Application.dispatch(writer, count_command(1))

      assert :ok =
               Application.catch_up(Application.new(%{store: store, reactors: [CountsReactor]}))

      refute_received {:got, "counts", _, _}
    end

    test "returns a reactor failure as a value, because retrying a catch-up is always safe" do
      store = inbox_store()
      writer = Application.new(%{store: store})
      {:ok, _} = Application.dispatch(writer, count_command(1))

      application = Application.new(%{store: store, reactors: [BoomHistoryReactor]})

      assert {:error, %ReactorError{failures: [%{name: "boom-history", reason: :kaboom}]}} =
               Application.catch_up(application)
    end

    test "puts the given metadata on the run and marks it nested inside a transaction" do
      store = inbox_store()
      writer = Application.new(%{store: store})
      {:ok, _} = Application.dispatch(writer, count_command(1))

      application =
        Application.new(%{
          store: store,
          reactors: [HistoryReactor],
          engine: {RecordingEngine, result: :ok}
        })

      assert :ok = Application.catch_up(application, metadata: %{"trace_id" => "abc123"})

      assert_received {:engine_run,
                       %ReactorRun{metadata: %{"trace_id" => "abc123"}, nested: false}, _store,
                       _opts}

      Store.transaction(store, fn -> Application.catch_up(application) end)

      assert_received {:engine_run, %ReactorRun{metadata: %{}, nested: true}, _store, _opts}
    end
  end

  describe "query/2" do
    test "queries the state from a composite" do
      application = application()

      {:ok, _} = Application.dispatch(application, count_command(1))
      {:ok, _} = Application.dispatch(application, count_command(1))

      assert {:ok, %{num_counts: 2, total_count: 3}} =
               Application.query(application, count_stats_query())
    end
  end
end
