defmodule Ariadne.Flow.ApplicationTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.AppendConditionError
  alias Ariadne.Flow.Application
  alias Ariadne.Flow.CommandError
  alias Ariadne.Flow.CommandHandler
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.PostCommitError
  alias Ariadne.Flow.Projection
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Scheduler
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Test.Repo
  alias Ecto.Adapters.SQL.Sandbox

  # An isolated reactor failure is logged rather than raised, so the suite would otherwise
  # print every deliberate failure it provokes.
  @moduletag capture_log: true

  defmodule CountEvent do
    @derive Ariadne.Flow.Event
    defstruct count: 1

    def tags(%{count: count}), do: ["count:#{count}"]
  end

  defmodule OtherEvent do
    @derive Ariadne.Flow.Event
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

  # The same reactor once the cause of its failure is fixed: same name, so same checkpoint.
  defmodule FixedBoomReactor do
    alias Ariadne.Flow.ApplicationTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(%{name: "boom", filter: %{types: [CountEvent]}}, fn event, metadata ->
        send(Process.get(:inbox), {:got, "boom", event, metadata})
        :ok
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

  defmodule RaisingHistoryReactor do
    alias Ariadne.Flow.ApplicationTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(
        %{name: "raising-history", filter: %{types: [CountEvent]}, start_after_position: 0},
        fn _event, _metadata -> raise "kaboom" end
      )
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

  defmodule RaisingSyncReactor do
    alias Ariadne.Flow.ApplicationTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(
        %{name: "raising-sync", filter: %{types: [CountEvent]}, sync: true},
        fn _event, _metadata -> raise "kaboom" end
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

  # A scheduler that reports every run as durably enqueued and hands it to another process
  # after a delay, standing in for a job system. `delay: :never` is the job row that is
  # written but never picked up — what Flow's own execution is not allowed to substitute for.
  defmodule ClaimingScheduler do
    @behaviour Scheduler

    @impl Scheduler
    def schedule(reactor_runs, store, opts) do
      send(self(), {:scheduled, reactor_runs, store, opts})

      Enum.each(reactor_runs, &defer(&1, store, Keyword.get(opts, :delay, :never)))

      reactor_runs
    end

    defp defer(_reactor_run, _store, :never), do: :ok

    defp defer(reactor_run, store, delay) do
      spawn(fn ->
        Process.sleep(delay)
        ReactorRun.execute(reactor_run, store)
      end)
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

  # A raising reactor takes the InMemory agent down with it, so a raise can only be
  # observed on a store whose reactors run in the calling process.
  defp postgres_store do
    :ok = Sandbox.checkout(Repo)
    Process.put(:inbox, self())

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

    test "defaults reactors to an empty list and to no scheduler at all" do
      store = Store.InMemory.init()

      assert %Application{store: ^store, reactors: [], scheduler: nil} =
               Application.new(%{store: store})
    end

    test "normalizes a bare scheduler module to a {module, opts} pair" do
      store = Store.InMemory.init()

      assert %Application{scheduler: {ClaimingScheduler, []}} =
               Application.new(%{store: store, scheduler: ClaimingScheduler})
    end

    test "keeps an explicit {module, opts} scheduler" do
      store = Store.InMemory.init()

      assert %Application{scheduler: {ClaimingScheduler, [foo: :bar]}} =
               Application.new(%{store: store, scheduler: {ClaimingScheduler, foo: :bar}})
    end

    # Two reactors under one name share a checkpoint, so each would consume events the
    # other never sees — a config error worth catching where the application is described.
    test "refuses reactors that share a name, a name being what a checkpoint is keyed on" do
      store = Store.InMemory.init()

      assert_raise ArgumentError, ~r/distinctly named.*"boom"/, fn ->
        Application.new(%{store: store, reactors: [BoomReactor, FixedBoomReactor]})
      end

      assert_raise ArgumentError, ~r/distinctly named/, fn ->
        Application.new(%{store: store, reactors: [CountsReactor, CountsReactor]})
      end
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

    test "keeps the command's events when every reactor succeeds" do
      store = inbox_store()
      application = Application.new(%{store: store, reactors: [CountsReactor]})

      assert {:ok, _} = Application.dispatch(application, count_command(1))

      assert %{events: [%{record: %Store.Record{data: %{"count" => 1}}}]} = Store.read(store)
    end

    test "hands the scheduler every run at once, with the dispatch metadata on each" do
      application =
        Application.new(%{
          store: Store.InMemory.init(),
          reactors: [CountsReactor, HistoryReactor],
          scheduler: {ClaimingScheduler, foo: :bar}
        })

      assert {:ok, _} = Application.dispatch(application, count_command(1))

      assert_received {:scheduled, runs, _store, [foo: :bar]}

      assert [
               %ReactorRun{reactor: CountsReactor, metadata: %{}},
               %ReactorRun{reactor: HistoryReactor, metadata: %{}}
             ] = runs

      assert {:ok, _} =
               Application.dispatch(application, count_command(1),
                 metadata: %{"tenant_id" => "acme", "trace_id" => "abc123"}
               )

      assert_received {:scheduled, [%ReactorRun{metadata: %{"tenant_id" => "acme"}} | _], _store,
                       _opts}
    end
  end

  describe "dispatch/3 isolates an async reactor's failure" do
    test "returns the dispatch's events when an async reactor errors" do
      store = inbox_store()
      application = Application.new(%{store: store, reactors: [BoomReactor]})

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               Application.dispatch(application, count_command(1))

      assert %{events: [%{record: %Store.Record{}}]} = Store.read(store)
    end

    test "keeps the command's events when an async reactor raises" do
      store = postgres_store()
      application = Application.new(%{store: store, reactors: [RaisingReactor]})

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               Application.dispatch(application, count_command(1))

      assert %{events: [%{record: %Store.Record{}}]} = Store.read(store)
    end

    test "logs the failure and says the reactor will get another run" do
      application = application([BoomReactor])

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, _} = Application.dispatch(application, count_command(1))
        end)

      assert log =~ ~s|reactor "boom" failed at position 0: :kaboom|
      assert log =~ "checkpoint stayed put"
    end

    test "runs every reactor declared after a failing one" do
      application = application([BoomReactor, CountsReactor, AlsoBoomReactor])

      assert {:ok, _} = Application.dispatch(application, count_command(1))

      assert_received {:got, "counts", %CountEvent{count: 1}, _}
    end

    # The checkpoint is parked in front of the event the reactor choked on, so the events
    # are reacted to at least once — a later run picks them up once the cause is fixed.
    test "leaves the failed reactor's checkpoint where it can retry the events" do
      store = inbox_store()

      assert {:ok, _} =
               Application.dispatch(
                 Application.new(%{store: store, reactors: [BoomReactor]}),
                 count_command(1)
               )

      assert Store.checkpoint(store, "boom") == 0

      assert :ok =
               Application.catch_up(
                 Application.new(%{store: store, reactors: [FixedBoomReactor]})
               )

      assert_received {:got, "boom", %CountEvent{count: 1}, _}
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

      assert %{events: [%{record: %Store.Record{data: %{"count" => 1}}}]} = Store.read(store)
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

    test "reports a sync reactor's failure as an exception, the events being committed" do
      assert_raise PostCommitError, fn ->
        Application.dispatch(application([BoomSyncReactor]), count_command(1))
      end

      assert_received {:telemetry, :exception, _, %{kind: :error, reason: %PostCommitError{}}}
    end

    test "reports an async reactor's failure as an ordinary dispatch, not an exception" do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, _} = Application.dispatch(application([BoomReactor]), count_command(1))
        end)

      assert log =~ "boom"
      assert_received {:telemetry, :stop, %{attempts: 1}, %{result: :ok}}
      refute_received {:telemetry, :exception, _, _}
    end
  end

  describe "dispatch/3 awaits its sync reactors" do
    test "returns once a scheduled sync run has advanced the reactor's checkpoint" do
      application =
        Application.new(%{
          store: inbox_store(),
          reactors: [SyncCountsReactor],
          scheduler: {ClaimingScheduler, delay: 20}
        })

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               Application.dispatch(application, count_command(1), await_timeout: 2_000)

      assert_received {:got, "sync-counts", %CountEvent{count: 1}, _}
    end

    # With no scheduler there is nothing to poll for: Flow ran the reactor itself before the
    # wait began, so the first look at the checkpoint confirms.
    test "confirms without polling delay when Flow ran the reactor itself" do
      application = Application.new(%{store: inbox_store(), reactors: [SyncCountsReactor]})

      assert {:ok, _} = Application.dispatch(application, count_command(1), await_timeout: 0)

      assert_received {:got, "sync-counts", %CountEvent{count: 1}, _}
    end

    test "raises a post-commit timeout naming the reactor and the position it awaited" do
      application =
        Application.new(%{
          store: inbox_store(),
          reactors: [SyncCountsReactor],
          scheduler: ClaimingScheduler
        })

      error =
        assert_raise PostCommitError, fn ->
          Application.dispatch(application, count_command(1), await_timeout: 50)
        end

      assert %PostCommitError{
               reason: :timeout,
               unconfirmed: [%{name: "sync-counts", position: 1}],
               timeout: 50
             } = error

      refute_received {:got, "sync-counts", _, _}
    end

    test "keeps the committed events when the await times out" do
      store = inbox_store()

      application =
        Application.new(%{
          store: store,
          reactors: [SyncCountsReactor],
          scheduler: ClaimingScheduler
        })

      assert_raise PostCommitError, fn ->
        Application.dispatch(application, count_command(1), await_timeout: 50)
      end

      assert %{events: [%{record: %Store.Record{}}]} = Store.read(store)
    end

    test "does not await an async reactor the scheduler scheduled" do
      application =
        Application.new(%{
          store: inbox_store(),
          reactors: [CountsReactor],
          scheduler: ClaimingScheduler
        })

      assert {:ok, _} = Application.dispatch(application, count_command(1), await_timeout: 0)
    end

    test "confirms a caught-up reactor when the dispatch's last event misses its filter" do
      application = Application.new(%{store: inbox_store(), reactors: [SyncCountsReactor]})

      assert {:ok, %{events: [_count, _other]}} =
               Application.dispatch(application, count_then_other_command(), await_timeout: 50)

      assert_received {:got, "sync-counts", %CountEvent{count: 1}, _}
    end

    test "raises the reactor's own failure rather than waiting the timeout out" do
      application = Application.new(%{store: inbox_store(), reactors: [BoomSyncReactor]})

      error =
        assert_raise PostCommitError, fn ->
          Application.dispatch(application, count_command(1), await_timeout: 10_000)
        end

      assert %PostCommitError{
               reason: :failure,
               failures: [%{name: "boom-sync", reason: :kaboom}]
             } = error
    end

    # The raise carries the message but not the origin, so the reactor that aborted the
    # request would otherwise be the one failure with no trace to find it by.
    test "logs a raising sync reactor's stacktrace, which the raise itself cannot carry" do
      store = postgres_store()
      application = Application.new(%{store: store, reactors: [RaisingSyncReactor]})

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert_raise PostCommitError, fn ->
            Application.dispatch(application, count_command(1), await_timeout: 0)
          end
        end)

      assert log =~ ~s|reactor "raising-sync" failed|
      assert log =~ "application_test.exs"
    end

    # A sync reactor failing inside somebody else's job system is invisible here, so the
    # wait is all the dispatch has — and it runs out.
    test "times out on a sync reactor whose failure it cannot see" do
      application =
        Application.new(%{
          store: inbox_store(),
          reactors: [BoomSyncReactor],
          scheduler: ClaimingScheduler
        })

      assert_raise PostCommitError, ~r/did not catch up/, fn ->
        Application.dispatch(application, count_command(1), await_timeout: 50)
      end
    end

    test "isolates an async failure from the sync reactors it is declared beside" do
      application =
        Application.new(%{store: inbox_store(), reactors: [BoomReactor, SyncCountsReactor]})

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, _} = Application.dispatch(application, count_command(1), await_timeout: 50)
        end)

      assert log =~ ~s|reactor "boom" failed|
      assert_received {:got, "sync-counts", %CountEvent{count: 1}, _}
    end
  end

  describe "dispatch/3 nested in an outer transaction" do
    test "never offers a sync run to the scheduler, running it itself instead" do
      store = inbox_store()

      application =
        Application.new(%{
          store: store,
          reactors: [SyncCountsReactor, CountsReactor],
          scheduler: ClaimingScheduler
        })

      Store.transaction(store, fn ->
        Application.dispatch(application, count_command(1), await_timeout: 0)
      end)

      assert_received {:scheduled, [%ReactorRun{reactor: CountsReactor}], _store, _opts}
      assert_received {:got, "sync-counts", %CountEvent{count: 1}, _}
    end

    test "runs the sync reactor instead of awaiting a confirmation that cannot come" do
      store = inbox_store()

      application =
        Application.new(%{
          store: store,
          reactors: [SyncCountsReactor],
          scheduler: ClaimingScheduler
        })

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               Store.transaction(store, fn ->
                 Application.dispatch(application, count_command(1), await_timeout: 0)
               end)

      assert_received {:got, "sync-counts", %CountEvent{count: 1}, _}
    end

    test "runs the sync reactor under a repo transaction the dispatch knows nothing about" do
      store = postgres_store()

      application =
        Application.new(%{
          store: store,
          reactors: [SyncCountsReactor],
          scheduler: ClaimingScheduler
        })

      assert {:ok, _} =
               Repo.transaction(fn ->
                 Application.dispatch(application, count_command(1), await_timeout: 0)
               end)

      assert_received {:got, "sync-counts", %CountEvent{count: 1}, _}
    end

    # Its work lands after the outer commit either way — through the job row the scheduler
    # wrote inside that transaction, or through the next dispatch or catch-up.
    test "leaves an async reactor to whatever runs after the outer commit" do
      store = inbox_store()
      application = Application.new(%{store: store, reactors: [CountsReactor]})

      assert {:ok, _} =
               Store.transaction(store, fn ->
                 Application.dispatch(application, count_command(1))
               end)

      refute_received {:got, "counts", _, _}
      assert Store.checkpoint(store, "counts") == 0

      assert :ok = Application.catch_up(application)

      assert_received {:got, "counts", %CountEvent{count: 1}, _}
    end

    # The dispatch has no transaction of its own to protect here, and the caller declared
    # it wanted this reactor's work — so the failure goes to whoever owns the transaction.
    test "raises a sync reactor's failure into the caller's transaction" do
      store = inbox_store()
      application = Application.new(%{store: store, reactors: [BoomSyncReactor]})

      assert_raise PostCommitError, ~r/sync reactor "boom-sync" failed/, fn ->
        Store.transaction(store, fn ->
          Application.dispatch(application, count_command(1), await_timeout: 0)
        end)
      end
    end

    # The raise goes back through the caller's transaction, so the events it carries are
    # undone with it — telling the caller never to re-dispatch would lose the write.
    test "does not claim the events are committed when they are the caller's to roll back" do
      store = postgres_store()
      application = Application.new(%{store: store, reactors: [BoomSyncReactor]})

      error =
        assert_raise PostCommitError, fn ->
          Store.transaction(store, fn ->
            Application.dispatch(application, count_command(1), await_timeout: 0)
          end)
        end

      assert %PostCommitError{reason: :failure, nested: true} = error
      assert Exception.message(error) =~ "the transaction you opened"
      refute Exception.message(error) =~ "never re-dispatch"

      assert %{events: []} = Store.read(store)
    end

    test "is not treated as nested when the caller opened no transaction" do
      store = postgres_store()

      application =
        Application.new(%{
          store: store,
          reactors: [SyncCountsReactor],
          scheduler: ClaimingScheduler
        })

      assert_raise PostCommitError, fn ->
        Application.dispatch(application, count_command(1), await_timeout: 50)
      end

      assert_received {:scheduled, [%ReactorRun{reactor: SyncCountsReactor}], _store, _opts}
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

    test "raises when a sync reactor fails" do
      application = application([BoomSyncReactor])

      assert_raise PostCommitError,
                   ~r/sync reactor "boom-sync" failed at position \d+: :kaboom/,
                   fn -> Application.dispatch!(application, count_command(1)) end
    end

    test "raises when a sync reactor does not confirm in time" do
      application =
        Application.new(%{
          store: inbox_store(),
          reactors: [SyncCountsReactor],
          scheduler: ClaimingScheduler
        })

      assert_raise PostCommitError, ~r/never re-dispatch/, fn ->
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

    test "offers its runs to the scheduler, carrying the metadata it was given" do
      store = inbox_store()
      writer = Application.new(%{store: store})
      {:ok, _} = Application.dispatch(writer, count_command(1))

      application =
        Application.new(%{store: store, reactors: [HistoryReactor], scheduler: ClaimingScheduler})

      assert :ok = Application.catch_up(application, metadata: %{"trace_id" => "abc123"})

      assert_received {:scheduled, [%ReactorRun{metadata: %{"trace_id" => "abc123"}}], _store, _}
    end

    # A catch-up has no events of its own to hide, so nesting changes nothing about it:
    # it runs its reactors, and its writes belong to the caller's transaction like any other.
    test "runs its reactors inside a transaction the caller opened" do
      store = inbox_store()
      writer = Application.new(%{store: store})
      {:ok, _} = Application.dispatch(writer, count_command(1))

      application = Application.new(%{store: store, reactors: [HistoryReactor]})

      assert :ok = Store.transaction(store, fn -> Application.catch_up(application) end)

      assert_received {:got, "history", %CountEvent{count: 1}, _}
      assert Store.checkpoint(store, "history") == 1
    end

    test "returns a raising reactor's failure as a value too, having run the rest" do
      store = postgres_store()

      {:ok, _} =
        Application.dispatch(Application.new(%{store: store}), count_command(1))

      application =
        Application.new(%{store: store, reactors: [RaisingHistoryReactor, HistoryReactor]})

      assert {:error, %ReactorError{failures: [%{name: "raising-history", reason: reason}]}} =
               Application.catch_up(application)

      assert %RuntimeError{message: "kaboom"} = reason
      assert_received {:got, "history", %CountEvent{count: 1}, _}
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
