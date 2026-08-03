defmodule Ariadne.Flow.ApplicationTest do
  use ExUnit.Case, async: true
  alias Ariadne.Flow.AfterCommit
  alias Ariadne.Flow.AppendConditionError
  alias Ariadne.Flow.Application
  alias Ariadne.Flow.CommandError
  alias Ariadne.Flow.CommandHandler
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Projection
  alias Ariadne.Flow.ReactorEngine
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Store
  alias Ecto.Adapters.SQL.Sandbox

  defmodule CountEvent do
    @derive Ariadne.Flow.Store.Event.Encoder
    defstruct count: 1

    def tags(%{count: count}), do: ["count:#{count}"]
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

  defmodule OtherCountsReactor do
    alias Ariadne.Flow.ApplicationTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(%{name: "other-counts", filter: %{types: [CountEvent]}}, fn _event, _metadata ->
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

  defmodule RaisingReactor do
    alias Ariadne.Flow.ApplicationTest.CountEvent
    alias Ariadne.Flow.Reactor

    def reactor do
      Reactor.new(%{name: "raising", filter: %{types: [CountEvent]}}, fn _event, _metadata ->
        raise "kaboom"
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

  # Shaped like the real async engine: sync reactors run inline, async ones are
  # deferred and report back through an after-commit callback.
  defmodule AfterCommitEngine do
    @behaviour ReactorEngine

    @impl ReactorEngine
    def run(%ReactorRun{reactor: reactor} = reactor_run, store, opts) do
      inbox = Keyword.fetch!(opts, :inbox)
      send(inbox, {:step, {:engine_ran, reactor}})

      if ReactorRun.sync?(reactor_run) do
        ReactorRun.execute(reactor_run, store)
      else
        {:ok,
         AfterCommit.new(fn _after_commit ->
           send(inbox, {:step, {:after_commit, reactor, self()}})
         end)}
      end
    end
  end

  defmodule RaisingAfterCommitEngine do
    @behaviour ReactorEngine

    @impl ReactorEngine
    def run(_reactor_run, _store, _opts),
      do: {:ok, AfterCommit.new(fn _after_commit -> raise "kaboom" end)}
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
    Composite.new(
      %{count: num_counts_projection()},
      fn
        %{count: 3} -> {:error, :count_too_high}
        %{count: count} -> {:ok, [%CountEvent{count: count + increase}]}
      end
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
    :ok = Sandbox.checkout(Ariadne.Flow.Test.Repo)
    Store.Postgres.init(repo: Ariadne.Flow.Test.Repo, prefix: "postgres_store_test_schema")
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

  defp after_commit_application(store, reactors) do
    Application.new(%{
      store: store,
      reactors: reactors,
      engine: {AfterCommitEngine, inbox: self()}
    })
  end

  # Appending from inside the decide function lands between the command's read and
  # its append, which is the conflict the append condition guards against.
  defp conflicting_count_command(store) do
    Composite.new(%{count: num_counts_projection()}, fn %{count: count} ->
      {:ok, _} = CommandHandler.handle(count_command(1), store)
      {:ok, [%CountEvent{count: count + 1}]}
    end)
  end

  defp recorded_steps(acc \\ []) do
    receive do
      {:step, step} -> recorded_steps([step | acc])
    after
      0 -> Enum.reverse(acc)
    end
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

    test "raises a reactor failure with its name, position and reason" do
      application = application([BoomReactor])

      error =
        assert_raise ReactorError, fn ->
          Application.dispatch(application, count_command(1))
        end

      assert %ReactorError{name: "boom", reason: :kaboom} = error
      assert is_integer(error.position)
    end

    test "keeps the command's events when a reactor returns an error" do
      store = postgres_store()
      application = Application.new(%{store: store, reactors: [BoomReactor]})

      assert_raise ReactorError, fn -> Application.dispatch(application, count_command(1)) end

      assert %{events: [%{event: %Store.Event{}}]} = Store.read(store)
    end

    test "returns a typed error when the append condition fails" do
      store = inbox_store()
      application = Application.new(%{store: store})

      assert {:error, %AppendConditionError{}} =
               Application.dispatch(application, conflicting_count_command(store))
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

  describe "dispatch/3 after-commit callbacks" do
    test "runs them once each in reactor-declaration order, after the whole reactor pass" do
      application = after_commit_application(inbox_store(), [CountsReactor, OtherCountsReactor])

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               Application.dispatch(application, count_command(1))

      assert [
               {:engine_ran, CountsReactor},
               {:engine_ran, OtherCountsReactor},
               {:after_commit, CountsReactor, first_pid},
               {:after_commit, OtherCountsReactor, second_pid}
             ] = recorded_steps()

      # Both in the dispatching process, so a callback may close over process-local state.
      assert first_pid == self()
      assert second_pid == self()
    end

    test "runs them outside the transaction, so a raising one leaves the events in place" do
      store = postgres_store()

      application =
        Application.new(%{
          store: store,
          reactors: [CountsReactor],
          engine: RaisingAfterCommitEngine
        })

      assert_raise RuntimeError, "kaboom", fn ->
        Application.dispatch(application, count_command(1))
      end

      assert %{events: [%{event: %Store.Event{}}]} = Store.read(store)
    end

    test "runs none when the command is refused" do
      store = inbox_store()
      base = Application.new(%{store: store})
      {:ok, _} = Application.dispatch(base, count_command(1))
      {:ok, _} = Application.dispatch(base, count_command(1))
      {:ok, _} = Application.dispatch(base, count_command(1))

      application = after_commit_application(store, [CountsReactor])

      assert {:error, :count_too_high} = Application.dispatch(application, count_command(1))

      assert recorded_steps() == []
    end

    test "runs none when the append condition fails" do
      store = inbox_store()
      application = after_commit_application(store, [CountsReactor])

      assert {:error, %AppendConditionError{}} =
               Application.dispatch(application, conflicting_count_command(store))

      assert recorded_steps() == []
    end

    test "discards the ones already collected when a later reactor fails" do
      application = after_commit_application(inbox_store(), [CountsReactor, BoomSyncReactor])

      assert_raise ReactorError, fn -> Application.dispatch(application, count_command(1)) end

      assert recorded_steps() == [
               {:engine_ran, CountsReactor},
               {:engine_ran, BoomSyncReactor}
             ]
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

    test "returns the plain result when a reactor deferred work to an after-commit callback" do
      application = after_commit_application(inbox_store(), [CountsReactor])

      assert {:ok, %{events: [%{event: %CountEvent{count: 1}}]}} =
               Application.dispatch!(application, count_command(1))

      assert [{:engine_ran, CountsReactor}, {:after_commit, CountsReactor, _}] = recorded_steps()
    end

    test "raises when a reactor fails" do
      application = application([BoomReactor])

      assert_raise ReactorError, ~r/reactor "boom" failed at position \d+: :kaboom/, fn ->
        Application.dispatch!(application, count_command(1))
      end
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
