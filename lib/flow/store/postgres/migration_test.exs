defmodule Ariadne.Flow.Store.Postgres.MigrationTest do
  use ExUnit.Case, async: true

  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Postgres
  alias Ariadne.Flow.Store.Postgres.Migration
  alias Ariadne.Flow.Store.Record
  alias Ariadne.Flow.Test.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @prefix "migration_test_schema"
  @tables ~w(modac_flow_store modac_flow_store_tags modac_flow_store_reactor_checkpoints)
  @views ~w(ariadne_flow_store ariadne_flow_store_tags ariadne_flow_store_reactor_checkpoints)

  defmodule Install do
    use Ecto.Migration

    @prefix "migration_test_schema"

    def up do
      create_schema()

      Migration.up(prefix: @prefix)
    end

    def down, do: Migration.down(prefix: @prefix)

    def v01 do
      create_schema()

      Migration.up(prefix: @prefix, version: 1)
    end

    def rollback_to_v01, do: Migration.down(prefix: @prefix, version: 2)

    defp create_schema, do: execute("CREATE SCHEMA IF NOT EXISTS #{@prefix}")
  end

  defmodule ReportVersion do
    use Ecto.Migration

    @prefix "migration_test_schema"

    def up, do: send(self(), {:migrated_version, Migration.migrated_version(prefix: @prefix)})
  end

  setup do
    :ok = Sandbox.checkout(Repo)
  end

  describe "up/1" do
    test "installs the current schema version" do
      install()

      for relation <- @tables ++ @views, do: assert(relation_exists?(relation))
      assert recorded_version() == "2"
    end

    test "leaves an already migrated store alone" do
      install()
      append_event()

      install()

      assert recorded_version() == "2"
      assert stored_positions() == [1]
    end

    test "adds the views over a version 1 store without recreating its rows" do
      install_v01()
      insert_event()

      install()

      for view <- @views, do: assert(relation_exists?(view))
      assert recorded_version() == "2"
      assert stored_positions() == [1]
      assert view_positions() == [1]
    end

    test "adopts a store that predates the versioning" do
      install_v01()
      forget_version()
      insert_event()

      install()

      assert recorded_version() == "2"
      assert stored_positions() == [1]
    end

    test "rejects a version it does not know" do
      assert_raise ArgumentError, ~r/:version to be between 1 and 2/, fn ->
        Migration.up(prefix: @prefix, version: 3)
      end
    end

    test "rejects a prefix that is not a schema name" do
      assert_raise ArgumentError, ~r/:prefix to be a schema name/, fn ->
        Migration.up(prefix: :migration_test_schema)
      end
    end
  end

  describe "down/1" do
    test "removes the tables, the views and the recorded version" do
      install()

      migrate(Install, :down)

      for relation <- @tables ++ @views, do: refute(relation_exists?(relation))
      refute recorded_version()
    end

    test "removes the views and re-records version 1" do
      install()

      migrate(Install, :rollback_to_v01)

      for view <- @views, do: refute(relation_exists?(view))
      for table <- @tables, do: assert(relation_exists?(table))
      assert recorded_version() == "1"
    end

    test "removes a store that predates the versioning" do
      install_v01()
      forget_version()

      migrate(Install, :down)

      for table <- @tables, do: refute(relation_exists?(table))
    end

    test "is a no-op on a schema without a store" do
      create_schema()

      migrate(Install, :down)

      refute relation_exists?("modac_flow_store")
    end

    test "rejects a version below the initial one" do
      assert_raise ArgumentError, ~r/:version to be between 1 and 2/, fn ->
        Migration.down(prefix: @prefix, version: 0)
      end
    end
  end

  describe "migrated_version/1" do
    test "is zero for a schema without a store" do
      create_schema()

      assert migrated_version() == 0
    end

    test "is the version the store was migrated to" do
      install()

      assert migrated_version() == 2
    end
  end

  defp install, do: migrate(Install, :up)

  defp install_v01, do: migrate(Install, :v01)

  defp migrated_version do
    migrate(ReportVersion, :up)
    assert_received {:migrated_version, version}

    version
  end

  defp migrate(module, operation) do
    Ecto.Migration.Runner.run(
      Repo,
      Repo.config(),
      1,
      module,
      :forward,
      operation,
      operation,
      log: false
    )
  end

  defp create_schema, do: Repo.query!("CREATE SCHEMA IF NOT EXISTS #{@prefix}")

  defp append_event do
    store = Postgres.init(repo: Repo, prefix: @prefix)

    Store.append(store, [%Record{type: "Kept", data: %{}, tags: []}])
  end

  defp insert_event do
    Repo.query!("""
    INSERT INTO "#{@prefix}".modac_flow_store (context, type, data, tags, created_at, metadata)
    VALUES ('default', 'Kept', '{}', '{}', now(), '{}')
    """)
  end

  defp stored_positions, do: positions("modac_flow_store")

  defp view_positions, do: positions("ariadne_flow_store")

  defp positions(relation) do
    %{rows: rows} = Repo.query!(~s(SELECT position FROM "#{@prefix}"."#{relation}"))

    List.flatten(rows)
  end

  defp forget_version do
    Repo.query!(~s(COMMENT ON TABLE "#{@prefix}".modac_flow_store IS NULL))
  end

  defp recorded_version do
    %{rows: [[version]]} =
      Repo.query!("SELECT obj_description(to_regclass($1), 'pg_class')", [
        "#{@prefix}.modac_flow_store"
      ])

    version
  end

  defp relation_exists?(relation) do
    %{rows: [[exists]]} =
      Repo.query!("SELECT to_regclass($1) IS NOT NULL", ["#{@prefix}.#{relation}"])

    exists
  end
end
