defmodule Ariadne.Flow.Store.Postgres.MigrationTest do
  use ExUnit.Case, async: true

  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Postgres
  alias Ariadne.Flow.Store.Postgres.Migration
  alias Ariadne.Flow.Store.Record
  alias Ariadne.Flow.Test.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @prefix "migration_test_schema"
  @relations ~w(ariadne_flow_store ariadne_flow_store_tags ariadne_flow_store_reactor_checkpoints)
  @legacy ~w(modac_flow_store modac_flow_store_tags modac_flow_store_reactor_checkpoints)

  @kind_query """
  SELECT pg_class.relkind::text
  FROM pg_class
  JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
  WHERE pg_class.relname = $2 AND pg_namespace.nspname = $1
  """

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

    def v02 do
      create_schema()

      Migration.up(prefix: @prefix, version: 2)
    end

    def rollback_to_v02, do: Migration.down(prefix: @prefix, version: 3)

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

      for relation <- @relations, do: assert(relation_kind(relation) == "r")
      for relation <- @legacy, do: refute(relation_exists?(relation))
      assert recorded_version() == "3"
    end

    test "leaves an already migrated store alone" do
      install()
      append_event()

      install()

      assert recorded_version() == "3"
      assert positions("ariadne_flow_store") == [1]
    end

    test "renames the tables of a version 2 store without moving its rows" do
      install_v02()
      append_event()

      install()

      for relation <- @relations, do: assert(relation_kind(relation) == "r")
      for relation <- @legacy, do: refute(relation_exists?(relation))
      assert recorded_version() == "3"
      assert positions("ariadne_flow_store") == [1]
    end

    test "keeps the position sequence of a renamed store" do
      install_v02()
      append_event()

      install()
      append_event()

      assert positions("ariadne_flow_store") == [1, 2]
    end

    test "walks a version 1 store to the current version without recreating its rows" do
      install_v01()
      insert_event()

      install()

      for relation <- @relations, do: assert(relation_kind(relation) == "r")
      assert recorded_version() == "3"
      assert positions("ariadne_flow_store") == [1]
    end

    test "adopts a store that predates the versioning" do
      install_v01()
      forget_version("modac_flow_store")
      insert_event()

      install()

      assert recorded_version() == "3"
      assert positions("ariadne_flow_store") == [1]
    end

    test "recognises a renamed store whose recorded version was lost" do
      install()
      append_event()
      forget_version()

      install()

      for relation <- @relations, do: assert(relation_kind(relation) == "r")
      for relation <- @legacy, do: refute(relation_exists?(relation))
      assert positions("ariadne_flow_store") == [1]
    end

    test "rejects a version it does not know" do
      assert_raise ArgumentError, ~r/:version to be between 1 and 3/, fn ->
        Migration.up(prefix: @prefix, version: 4)
      end
    end

    test "rejects a prefix that is not a schema name" do
      assert_raise ArgumentError, ~r/:prefix to be a schema name/, fn ->
        Migration.up(prefix: :migration_test_schema)
      end
    end
  end

  describe "down/1" do
    test "removes the tables and the recorded version" do
      install()

      migrate(Install, :down)

      for relation <- @relations ++ @legacy, do: refute(relation_exists?(relation))
    end

    test "restores the views over the renamed-back tables and re-records version 2" do
      install()
      append_event()

      migrate(Install, :rollback_to_v02)

      for relation <- @legacy, do: assert(relation_kind(relation) == "r")
      for relation <- @relations, do: assert(relation_kind(relation) == "v")
      assert recorded_version("modac_flow_store") == "2"
      assert positions("modac_flow_store") == [1]
      assert positions("ariadne_flow_store") == [1]
    end

    test "removes the views and re-records version 1" do
      install()

      migrate(Install, :rollback_to_v01)

      for relation <- @relations, do: refute(relation_exists?(relation))
      for relation <- @legacy, do: assert(relation_kind(relation) == "r")
      assert recorded_version("modac_flow_store") == "1"
    end

    test "removes a store whose recorded version was lost" do
      install()
      forget_version()

      migrate(Install, :down)

      for relation <- @relations ++ @legacy, do: refute(relation_exists?(relation))
    end

    test "removes a version 2 store whose recorded version was lost" do
      install_v02()
      forget_version("modac_flow_store")

      migrate(Install, :down)

      for relation <- @relations ++ @legacy, do: refute(relation_exists?(relation))
    end

    test "removes a store that predates the versioning" do
      install_v01()
      forget_version("modac_flow_store")

      migrate(Install, :down)

      for relation <- @legacy, do: refute(relation_exists?(relation))
    end

    test "is a no-op on a schema without a store" do
      create_schema()

      migrate(Install, :down)

      refute relation_exists?("ariadne_flow_store")
    end

    test "rejects a version below the initial one" do
      assert_raise ArgumentError, ~r/:version to be between 1 and 3/, fn ->
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

      assert migrated_version() == 3
    end

    test "reads the version of a store still on the previous one" do
      install_v02()

      assert migrated_version() == 2
    end

    test "reads a renamed store whose recorded version was lost from its shape" do
      install()
      forget_version()

      assert migrated_version() == 3
    end

    test "is zero for a version 2 store whose recorded version was lost" do
      install_v02()
      forget_version("modac_flow_store")

      assert migrated_version() == 0
    end
  end

  defp install, do: migrate(Install, :up)

  defp install_v01, do: migrate(Install, :v01)

  defp install_v02, do: migrate(Install, :v02)

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

  defp positions(relation) do
    %{rows: rows} =
      Repo.query!(~s(SELECT position FROM "#{@prefix}"."#{relation}" ORDER BY position))

    List.flatten(rows)
  end

  defp forget_version(relation \\ "ariadne_flow_store") do
    Repo.query!(~s(COMMENT ON TABLE "#{@prefix}"."#{relation}" IS NULL))
  end

  defp recorded_version(relation \\ "ariadne_flow_store") do
    %{rows: [[version]]} =
      Repo.query!("SELECT obj_description(to_regclass($1), 'pg_class')", [
        "#{@prefix}.#{relation}"
      ])

    version
  end

  defp relation_exists?(relation), do: relation_kind(relation) != nil

  defp relation_kind(relation) do
    case Repo.query!(@kind_query, [@prefix, relation]) do
      %{rows: [[kind]]} -> kind
      %{rows: []} -> nil
    end
  end
end
