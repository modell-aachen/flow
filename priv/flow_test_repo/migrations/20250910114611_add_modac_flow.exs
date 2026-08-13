defmodule Ariadne.Flow.Test.Repo.Migrations.AddModacFlow do
  use Ecto.Migration
  alias Ariadne.Flow.Store.Postgres.Migration

  @prefix "postgres_store_test_schema"

  def up do
    execute("CREATE SCHEMA IF NOT EXISTS #{@prefix}")

    Migration.up(prefix: @prefix)
  end

  def down do
    Migration.down(prefix: @prefix)

    execute("DROP SCHEMA IF EXISTS #{@prefix} CASCADE")
  end
end
