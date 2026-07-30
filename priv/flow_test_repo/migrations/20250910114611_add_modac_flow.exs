defmodule Ariadne.Flow.Test.Repo.Migrations.AddModacFlow do
  use Ecto.Migration
  alias Ariadne.Flow.Store.Postgres.Migration

  def change do
    execute(
      "CREATE SCHEMA IF NOT EXISTS postgres_store_test_schema",
      "DROP SCHEMA IF EXISTS postgres_store_test_schema CASCADE"
    )

    Migration.run("postgres_store_test_schema")
  end
end
