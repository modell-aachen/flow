defmodule Ariadne.Flow.Store.Speedrun.Repo.Migrations.AddModacFlowStore do
  use Ecto.Migration
  alias Ariadne.Flow.Store.Postgres.Migration

  def up, do: Migration.up(version: 1)

  def down, do: Migration.down()
end
