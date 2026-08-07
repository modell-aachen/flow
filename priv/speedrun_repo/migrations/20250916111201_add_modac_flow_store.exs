defmodule Ariadne.Flow.Store.Speedrun.Repo.Migrations.AddModacFlowStore do
  use Ecto.Migration
  alias Ariadne.Flow.Store.Postgres.Migration

  def up, do: Migration.up()

  def down, do: Migration.down()
end
