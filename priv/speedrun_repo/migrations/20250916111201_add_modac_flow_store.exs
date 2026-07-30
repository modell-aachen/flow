defmodule Ariadne.Flow.Store.Speedrun.Repo.Migrations.AddModacFlowStore do
  use Ecto.Migration
  alias Ariadne.Flow.Store.Postgres.Migration

  def change do
    Migration.run(prefix())
  end
end
