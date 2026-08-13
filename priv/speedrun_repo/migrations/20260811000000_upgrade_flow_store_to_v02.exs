defmodule Ariadne.Flow.Store.Speedrun.Repo.Migrations.UpgradeFlowStoreToV02 do
  use Ecto.Migration
  alias Ariadne.Flow.Store.Postgres.Migration

  def up, do: Migration.up(version: 2)

  def down, do: Migration.down(version: 2)
end
