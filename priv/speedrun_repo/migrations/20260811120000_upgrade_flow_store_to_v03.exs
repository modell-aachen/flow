defmodule Ariadne.Flow.Store.Speedrun.Repo.Migrations.UpgradeFlowStoreToV03 do
  use Ecto.Migration
  alias Ariadne.Flow.Store.Postgres.Migration

  def up, do: Migration.up(version: 3)

  def down, do: Migration.down(version: 3)
end
