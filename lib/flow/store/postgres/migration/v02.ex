defmodule Ariadne.Flow.Store.Postgres.Migration.V02 do
  @moduledoc false
  @behaviour Ariadne.Flow.Store.Postgres.Migration.Version

  use Ecto.Migration

  alias Ariadne.Flow.Store.Postgres.Migration

  @views [
    {"ariadne_flow_store", "modac_flow_store"},
    {"ariadne_flow_store_tags", "modac_flow_store_tags"},
    {"ariadne_flow_store_reactor_checkpoints", "modac_flow_store_reactor_checkpoints"}
  ]

  @impl Ariadne.Flow.Store.Postgres.Migration.Version
  def up(%{prefix: prefix}) do
    Enum.each(@views, fn {view, table} ->
      execute(
        "CREATE OR REPLACE VIEW #{Migration.qualified(prefix, view)} " <>
          "AS SELECT * FROM #{Migration.qualified(prefix, table)}"
      )
    end)
  end

  @impl Ariadne.Flow.Store.Postgres.Migration.Version
  def down(%{prefix: prefix}) do
    Enum.each(@views, fn {view, _table} ->
      execute("DROP VIEW IF EXISTS #{Migration.qualified(prefix, view)}")
    end)
  end
end
