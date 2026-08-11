defmodule Ariadne.Flow.Store.Postgres.Migration.V03 do
  @moduledoc false
  @behaviour Ariadne.Flow.Store.Postgres.Migration.Version

  use Ecto.Migration

  alias Ariadne.Flow.Store.Postgres.Migration
  alias Ariadne.Flow.Store.Postgres.Migration.V02

  @tables [
    {"modac_flow_store", "ariadne_flow_store"},
    {"modac_flow_store_tags", "ariadne_flow_store_tags"},
    {"modac_flow_store_reactor_checkpoints", "ariadne_flow_store_reactor_checkpoints"}
  ]

  @sequences [{"modac_flow_store_position_seq", "ariadne_flow_store_position_seq"}]

  @indexes [
    {"modac_flow_store_type_index", "ariadne_flow_store_type_index"},
    {"modac_flow_store_tags_tag_index", "ariadne_flow_store_tags_tag_index"}
  ]

  @constraints [
    {"ariadne_flow_store", "modac_flow_store_pkey", "ariadne_flow_store_pkey"},
    {"ariadne_flow_store_tags", "modac_flow_store_tags_pkey", "ariadne_flow_store_tags_pkey"},
    {"ariadne_flow_store_tags", "modac_flow_store_tags_position_fkey",
     "ariadne_flow_store_tags_position_fkey"},
    {"ariadne_flow_store_reactor_checkpoints", "modac_flow_store_reactor_checkpoints_pkey",
     "ariadne_flow_store_reactor_checkpoints_pkey"}
  ]

  @impl Ariadne.Flow.Store.Postgres.Migration.Version
  def up(%{prefix: prefix} = opts) do
    drop_views(opts)

    rename_relations(prefix, "TABLE", @tables)
    rename_relations(prefix, "SEQUENCE", @sequences)
    rename_relations(prefix, "INDEX", @indexes)
    rename_constraints(prefix, @constraints)
  end

  @impl Ariadne.Flow.Store.Postgres.Migration.Version
  def down(%{prefix: prefix} = opts) do
    if Migration.renamed?(prefix) do
      rename_constraints(prefix, reversed_constraints())
      rename_relations(prefix, "INDEX", reversed(@indexes))
      rename_relations(prefix, "SEQUENCE", reversed(@sequences))
      rename_relations(prefix, "TABLE", reversed(@tables))

      create_views(opts)
    end

    :ok
  end

  defp drop_views(opts), do: V02.down(opts)

  defp create_views(opts), do: V02.up(opts)

  defp reversed(pairs), do: Enum.map(pairs, fn {from, to} -> {to, from} end)

  defp reversed_constraints do
    Enum.map(@constraints, fn {table, from, to} -> {table, to, from} end)
  end

  defp rename_relations(prefix, kind, pairs) do
    Enum.each(pairs, fn {from, to} ->
      execute(
        "ALTER #{kind} IF EXISTS #{Migration.qualified(prefix, from)} " <>
          "RENAME TO #{Migration.quoted(to)}"
      )
    end)
  end

  defp rename_constraints(prefix, constraints) do
    Enum.each(constraints, fn {table, from, to} ->
      execute("""
      DO $$ BEGIN
        ALTER TABLE IF EXISTS #{Migration.qualified(prefix, table)}
          RENAME CONSTRAINT #{Migration.quoted(from)} TO #{Migration.quoted(to)};
      EXCEPTION WHEN undefined_object THEN NULL;
      END $$
      """)
    end)
  end
end
