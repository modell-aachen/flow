defmodule Ariadne.Flow.Store.Postgres.Migration.V01 do
  @moduledoc false
  @behaviour Ariadne.Flow.Store.Postgres.Migration.Version

  use Ecto.Migration

  @impl Ariadne.Flow.Store.Postgres.Migration.Version
  def up(%{prefix: prefix}) do
    create_if_not_exists table(:modac_flow_store, primary_key: false, prefix: prefix) do
      add(:position, :bigserial, primary_key: true)
      add(:context, :string, size: 70, null: false)
      add(:type, :string, size: 150, null: false)
      add(:data, :map, null: false)
      add(:tags, {:array, :string}, null: false)
      add(:created_at, :utc_datetime_usec, null: false)
      add(:metadata, :map, null: false)
    end

    create_if_not_exists(index(:modac_flow_store, [:type], prefix: prefix))

    create_if_not_exists table(:modac_flow_store_tags, primary_key: false, prefix: prefix) do
      add(:position, references(:modac_flow_store, column: :position), primary_key: true)
      add(:tag, :string, null: false, primary_key: true)
    end

    create_if_not_exists(index(:modac_flow_store_tags, [:tag], prefix: prefix))

    create_if_not_exists table(:modac_flow_store_reactor_checkpoints,
                           primary_key: false,
                           prefix: prefix
                         ) do
      add(:context, :string, size: 70, null: false, primary_key: true)
      add(:name, :string, size: 200, null: false, primary_key: true)
      add(:position, :bigint, null: false, default: 0)
      add(:updated_at, :utc_datetime_usec, null: false)
    end

    :ok
  end

  @impl Ariadne.Flow.Store.Postgres.Migration.Version
  def down(%{prefix: prefix}) do
    drop_if_exists(table(:modac_flow_store_reactor_checkpoints, prefix: prefix))
    drop_if_exists(table(:modac_flow_store_tags, prefix: prefix))
    drop_if_exists(table(:modac_flow_store, prefix: prefix))

    :ok
  end
end
