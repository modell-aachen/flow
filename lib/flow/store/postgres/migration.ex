defmodule Ariadne.Flow.Store.Postgres.Migration do
  @moduledoc false
  use Ecto.Migration

  @initial_version 1
  @current_version 3
  @renamed_version 3
  @default_prefix "public"
  @version_tables ["ariadne_flow_store", "modac_flow_store"]

  @version_query """
  SELECT pg_description.description
  FROM pg_class
  JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
  JOIN pg_description ON pg_description.objoid = pg_class.oid AND pg_description.objsubid = 0
  WHERE pg_class.relname = $1 AND pg_namespace.nspname = $2
  """

  @renamed_query """
  SELECT EXISTS (
    SELECT 1
    FROM pg_class
    JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
    WHERE pg_class.relname = 'ariadne_flow_store'
      AND pg_namespace.nspname = $1
      AND pg_class.relkind = 'r'
  )
  """

  @type opts :: %{prefix: String.t(), version: pos_integer()}

  @spec up(keyword()) :: :ok
  def up(opts \\ []) do
    opts = normalize(opts, @current_version)
    installed = installed_version(opts.prefix)

    if installed < opts.version do
      apply_versions((installed + 1)..opts.version, :up, opts)
      record_version(opts, opts.version)
    else
      :ok
    end
  end

  @spec down(keyword()) :: :ok
  def down(opts \\ []) do
    opts = normalize(opts, @initial_version)
    stamped = stamped_version(opts.prefix)
    installed = rollback_from(stamped)

    if installed >= opts.version do
      apply_versions(installed..opts.version//-1, :down, opts)
      record_rollback(opts, stamped)
    else
      :ok
    end
  end

  @spec migrated_version(keyword()) :: non_neg_integer()
  def migrated_version(opts \\ []) do
    opts
    |> normalize(@current_version)
    |> Map.fetch!(:prefix)
    |> installed_version()
  end

  @doc false
  @spec qualified(String.t(), String.t()) :: String.t()
  def qualified(prefix, relation), do: "#{quoted(prefix)}.#{quoted(relation)}"

  @doc false
  @spec quoted(String.t()) :: String.t()
  def quoted(identifier), do: ~s("#{String.replace(identifier, ~s("), ~s(""))}")

  @doc false
  @spec renamed?(String.t()) :: boolean()
  def renamed?(prefix) do
    %{rows: [[renamed]]} = repo().query!(@renamed_query, [prefix], log: false)

    renamed
  end

  defp rollback_from(nil), do: @current_version
  defp rollback_from(stamped), do: stamped

  defp record_rollback(_opts, nil), do: :ok
  defp record_rollback(opts, _stamped), do: record_version(opts, opts.version - 1)

  defp apply_versions(versions, direction, opts) do
    Enum.each(versions, fn version -> apply(version_module(version), direction, [opts]) end)
  end

  defp version_module(version) do
    Module.concat(__MODULE__, "V" <> String.pad_leading(to_string(version), 2, "0"))
  end

  defp installed_version(prefix), do: stamped_version(prefix) || shape_floor(prefix)

  defp stamped_version(prefix) do
    Enum.find_value(@version_tables, &comment_version(&1, prefix))
  end

  defp shape_floor(prefix) do
    if renamed?(prefix), do: @renamed_version, else: 0
  end

  defp comment_version(table, prefix) do
    %{rows: rows} = repo().query!(@version_query, [table, prefix], log: false)

    with [[description]] <- rows,
         {version, ""} <- Integer.parse(description) do
      version
    else
      _unversioned -> nil
    end
  end

  defp record_version(_opts, 0), do: :ok

  defp record_version(%{prefix: prefix}, version) do
    execute("COMMENT ON TABLE #{qualified(prefix, version_table(version))} IS '#{version}'")
  end

  defp version_table(version) when version >= @renamed_version, do: "ariadne_flow_store"
  defp version_table(_version), do: "modac_flow_store"

  defp normalize(opts, default_version) do
    opts
    |> Enum.into(%{prefix: @default_prefix, version: default_version})
    |> validate!()
  end

  defp validate!(%{prefix: prefix}) when not is_binary(prefix) do
    raise ArgumentError, "expected :prefix to be a schema name, got: #{inspect(prefix)}"
  end

  defp validate!(%{version: version})
       when not is_integer(version) or version < @initial_version or version > @current_version do
    raise ArgumentError,
          "expected :version to be between #{@initial_version} and #{@current_version}, " <>
            "got: #{inspect(version)}"
  end

  defp validate!(opts), do: opts
end
