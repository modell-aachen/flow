defmodule Ariadne.Flow.Store.Postgres.Migration do
  @moduledoc false
  use Ecto.Migration

  @initial_version 1
  @current_version 2
  @default_prefix "public"
  @version_table "modac_flow_store"

  @version_query """
  SELECT pg_description.description
  FROM pg_class
  JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
  JOIN pg_description ON pg_description.objoid = pg_class.oid AND pg_description.objsubid = 0
  WHERE pg_class.relname = $1 AND pg_namespace.nspname = $2
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
    installed = rollback_from(installed_version(opts.prefix))

    if installed >= opts.version do
      apply_versions(installed..opts.version//-1, :down, opts)
      record_version(opts, opts.version - 1)
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

  defp rollback_from(0), do: @current_version
  defp rollback_from(installed), do: installed

  defp apply_versions(versions, direction, opts) do
    Enum.each(versions, fn version -> apply(version_module(version), direction, [opts]) end)
  end

  defp version_module(version) do
    Module.concat(__MODULE__, "V" <> String.pad_leading(to_string(version), 2, "0"))
  end

  defp installed_version(prefix) do
    %{rows: rows} = repo().query!(@version_query, [@version_table, prefix], log: false)

    with [[description]] <- rows,
         {version, ""} <- Integer.parse(description) do
      version
    else
      _unversioned -> 0
    end
  end

  defp record_version(_opts, 0), do: :ok

  defp record_version(%{prefix: prefix}, version) do
    execute("COMMENT ON TABLE #{qualified(prefix, @version_table)} IS '#{version}'")
  end

  defp quoted(identifier), do: ~s("#{String.replace(identifier, ~s("), ~s(""))}")

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
