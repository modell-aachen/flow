defmodule Ariadne.Flow.Store do
  @moduledoc """
  A store: a backend module paired with the config that backend needs.

  Every function here dispatches to the backend and adds what is the same for all of
  them — query and append condition normalisation, telemetry spans. What a backend has
  to provide in return is `Ariadne.Flow.Store.Backend`. Build a store with the
  backend's own `init/1`, `Ariadne.Flow.Store.Postgres.init/1` for the store Flow ships
  for production use.
  """
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store.AppendCondition
  alias Ariadne.Flow.Store.Backend
  alias Ariadne.Flow.Store.Read
  alias Ariadne.Flow.Store.StoredEventReactor

  @enforce_keys [:module, :config]
  defstruct @enforce_keys

  @type t :: %__MODULE__{module: module(), config: Backend.config()}

  @doc """
  Reads the events matching `query` and returns them as an `Ariadne.Flow.Store.Read` —
  the events together with the normalised query that selected them, so a dispatch
  normalises its query once and builds its append condition from the same read.
  """
  def read(%__MODULE__{module: module, config: config}, query \\ :all, opts \\ []) do
    query = Query.new(query)
    base_metadata = telemetry_metadata(module, config)

    :telemetry.span(
      [:ariadne, :flow, :store, :read],
      base_metadata,
      fn ->
        %{events: events} = module.read(config, query, opts)

        {Read.new(query, events), %{event_count: length(events)},
         Map.put(base_metadata, :query, query)}
      end
    )
  end

  def count(%__MODULE__{module: module, config: config}) do
    module.count(config)
  end

  def consume(%__MODULE__{module: module, config: config}, %StoredEventReactor{} = reactor) do
    module.consume(config, %{reactor | query: Query.new(reactor.query)})
  end

  def transaction(%__MODULE__{module: module, config: config}, fun) when is_function(fun, 0) do
    module.transaction(config, fun)
  end

  def dump(%__MODULE__{module: module, config: config}) do
    %{"module" => Atom.to_string(module), "config" => module.dump(config)}
  end

  def load(%{"module" => module, "config" => config}) do
    module = String.to_existing_atom(module)

    %__MODULE__{module: module, config: module.load(config)}
  end

  def append(%__MODULE__{module: module, config: config}, events, opts \\ []) do
    opts = validate_append_condition(opts)

    base_metadata =
      Map.put(telemetry_metadata(module, config), :condition, Keyword.get(opts, :condition))

    :telemetry.span(
      [:ariadne, :flow, :store, :append],
      base_metadata,
      fn ->
        result = module.append(config, events, opts)
        {measurements, metadata} = append_stop(base_metadata, events, result)
        {result, measurements, metadata}
      end
    )
  end

  defp validate_append_condition(opts) do
    case Keyword.get(opts, :condition) do
      nil -> opts
      condition -> Keyword.put(opts, :condition, AppendCondition.new(condition))
    end
  end

  defp count_events(events) when is_list(events), do: length(events)
  defp count_events(_event), do: 1

  defp append_stop(base_metadata, _events, {:ok, %{events: sequenced}}) do
    {%{event_count: length(sequenced)}, Map.put(base_metadata, :result, :ok)}
  end

  defp append_stop(base_metadata, events, {:error, reason}) do
    {%{event_count: count_events(events)},
     Map.merge(base_metadata, %{result: :error, error: reason})}
  end

  defp telemetry_metadata(module, config) do
    Map.put(module.telemetry_metadata(config), :backend, module)
  end
end
