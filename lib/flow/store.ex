defmodule Ariadne.Flow.Store do
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store.AppendCondition
  alias Ariadne.Flow.Store.StoredEventReactor

  @enforce_keys [:module, :config]
  defstruct @enforce_keys

  def read(%__MODULE__{module: module, config: config}, query \\ :all, opts \\ []) do
    query = Query.new(query)
    base_metadata = telemetry_metadata(module, config)

    :telemetry.span(
      [:ariadne, :flow, :store, :read],
      base_metadata,
      fn ->
        result =
          config
          |> module.read(query, opts)
          |> add_append_condition(query)

        {result, %{event_count: length(result.events)}, Map.put(base_metadata, :query, query)}
      end
    )
  end

  defp add_append_condition(%{events: events} = read_result, query) do
    after_condition = List.last(events, %{position: 0}).position

    Map.put(
      read_result,
      :append_condition,
      AppendCondition.new(%{fail_if_events_match: query, after: after_condition})
    )
  end

  def consume(%__MODULE__{module: module, config: config}, %StoredEventReactor{} = reactor) do
    module.consume(config, reactor)
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
