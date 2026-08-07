defmodule Ariadne.Flow.Store.SlowOperationLogger do
  @moduledoc false
  require Logger

  @handler_id "modac-flow-store-slow-operations"

  @default_thresholds [read: 20, append: 100, init_checkpoints: 100]

  @events [
    [:ariadne, :flow, :store, :read, :stop],
    [:ariadne, :flow, :store, :append, :stop],
    [:ariadne, :flow, :store, :init_checkpoints, :stop]
  ]

  def attach(thresholds \\ thresholds()) do
    if Application.get_env(:ariadne_flow, :flow_store_slow_logging, true) do
      :telemetry.attach_many(@handler_id, @events, &__MODULE__.handle_event/4, thresholds)
    else
      :ok
    end
  end

  def handle_event(
        [:ariadne, :flow, :store, operation, :stop],
        %{duration: duration} = measurements,
        metadata,
        thresholds
      ) do
    duration_ms = System.convert_time_unit(duration, :native, :millisecond)
    threshold = Keyword.fetch!(thresholds, operation)

    if duration_ms >= threshold do
      log_slow_operation(operation, duration_ms, threshold, measurements, metadata)
    end

    :ok
  end

  defp log_slow_operation(operation, duration_ms, threshold, measurements, metadata) do
    Logger.info(
      "Slow flow store #{operation}: #{duration_ms}ms (threshold #{threshold}ms)",
      attributes(operation, duration_ms, threshold, measurements, metadata)
    )
  end

  defp attributes(operation, duration_ms, threshold, measurements, metadata) do
    Map.reject(
      %{
        "flow.operation" => operation,
        "flow.duration_ms" => duration_ms,
        "flow.threshold_ms" => threshold,
        "flow.context" => Map.get(metadata, :context),
        "flow.backend" => backend(metadata),
        "flow.event_count" => Map.get(measurements, :event_count),
        "flow.checkpoint_count" => Map.get(measurements, :checkpoint_count),
        "flow.query" => Map.get(metadata, :query),
        "flow.condition" => Map.get(metadata, :condition)
      },
      fn {_key, value} -> is_nil(value) end
    )
  end

  defp backend(%{backend: nil}), do: nil
  defp backend(%{backend: backend}), do: inspect(backend)
  defp backend(_metadata), do: nil

  defp thresholds do
    Keyword.merge(
      @default_thresholds,
      Application.get_env(:ariadne_flow, :flow_store_slow_thresholds, [])
    )
  end
end
