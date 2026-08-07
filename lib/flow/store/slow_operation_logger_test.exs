defmodule Ariadne.Flow.Store.SlowOperationLoggerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Ariadne.Flow.Store.SlowOperationLogger

  defmodule Capture do
    @moduledoc false
    def log(%{meta: %{pid: pid} = meta}, %{config: %{pid: pid}}), do: send(pid, {:log_meta, meta})
    def log(_event, _config), do: :ok
  end

  @moduletag :capture_log

  @thresholds [read: 20, append: 100]

  setup do
    Logger.put_module_level(SlowOperationLogger, :info)
  end

  defp emit(operation, duration_ms, metadata \\ %{}, measurements \\ %{}) do
    capture_log(fn -> trigger(operation, duration_ms, metadata, measurements) end)
  end

  defp emit_attributes(operation, duration_ms, metadata, measurements \\ %{}) do
    handler_id = :"capture_#{System.unique_integer([:positive])}"
    :ok = :logger.add_handler(handler_id, Capture, %{config: %{pid: self()}, level: :all})
    trigger(operation, duration_ms, metadata, measurements)
    :logger.remove_handler(handler_id)

    receive do
      {:log_meta, meta} -> meta
    after
      0 -> %{}
    end
  end

  defp trigger(operation, duration_ms, metadata, measurements) do
    SlowOperationLogger.handle_event(
      [:ariadne, :flow, :store, operation, :stop],
      Map.put(measurements, :duration, ms(duration_ms)),
      metadata,
      @thresholds
    )
  end

  defp ms(value), do: System.convert_time_unit(value, :millisecond, :native)

  test "logs at info when a read exceeds its threshold" do
    log = emit(:read, 25)

    assert log =~ "[info]"
    assert log =~ "Slow flow store read: 25ms (threshold 20ms)"
  end

  test "logs at info when an append exceeds its threshold" do
    log = emit(:append, 150)

    assert log =~ "Slow flow store append: 150ms (threshold 100ms)"
  end

  # Silence is asserted through the pid-targeted handler rather than capture_log/1, which
  # captures the whole VM's output and so sees any async test logging alongside this one.
  test "stays silent when a read is under its threshold" do
    assert %{} == emit_attributes(:read, 19, %{})
  end

  test "stays silent when an append is under its threshold" do
    assert %{} == emit_attributes(:append, 99, %{})
  end

  test "logs at the threshold boundary" do
    assert emit(:read, 20) =~ "Slow flow store read"
  end

  test "includes context, query and event count as attributes in a slow read log" do
    meta =
      emit_attributes(
        :read,
        50,
        %{context: "default", backend: Ariadne.Flow.Store.Postgres, query: :all},
        %{event_count: 42}
      )

    assert %{
             "flow.operation" => :read,
             "flow.duration_ms" => 50,
             "flow.threshold_ms" => 20,
             "flow.context" => "default",
             "flow.backend" => "Ariadne.Flow.Store.Postgres",
             "flow.event_count" => 42,
             "flow.query" => :all
           } = meta
  end

  test "includes the append condition as an attribute in a slow append log" do
    meta = emit_attributes(:append, 200, %{context: "default", condition: %{after: 7}})

    assert %{"flow.context" => "default", "flow.condition" => %{after: 7}} = meta
  end

  test "omits absent metadata fields" do
    meta = emit_attributes(:read, 50, %{context: "default"})

    refute Map.has_key?(meta, "flow.query")
    refute Map.has_key?(meta, "flow.condition")
    refute Map.has_key?(meta, "flow.backend")
  end
end
