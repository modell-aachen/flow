defmodule Ariadne.Flow.Store.Speedrun.Reporter do
  use GenServer

  def start_link do
    GenServer.start_link(
      __MODULE__,
      %{
        start_time: :os.system_time(:millisecond),
        total_read_count: 0,
        total_read_time: 0,
        total_append_count: 0,
        total_append_time: 0
      },
      name: __MODULE__
    )
  end

  def init(state) do
    {:ok, state}
  end

  def stats do
    GenServer.call(__MODULE__, :stats)
  end

  def handle_call(:stats, _from, state) do
    average_read_time =
      if state.total_read_count > 0 do
        state.total_read_time / state.total_read_count
      else
        0
      end

    elapsed_time = :os.system_time(:millisecond) - state.start_time

    average_append_time =
      if state.total_append_count > 0 do
        state.total_append_time / state.total_append_count
      else
        0
      end

    result = %{
      average_read_time_ms: average_read_time / 1000,
      read_ops_per_sec: state.total_read_count * 1000 / elapsed_time,
      average_append_time_ms: average_append_time / 1000,
      append_ops_per_sec: state.total_append_count * 1000 / elapsed_time
    }

    {:reply, result, state}
  end

  def record_read(time) do
    GenServer.cast(__MODULE__, {:record_read, time})
  end

  def record_append(time) do
    GenServer.cast(__MODULE__, {:record_append, time})
  end

  def handle_cast({:record_read, time}, state) do
    {:noreply,
     %{
       state
       | total_read_count: state.total_read_count + 1,
         total_read_time: state.total_read_time + time
     }}
  end

  def handle_cast({:record_append, time}, state) do
    {:noreply,
     %{
       state
       | total_append_count: state.total_append_count + 1,
         total_append_time: state.total_append_time + time
     }}
  end
end
