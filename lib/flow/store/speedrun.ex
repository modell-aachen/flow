defmodule Ariadne.Flow.Store.Speedrun do
  alias Ariadne.Flow.Store

  def run_worker(module, adapter) do
    Task.async(fn ->
      store = adapter.start_store()

      fn -> module.run_iteration(store) end
      |> Stream.repeatedly()
      |> Stream.run()
    end)
  end

  def fill_events(module, adapter, progress_fn) do
    store = adapter.start_store()
    initial_events_count = Store.count(store)
    events_to_fill_count = module.initial_events_count() - initial_events_count

    if events_to_fill_count <= 0 do
      progress_fn.(100.0)
    else
      module.fill_events_stream()
      |> Stream.take(events_to_fill_count)
      |> Stream.chunk_every(1_000)
      |> Stream.chunk_every(10)
      |> Stream.each(&append_chunks_in_parallel(&1, store, module, adapter, progress_fn))
      |> Stream.run()
    end
  end

  defp append_chunks_in_parallel(chunks, store, module, adapter, progress_fn) do
    chunks
    |> Enum.map(fn chunk -> Task.async(fn -> Store.append(store, chunk) end) end)
    |> Task.await_many(60_000)

    events_count = Store.count(store)

    progress = Float.round(events_count / module.initial_events_count() * 100, 2)

    progress_fn.(progress)
  end
end
