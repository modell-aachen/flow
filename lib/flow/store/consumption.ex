defmodule Ariadne.Flow.Store.Consumption do
  @moduledoc false
  alias Ariadne.Flow.ConsumeResult

  @doc """
  Splits what a read of `batch_size + 1` events returned into the batch a reactor is
  handed and whether the store is holding more behind it.
  """
  def split(events, batch_size) when is_list(events) and is_integer(batch_size) do
    if length(events) > batch_size,
      do: {Enum.take(events, batch_size), true},
      else: {events, false}
  end

  @doc """
  Hands the batch to the handler and returns its result together with the position the
  reactor's checkpoint moves to — the last event the handler got through, or where the
  reactor already stood when it got through none.
  """
  def run(batch, prior_position, more_in_store?, handler)
      when is_list(batch) and is_function(handler, 1) do
    case handler.(batch) do
      {:ok, results} ->
        consumed(batch, prior_position, :ok, results, more_in_store?, nil)

      {:error, results, failure} ->
        consumed(batch, prior_position, :error, results, false, failure)
    end
  end

  defp consumed(batch, prior_position, status, results, more?, failure) do
    processed = length(results)
    new_position = position_after(batch, processed, prior_position)

    {%ConsumeResult{
       status: status,
       processed: processed,
       results: results,
       last_position: new_position,
       more?: more?,
       failure: failure
     }, new_position}
  end

  defp position_after(_batch, 0, prior_position), do: prior_position
  defp position_after(batch, count, _prior) when count > 0, do: Enum.at(batch, count - 1).position
end
