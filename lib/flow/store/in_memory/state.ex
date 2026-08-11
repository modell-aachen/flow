defmodule Ariadne.Flow.Store.InMemory.State do
  @moduledoc false
  alias Ariadne.Flow.Filter
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store.Record
  alias Ariadne.Flow.Store.SequencedRecord
  alias Ariadne.Flow.Store.StoredEventReactor

  defstruct position: 0, events: [], checkpoints: %{}, consume_locks: %{}

  def init(_opts \\ []), do: %__MODULE__{}

  # Who is consuming is not stored data, so a rollback must keep the locks the store
  # holds now rather than resurrecting the ones it held when the snapshot was taken.
  def restore(%__MODULE__{} = snapshot, %__MODULE__{consume_locks: consume_locks}),
    do: %__MODULE__{snapshot | consume_locks: consume_locks}

  def read(%__MODULE__{} = state, query, opts) do
    after_position = Keyword.get(opts, :after, 0)
    limit = Keyword.get(opts, :limit, :infinity)

    events =
      state.events
      |> Enum.reverse()
      |> Enum.filter(&(&1.position > after_position))
      |> filter(query)
      |> take(limit)

    %{events: events}
  end

  def count(%__MODULE__{events: events}), do: length(events)

  def checkpoint(%__MODULE__{checkpoints: checkpoints}, name), do: Map.get(checkpoints, name)

  def init_checkpoints(%__MODULE__{} = state, checkpoints) do
    Enum.reduce(checkpoints, state, fn %{name: name, position: position}, %__MODULE__{} = acc ->
      %__MODULE__{acc | checkpoints: Map.put_new(acc.checkpoints, name, position)}
    end)
  end

  def append(%__MODULE__{} = state, events, opts) when is_list(events) and is_list(opts) do
    append_condition = Keyword.get(opts, :condition)

    if append_condition_passed?(state, append_condition) do
      {new_state, appended_events} = append_events(state, events, opts)
      {:ok, new_state, appended_events}
    else
      {:error, :append_condition_failed}
    end
  end

  def append(%__MODULE__{} = state, %Record{} = record, opts), do: append(state, [record], opts)

  def unconsumed(%__MODULE__{} = state, %StoredEventReactor{name: name, query: query}, limit)
      when is_integer(limit) and limit > 0 do
    prior_position = checkpoint(state, name) || 0
    %{events: events} = read(state, query, after: prior_position, limit: limit)

    {prior_position, events}
  end

  def put_checkpoint(%__MODULE__{} = state, name, position) do
    %__MODULE__{state | checkpoints: Map.put(state.checkpoints, name, position)}
  end

  # A holder that died without unlocking leaves its lock behind, so an acquisition takes
  # one over rather than waiting on a process that will never release it.
  def lock_consume(%__MODULE__{consume_locks: consume_locks} = state, name, pid) do
    case Map.get(consume_locks, name) do
      nil ->
        {:ok, put_consume_lock(state, name, %{holder: pid, waiting: []})}

      %{holder: holder, waiting: waiting} = lock ->
        if Process.alive?(holder) do
          {{:wait, holder},
           put_consume_lock(state, name, %{lock | waiting: joining(waiting, pid)})}
        else
          {:ok, put_consume_lock(state, name, %{holder: pid, waiting: waiting})}
        end
    end
  end

  def unlock_consume(%__MODULE__{consume_locks: consume_locks} = state, name) do
    {%{waiting: waiting}, remaining} = Map.pop!(consume_locks, name)

    {waiting, %__MODULE__{state | consume_locks: remaining}}
  end

  defp put_consume_lock(%__MODULE__{} = state, name, lock) do
    %__MODULE__{state | consume_locks: Map.put(state.consume_locks, name, lock)}
  end

  defp joining(waiting, pid), do: if(pid in waiting, do: waiting, else: [pid | waiting])

  defp take(list, :infinity), do: list
  defp take(list, n) when is_integer(n), do: Enum.take(list, n)

  defp filter(sequenced_events, %Query{filters: :all}) when is_list(sequenced_events),
    do: sequenced_events

  defp filter(sequenced_events, %Query{filters: filters}) when is_list(sequenced_events) do
    filters
    |> Enum.flat_map(&select(sequenced_events, &1))
    |> Enum.uniq_by(& &1.position)
    |> Enum.sort_by(& &1.position)
  end

  defp select(sequenced_events, %Filter{} = filter) do
    sequenced_events
    |> Enum.filter(&Filter.matches?(filter, &1.record))
    |> Filter.take_last(filter)
  end

  defp append_events(state, records, opts) do
    created_at = Keyword.get(opts, :created_at, DateTime.utc_now())
    metadata = Keyword.get(opts, :metadata, %{})

    {new_state, appended} =
      Enum.reduce(records, {state, []}, fn %Record{} = record, {%__MODULE__{} = acc, appended} ->
        sequenced = %SequencedRecord{
          record: record,
          position: acc.position + 1,
          created_at: created_at,
          metadata: metadata
        }

        {%__MODULE__{acc | position: acc.position + 1, events: [sequenced | acc.events]},
         [sequenced | appended]}
      end)

    {new_state, Enum.reverse(appended)}
  end

  defp append_condition_passed?(_state, nil), do: true

  defp append_condition_passed?(%__MODULE__{} = state, %{
         fail_if_events_match: query,
         after: after_condition
       }) do
    %{events: events} = read(state, query, after: after_condition, limit: 1)

    Enum.empty?(events)
  end

  defp append_condition_passed?(_, _), do: raise("Unsupported append condition")
end
