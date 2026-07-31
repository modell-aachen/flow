defmodule Ariadne.Flow.Store.InMemory.State do
  @moduledoc false
  alias Ariadne.Flow.ConsumeResult
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store.Event
  alias Ariadne.Flow.Store.SequencedEvent
  alias Ariadne.Flow.Store.StoredEventReactor

  defstruct position: 0, events: [], checkpoints: %{}

  def init(_opts \\ []), do: %__MODULE__{}

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

  def append(%__MODULE__{} = state, events, opts) when is_list(events) and is_list(opts) do
    append_condition = Keyword.get(opts, :condition)

    if append_condition_passed?(state, append_condition) do
      {new_state, appended_events} = append_events(state, events, opts)
      {:ok, new_state, appended_events}
    else
      {:error, :append_condition_failed}
    end
  end

  def append(%__MODULE__{} = state, %Event{} = event, opts), do: append(state, [event], opts)

  def consume(
        %__MODULE__{} = state,
        %StoredEventReactor{
          name: name,
          query: query,
          handler: handler,
          start_after_position: start_after_position
        },
        batch_size
      )
      when is_integer(batch_size) and batch_size > 0 do
    prior_position = Map.get(state.checkpoints, name, start_after_position)

    %{events: events} = read(state, query, after: prior_position, limit: batch_size + 1)

    {batch, more_in_store?} =
      if length(events) > batch_size,
        do: {Enum.take(events, batch_size), true},
        else: {events, false}

    {result, new_position} = build_result(batch, prior_position, more_in_store?, handler)
    {put_checkpoint_position(state, name, new_position), result}
  end

  defp build_result(batch, prior_position, more_in_store?, handler) do
    case handler.(batch) do
      {:ok, count} ->
        new_position = position_after(batch, count, prior_position)

        {%ConsumeResult{
           status: :ok,
           processed: count,
           last_position: new_position,
           more?: more_in_store?
         }, new_position}

      {:error, count, failure} ->
        new_position = position_after(batch, count, prior_position)

        {%ConsumeResult{
           status: :error,
           processed: count,
           last_position: new_position,
           more?: false,
           failure: failure
         }, new_position}
    end
  end

  defp position_after(_batch, 0, prior_position), do: prior_position
  defp position_after(batch, count, _prior) when count > 0, do: Enum.at(batch, count - 1).position

  defp put_checkpoint_position(%__MODULE__{} = state, name, position) do
    %__MODULE__{state | checkpoints: Map.put(state.checkpoints, name, position)}
  end

  defp take(list, :infinity), do: list
  defp take(list, n) when is_integer(n), do: Enum.take(list, n)

  defp filter(sequenced_events, :all) when is_list(sequenced_events), do: sequenced_events

  defp filter(sequenced_events, query) when is_list(sequenced_events) and is_list(query) do
    query
    |> Enum.flat_map(&select(sequenced_events, &1))
    |> Enum.uniq_by(& &1.position)
    |> Enum.sort_by(& &1.position)
  end

  defp select(sequenced_events, %Query.Item{} = item) do
    sequenced_events
    |> Enum.filter(&Query.Item.matches?(item, &1.event))
    |> take_last(item)
  end

  defp take_last(sequenced_events, %Query.Item{only_last_event: true}),
    do: Enum.take(sequenced_events, -1)

  defp take_last(sequenced_events, %Query.Item{only_last_event: false}), do: sequenced_events

  defp append_events(state, events, opts) do
    created_at = Keyword.get(opts, :created_at, DateTime.utc_now())
    metadata = Keyword.get(opts, :metadata, %{})

    {new_state, appended} =
      Enum.reduce(events, {state, []}, fn %Event{} = event, {acc, appended} ->
        sequenced = %SequencedEvent{
          event: event,
          position: acc.position + 1,
          created_at: created_at,
          metadata: metadata
        }

        {%__MODULE__{
           position: acc.position + 1,
           events: [sequenced | acc.events],
           checkpoints: acc.checkpoints
         }, [sequenced | appended]}
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
