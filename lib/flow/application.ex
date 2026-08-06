defmodule Ariadne.Flow.Application do
  @moduledoc false
  alias Ariadne.Flow.AppendConditionError
  alias Ariadne.Flow.Attempts
  alias Ariadne.Flow.CommandError
  alias Ariadne.Flow.CommandHandler
  alias Ariadne.Flow.Consistency
  alias Ariadne.Flow.EventReducer
  alias Ariadne.Flow.Handoff
  alias Ariadne.Flow.PostCommitError
  alias Ariadne.Flow.ReactorEngine
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.ReactorRun
  alias Ariadne.Flow.Store

  require Logger

  defstruct [:store, reactors: [], engine: nil]

  def new(%{store: %Store{} = store} = attrs) do
    %__MODULE__{
      store: store,
      reactors: Map.get(attrs, :reactors, []),
      engine: ReactorEngine.normalize(Map.get(attrs, :engine))
    }
  end

  def dispatch(%__MODULE__{store: store, reactors: reactors, engine: engine}, command, opts \\ []) do
    nested = Store.in_transaction?(store)
    metadata = Keyword.get(opts, :metadata, %{})

    command_handler =
      CommandHandler.new(%{
        command: command,
        metadata: metadata,
        created_at: Keyword.get(opts, :created_at)
      })

    handoff =
      Handoff.new(%{reactors: reactors, engine: engine, metadata: metadata, nested: nested})

    consistency =
      Consistency.new(%{
        reactors: reactors,
        nested: nested,
        await_timeout: Keyword.get(opts, :await_timeout)
      })

    attempts = Attempts.new(%{attempts: Keyword.get(opts, :attempts), nested: nested})

    :telemetry.span([:ariadne, :flow, :dispatch], %{}, fn ->
      {committed, attempted} =
        Attempts.run(attempts, fn -> append_and_hand_off(store, command_handler, handoff) end)

      result = react(committed, store, consistency)

      {result, %{attempts: attempted}, %{result: outcome(result)}}
    end)
  end

  def dispatch!(%__MODULE__{} = application, command, opts \\ []) do
    case dispatch(application, command, opts) do
      {:error, %AppendConditionError{} = error} -> raise error
      {:error, reason} -> raise CommandError, reason: reason
      result -> result
    end
  end

  def catch_up(%__MODULE__{store: store, reactors: reactors, engine: engine}, opts \\ []) do
    %{reactors: reactors, engine: engine, metadata: Keyword.get(opts, :metadata, %{})}
    |> Handoff.new()
    |> Handoff.catch_up(store)
    |> Handoff.execute(store)
    |> reactor_error()
  end

  def query(%__MODULE__{store: store}, event_reducer) do
    EventReducer.evaluate(event_reducer, store).result
  end

  defp append_and_hand_off(store, command_handler, handoff) do
    Store.transaction(store, fn ->
      with {:ok, %{events: events} = result} <- CommandHandler.handle(command_handler, store) do
        {:ok, result, Handoff.hand_off(handoff, store, events)}
      end
    end)
  end

  defp react({:ok, result, reactor_runs}, store, consistency) do
    reactor_runs
    |> Handoff.execute(store)
    |> surface()

    await({:ok, result}, consistency, store)
  end

  defp react(result, _store, _consistency), do: result

  defp surface(failures) do
    {awaited, isolated} = Enum.split_with(failures, &ReactorRun.sync?(&1.run))

    Enum.each(isolated, &log/1)

    if awaited != [], do: raise(PostCommitError.failure(Handoff.summarize(awaited)))
  end

  defp log(%{name: name, position: position, reason: reason, stacktrace: stacktrace}) do
    Logger.error(fn ->
      "[ariadne.flow] reactor #{inspect(name)} failed#{at(position)}: " <>
        "#{describe(reason, stacktrace)}\nThe dispatch's events are committed and the " <>
        "reactor's checkpoint stayed put, so the next dispatch or catch_up runs it again."
    end)
  end

  defp at(nil), do: ""
  defp at(position), do: " at position #{position}"

  defp describe(reason, nil), do: inspect(reason)
  defp describe(exception, stacktrace), do: Exception.format(:error, exception, stacktrace)

  defp reactor_error([]), do: :ok
  defp reactor_error(failures), do: {:error, %ReactorError{failures: Handoff.summarize(failures)}}

  defp outcome({:ok, _result}), do: :ok
  defp outcome({:error, %AppendConditionError{}}), do: :conflict
  defp outcome({:error, _reason}), do: :error

  defp await({:ok, %{events: events}} = result, consistency, store) do
    case Consistency.await(consistency, store, events) do
      :ok -> result
      {:error, %PostCommitError{} = error} -> raise error
    end
  end
end
