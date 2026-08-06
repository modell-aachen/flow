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

  defstruct [:store, reactors: [], engine: nil]

  def new(%{store: %Store{} = store} = attrs) do
    %__MODULE__{
      store: store,
      reactors: distinctly_named(Map.get(attrs, :reactors, [])),
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

      result = react(committed, store, consistency, nested)

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

  defp distinctly_named(reactors) do
    case reactors
         |> Enum.frequencies_by(& &1.reactor().name)
         |> Enum.filter(&(elem(&1, 1) > 1)) do
      [] ->
        reactors

      repeated ->
        raise ArgumentError,
              "reactors must be distinctly named, a name being what a checkpoint is keyed " <>
                "on — repeated: #{Enum.map_join(repeated, ", ", &inspect(elem(&1, 0)))}"
    end
  end

  defp append_and_hand_off(store, command_handler, handoff) do
    Store.transaction(store, fn ->
      with {:ok, %{events: events} = result} <- CommandHandler.handle(command_handler, store) do
        {:ok, result, Handoff.hand_off(handoff, store, events)}
      end
    end)
  end

  defp react({:ok, result, reactor_runs}, store, consistency, nested) do
    reactor_runs
    |> Handoff.execute(store)
    |> surface(nested)

    await({:ok, result}, consistency, store)
  end

  defp react(result, _store, _consistency, _nested), do: result

  defp surface(failures, nested) do
    case Enum.filter(failures, &ReactorRun.sync?(&1.run)) do
      [] -> :ok
      awaited -> raise PostCommitError.failure(Handoff.summarize(awaited), nested)
    end
  end

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
