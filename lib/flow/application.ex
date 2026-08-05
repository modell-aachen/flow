defmodule Ariadne.Flow.Application do
  @moduledoc false
  alias Ariadne.Flow.AppendConditionError
  alias Ariadne.Flow.Attempts
  alias Ariadne.Flow.CommandError
  alias Ariadne.Flow.CommandHandler
  alias Ariadne.Flow.Consistency
  alias Ariadne.Flow.ConsistencyTimeoutError
  alias Ariadne.Flow.EventReducer
  alias Ariadne.Flow.Handoff
  alias Ariadne.Flow.ReactorEngine
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.Store

  defstruct [:store, reactors: [], engine: {ReactorEngine.Inline, []}]

  def new(%{store: %Store{} = store} = attrs) do
    %__MODULE__{
      store: store,
      reactors: Map.get(attrs, :reactors, []),
      engine: ReactorEngine.normalize(Map.get(attrs, :engine, ReactorEngine.Inline))
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
      {appended, attempted} =
        Attempts.run(attempts, fn -> append_and_hand_off(store, command_handler, handoff) end)

      result =
        appended
        |> raise_reactor_failure()
        |> await(consistency, store)

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
    %{
      reactors: reactors,
      engine: engine,
      metadata: Keyword.get(opts, :metadata, %{}),
      nested: Store.in_transaction?(store)
    }
    |> Handoff.new()
    |> Handoff.catch_up(store)
  end

  def query(%__MODULE__{store: store}, event_reducer) do
    EventReducer.evaluate(event_reducer, store).result
  end

  defp append_and_hand_off(store, command_handler, handoff) do
    Store.transaction(store, fn ->
      with {:ok, %{events: events} = result} <- CommandHandler.handle(command_handler, store),
           :ok <- Handoff.hand_off(handoff, store, events) do
        {:ok, result}
      end
    end)
  end

  defp outcome({:ok, _result}), do: :ok
  defp outcome({:error, %AppendConditionError{}}), do: :conflict
  defp outcome({:error, _reason}), do: :error

  defp raise_reactor_failure({:error, %ReactorError{} = error}), do: raise(error)
  defp raise_reactor_failure(result), do: result

  defp await({:ok, %{events: events}} = result, consistency, store) do
    case Consistency.await(consistency, store, events) do
      :ok -> result
      {:error, %ConsistencyTimeoutError{} = error} -> raise error
    end
  end

  defp await(result, _consistency, _store), do: result
end
