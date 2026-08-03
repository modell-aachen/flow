defmodule Ariadne.Flow.Application do
  @moduledoc false
  alias Ariadne.Flow.AfterCommit
  alias Ariadne.Flow.AppendConditionError
  alias Ariadne.Flow.CommandError
  alias Ariadne.Flow.CommandHandler
  alias Ariadne.Flow.EventReducer
  alias Ariadne.Flow.Reactions
  alias Ariadne.Flow.ReactorEngine
  alias Ariadne.Flow.ReactorError
  alias Ariadne.Flow.Store

  defstruct [:store, reactors: [], engine: {ReactorEngine.Inline, []}]

  def new(%{store: %Store{} = store} = attrs) do
    %__MODULE__{
      store: store,
      reactors: Map.get(attrs, :reactors, []),
      engine: Reactions.normalize_engine(Map.get(attrs, :engine, ReactorEngine.Inline))
    }
  end

  def dispatch(%__MODULE__{store: store, reactors: reactors, engine: engine}, command, opts \\ []) do
    metadata = Keyword.get(opts, :metadata, %{})

    store
    |> Store.transaction(fn ->
      with {:ok, %{events: events} = result} <- CommandHandler.handle(command, store, opts),
           {:ok, after_commits} <- Reactions.react(reactors, store, events, metadata, engine) do
        {:ok, result, after_commits}
      end
    end)
    |> raise_reactor_failure()
    |> run_after_commits()
  end

  def dispatch!(%__MODULE__{} = application, command, opts \\ []) do
    case dispatch(application, command, opts) do
      {:error, %AppendConditionError{} = error} -> raise error
      {:error, reason} -> raise CommandError, reason: reason
      result -> result
    end
  end

  def query(%__MODULE__{store: store}, event_reducer) do
    EventReducer.evaluate(event_reducer, store).result
  end

  # A reactor failure happens after the append committed — Store.transaction commits
  # whenever the closure returns a value — so the raise must stay outside the closure:
  # raising inside would roll back the events and the failed reactor's checkpoint.
  defp raise_reactor_failure({:error, %ReactorError{} = error}), do: raise(error)
  defp raise_reactor_failure(result), do: result

  defp run_after_commits({:ok, result, after_commits}) do
    Enum.each(after_commits, &AfterCommit.run/1)
    {:ok, result}
  end

  defp run_after_commits(error), do: error
end
