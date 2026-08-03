defmodule Ariadne.Flow.Application do
  @moduledoc false
  alias Ariadne.Flow.AppendConditionError
  alias Ariadne.Flow.CommandError
  alias Ariadne.Flow.CommandHandler
  alias Ariadne.Flow.Consistency
  alias Ariadne.Flow.ConsistencyTimeoutError
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

    # Asked before the dispatch opens its own transaction, because inside one the answer is
    # always yes: what decides whether a sync run can be confirmed from outside is the
    # transaction the *caller* brought, not the one the dispatch is about to open.
    nested = Store.in_transaction?(store)
    consistency = Consistency.new(reactors, nested, opts)

    store
    |> Store.transaction(fn ->
      with {:ok, %{events: events} = result} <- CommandHandler.handle(command, store, opts),
           :ok <- Reactions.react(reactors, store, events, metadata, engine, nested) do
        {:ok, result}
      end
    end)
    |> raise_reactor_failure()
    |> confirm(consistency, store)
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

  # The wait belongs here, after the commit, and only here: while the dispatch's own
  # transaction is open its events are invisible, so no sync reactor's checkpoint could
  # pass them. A reactor that already failed the pass has raised by now — a definitive
  # failure says more than a wait that would only ever run out.
  defp confirm({:ok, %{events: events}} = result, consistency, store) do
    case Consistency.await(consistency, store, events) do
      :ok -> result
      {:error, %ConsistencyTimeoutError{} = error} -> raise error
    end
  end

  defp confirm(result, _consistency, _store), do: result
end
