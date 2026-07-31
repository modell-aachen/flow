defmodule Ariadne.Flow.Store.Postgres do
  @moduledoc """
  The store Flow ships for production use: events in Postgres, ordered by a global
  `bigserial` position.

  It implements `Ariadne.Flow.Store.Backend`. `Ariadne.Flow.Store.Postgres.Migration`
  defines the tables it needs; `init/1` takes the repo, schema prefix and context to
  reach them. See [the store guide](store.html) for both.
  """
  @behaviour Ariadne.Flow.Store.Backend

  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Backend
  alias Ariadne.Flow.Store.Postgres.Query
  alias Ariadne.Flow.Store.StoredEventReactor

  @batch_size 100

  @doc """
  Builds a store on the tables the migration created.

  * `:repo` (required) — the `Ecto.Repo` the queries run through
  * `:prefix` (default `"public"`) — the schema the tables live in, the one the
    migration ran against
  * `:context` (default `"default"`) — an isolation key: events appended through one
    context are invisible to every other, tables shared or not
  """
  @impl Backend
  def init(opts \\ []) do
    %Store{module: __MODULE__, config: Query.init(opts)}
  end

  @impl Backend
  def read(%Query{} = config, query, opts) do
    Query.read(config, query, opts)
  end

  @impl Backend
  def append(%Query{} = config, events, opts) do
    Query.append_events(config, events, opts)
  end

  @impl Backend
  def consume(%Query{} = config, %StoredEventReactor{} = reactor) do
    Query.consume(config, reactor, @batch_size)
  end

  @impl Backend
  def transaction(%Query{} = config, fun) when is_function(fun, 0) do
    Query.transaction(config, fun)
  end

  def total_events(%Store{module: __MODULE__, config: %Query{} = config}) do
    Query.count_total_events(config)
  end

  @impl Backend
  def telemetry_metadata(%Query{prefix: prefix, context: context}) do
    %{prefix: prefix, context: context}
  end

  @impl Backend
  def dump(%Query{repo: repo, prefix: prefix, context: context}) do
    %{"repo" => Atom.to_string(repo), "prefix" => prefix, "context" => context}
  end

  @impl Backend
  def load(%{"repo" => repo, "prefix" => prefix, "context" => context}) do
    Query.init(repo: String.to_existing_atom(repo), prefix: prefix, context: context)
  end
end
