defmodule Ariadne.Flow.Store.Postgres do
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Postgres.Query
  alias Ariadne.Flow.Store.StoredEventReactor

  @batch_size 100

  def init(opts \\ []) do
    %Store{module: __MODULE__, config: Query.init(opts)}
  end

  def read(%Query{} = config, query, opts) do
    Query.read(config, query, opts)
  end

  def append(%Query{} = config, events, opts) do
    Query.append_events(config, events, opts)
  end

  def consume(%Query{} = config, %StoredEventReactor{} = reactor) do
    Query.consume(config, reactor, @batch_size)
  end

  def transaction(%Query{} = config, fun) when is_function(fun, 0) do
    Query.transaction(config, fun)
  end

  def total_events(%Store{module: __MODULE__, config: %Query{} = config}) do
    Query.count_total_events(config)
  end

  def telemetry_metadata(%Query{prefix: prefix, context: context}) do
    %{prefix: prefix, context: context}
  end

  def dump(%Query{repo: repo, prefix: prefix, context: context}) do
    %{"repo" => Atom.to_string(repo), "prefix" => prefix, "context" => context}
  end

  def load(%{"repo" => repo, "prefix" => prefix, "context" => context}) do
    Query.init(repo: String.to_existing_atom(repo), prefix: prefix, context: context)
  end
end
