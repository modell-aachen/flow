defmodule Ariadne.Flow.Store.Postgres do
  @moduledoc false
  @behaviour Ariadne.Flow.Store.Backend

  import Ecto.Query

  alias Ariadne.Flow.ConsumeResult
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.Backend
  alias Ariadne.Flow.Store.SequencedRecord
  alias Ariadne.Flow.Store.StoredEventReactor

  @enforce_keys [:repo, :prefix, :context]

  @batch_size 100
  @store_lock_domain "modac_flow_postgres_store"
  @checkpoint_lock_domain "modac_flow_reactor_checkpoint_lock"

  defstruct @enforce_keys

  defmodule EctoEvent do
    @moduledoc false
    use Ecto.Schema

    @primary_key {:position, :integer, []}
    schema "modac_flow_store" do
      field(:type, :string)
      field(:context, :string)
      field(:data, :map)
      field(:tags, {:array, :string})
      field(:metadata, :map)
      field(:created_at, :utc_datetime_usec)

      has_many(:tag_entries, Ariadne.Flow.Store.Postgres.EctoTag, foreign_key: :position)
    end
  end

  defmodule EctoTag do
    @moduledoc false
    use Ecto.Schema

    @primary_key false
    schema "modac_flow_store_tags" do
      field(:position, :integer, primary_key: true)
      field(:tag, :string, primary_key: true)

      belongs_to(:event, Ariadne.Flow.Store.Postgres.EctoEvent,
        foreign_key: :position,
        references: :position,
        define_field: false
      )
    end
  end

  defmodule EctoReactorCheckpoint do
    @moduledoc false
    use Ecto.Schema

    @primary_key false
    schema "modac_flow_store_reactor_checkpoints" do
      field(:context, :string, primary_key: true)
      field(:name, :string, primary_key: true)
      field(:position, :integer)
      field(:updated_at, :utc_datetime_usec)
    end
  end

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
    config = %__MODULE__{
      repo: Keyword.fetch!(opts, :repo),
      prefix: Keyword.get(opts, :prefix, "public"),
      context: Keyword.get(opts, :context, "default")
    }

    %Store{module: __MODULE__, config: config}
  end

  @impl Backend
  def read(%__MODULE__{repo: repo, prefix: prefix, context: context}, %Query{filters: :all}, opts) do
    after_position = Keyword.get(opts, :after, 0)
    limit = Keyword.get(opts, :limit)

    events =
      EctoEvent
      |> where([e], e.context == ^context and e.position > ^after_position)
      |> order_by([e], e.position)
      |> apply_limit(limit)
      |> repo.all(prefix: prefix)
      |> Enum.map(&to_sequenced_record/1)

    %{events: events}
  end

  def read(%__MODULE__{}, %Query{filters: []}, _opts) do
    %{events: []}
  end

  def read(
        %__MODULE__{repo: repo, prefix: prefix, context: context},
        %Query{filters: filters},
        opts
      ) do
    after_position = Keyword.get(opts, :after, 0)
    limit = Keyword.get(opts, :limit)

    events =
      filters
      |> to_ecto_query(%{after_position: after_position, context: context, limit: limit})
      |> repo.all(prefix: prefix)
      |> Enum.map(&to_sequenced_record/1)

    %{events: events}
  end

  @impl Backend
  def append(%__MODULE__{repo: repo, prefix: prefix, context: context} = config, events, opts)
      when is_list(events) and is_list(opts) do
    append_condition = Keyword.get(opts, :condition)

    {:ok, result} =
      repo.transaction(fn ->
        lock_store!(repo, prefix, context)

        if append_condition_passed?(config, append_condition, opts) do
          serialized_events = Enum.map(events, &map_to_ecto(config, &1, opts))

          {_count, inserted_events} =
            repo.insert_all(EctoEvent, serialized_events,
              returning: true,
              prefix: prefix
            )

          tag_entries = to_tag_entries(inserted_events)

          repo.insert_all(EctoTag, tag_entries, prefix: prefix)

          {:ok, %{events: Enum.map(inserted_events, &to_sequenced_record/1)}}
        else
          {:error, :append_condition_failed}
        end
      end)

    result
  end

  def append(%__MODULE__{} = config, %Store.Record{} = event, opts),
    do: append(config, [event], opts)

  @impl Backend
  def count(%__MODULE__{repo: repo, prefix: prefix, context: context}) do
    EctoEvent
    |> where([e], e.context == ^context)
    |> repo.aggregate(:count, prefix: prefix)
  end

  @impl Backend
  def consume(%__MODULE__{repo: repo} = config, %StoredEventReactor{
        name: name,
        query: query,
        handler: handler
      }) do
    {:ok, result} =
      repo.transaction(fn ->
        lock_checkpoint!(repo, config.prefix, config.context, name)

        prior_position = checkpoint(config, name) || 0

        %{events: events} = read(config, query, after: prior_position, limit: @batch_size + 1)

        {batch, more_in_store?} =
          if length(events) > @batch_size,
            do: {Enum.take(events, @batch_size), true},
            else: {events, false}

        {result, new_position} = build_result(batch, prior_position, more_in_store?, handler)
        write_checkpoint_position(config, name, new_position)
        result
      end)

    result
  end

  @impl Backend
  def checkpoint(%__MODULE__{repo: repo, prefix: prefix, context: context}, name) do
    case repo.get_by(EctoReactorCheckpoint, [context: context, name: name], prefix: prefix) do
      nil -> nil
      %EctoReactorCheckpoint{position: position} -> position
    end
  end

  @impl Backend
  def init_checkpoints(%__MODULE__{repo: repo, prefix: prefix, context: context}, checkpoints) do
    updated_at = DateTime.utc_now()

    entries =
      Enum.map(checkpoints, fn %{name: name, position: position} ->
        %{context: context, name: name, position: position, updated_at: updated_at}
      end)

    repo.insert_all(EctoReactorCheckpoint, entries,
      on_conflict: :nothing,
      conflict_target: [:context, :name],
      prefix: prefix
    )

    :ok
  end

  @impl Backend
  def transaction(%__MODULE__{repo: repo}, fun) when is_function(fun, 0) do
    case repo.transaction(fun) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  @impl Backend
  def in_transaction?(%__MODULE__{repo: repo}), do: repo.in_transaction?()

  @impl Backend
  def telemetry_metadata(%__MODULE__{prefix: prefix, context: context}) do
    %{prefix: prefix, context: context}
  end

  @impl Backend
  def dump(%__MODULE__{repo: repo, prefix: prefix, context: context}) do
    %{"repo" => Atom.to_string(repo), "prefix" => prefix, "context" => context}
  end

  @impl Backend
  def load(%{"repo" => repo, "prefix" => prefix, "context" => context}) do
    %__MODULE__{repo: String.to_existing_atom(repo), prefix: prefix, context: context}
  end

  @doc false
  def advisory_lock_key(parts) when is_list(parts) do
    <<key::signed-integer-size(64), _rest::binary>> =
      :crypto.hash(:sha256, Enum.map_join(parts, &"#{byte_size(&1)}:#{&1}"))

    key
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

  defp write_checkpoint_position(
         %__MODULE__{repo: repo, prefix: prefix, context: context},
         name,
         position
       ) do
    repo.insert_all(
      EctoReactorCheckpoint,
      [%{context: context, name: name, position: position, updated_at: DateTime.utc_now()}],
      on_conflict: {:replace, [:position, :updated_at]},
      conflict_target: [:context, :name],
      prefix: prefix
    )
  end

  defp to_sequenced_record(%EctoEvent{} = event) do
    %SequencedRecord{
      created_at: event.created_at,
      record: %Store.Record{
        type: event.type,
        data: event.data,
        tags: event.tags
      },
      position: event.position,
      metadata: event.metadata
    }
  end

  defp to_ecto_query(filters, %{limit: limit} = opts) do
    filters
    |> Enum.map(&build_filter_query(&1, opts))
    |> combine_with_union()
    |> apply_limit(limit)
  end

  defp build_filter_query(filter, %{after_position: after_position, context: context}) do
    filter
    |> build_individual_query()
    |> where([e], e.position > ^after_position)
    |> where([e], e.context == ^context)
    |> restrict_to_last_event(filter)
  end

  defp restrict_to_last_event(query, %{only_last_event: false}), do: query

  defp restrict_to_last_event(query, %{only_last_event: true}) do
    last_event =
      query
      |> order_by([e], desc: e.position)
      |> limit(1)

    from(e in subquery(last_event))
  end

  defp build_individual_query(%{types: types, tags: nil}) do
    where(EctoEvent, [e], e.type in ^types)
  end

  defp build_individual_query(%{types: types, tags: tags}) do
    EctoEvent
    |> join(:inner, [e], t in EctoTag, on: t.position == e.position)
    |> where([e, t], e.type in ^types and t.tag in ^tags)
    |> group_by([e], e.position)
    |> having([e, t], count(t.tag) == ^length(tags))
    |> select([e], e)
  end

  defp combine_with_union([first_query | rest_queries]) do
    rest_queries
    |> Enum.reduce(first_query, fn query, acc ->
      acc
      |> subquery()
      |> union(^query)
    end)
    |> subquery()
    |> order_by([e], e.position)
  end

  defp apply_limit(query, nil), do: query
  defp apply_limit(query, n) when is_integer(n), do: limit(query, ^n)

  defp to_tag_entries(events) do
    Enum.flat_map(events, fn %{position: position, tags: tags} ->
      Enum.map(tags, &%{position: position, tag: &1})
    end)
  end

  defp lock_store!(repo, prefix, context),
    do: acquire_advisory_lock!(repo, [@store_lock_domain, prefix, context])

  defp lock_checkpoint!(repo, prefix, context, name),
    do: acquire_advisory_lock!(repo, [@checkpoint_lock_domain, prefix, context, name])

  defp acquire_advisory_lock!(repo, parts) do
    repo.query!("SELECT pg_advisory_xact_lock($1)", [advisory_lock_key(parts)])
  end

  defp append_condition_passed?(_, nil, _), do: true

  defp append_condition_passed?(
         %__MODULE__{} = config,
         %{fail_if_events_match: query, after: after_condition},
         opts
       ) do
    opts =
      opts
      |> Keyword.put(:after, after_condition)
      |> Keyword.put(:limit, 1)

    config
    |> read(query, opts)
    |> Map.get(:events, [])
    |> Enum.empty?()
  end

  defp map_to_ecto(%__MODULE__{context: context}, %Store.Record{} = event, opts) do
    created_at = Keyword.get(opts, :created_at, DateTime.utc_now())
    metadata = Keyword.get(opts, :metadata, %{})

    %{
      type: event.type,
      data: event.data,
      tags: event.tags,
      created_at: created_at,
      metadata: metadata,
      context: context
    }
  end
end
