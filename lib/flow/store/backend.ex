defmodule Ariadne.Flow.Store.Backend do
  @moduledoc """
  The contract every store backend implements.

  A store is a `t:Ariadne.Flow.Store.t/0` — a backend module paired with the config
  that module needs. `Ariadne.Flow.Store` keeps no state of its own; every call it
  takes it dispatches to `module.callback(config, ...)`. Flow ships two backends:
  `Ariadne.Flow.Store.Postgres` and `Ariadne.Flow.Store.InMemory`.

  Callers never invoke these callbacks directly. `Ariadne.Flow.Store` normalises the
  arguments first — the query through `Ariadne.Flow.Query.new/1`, the append condition
  through `Ariadne.Flow.Store.AppendCondition.new/1` — so a backend always receives an
  optimised `t:Ariadne.Flow.Query.t/0` and an `t:Ariadne.Flow.Store.AppendCondition.t/0`
  struct rather than the raw maps a caller wrote. `Ariadne.Flow.Store` also wraps
  `c:read/3` and `c:append/3` in telemetry spans. A backend implements storage, not
  validation and not instrumentation.

  ## What a backend guarantees

  * **Total order.** Every appended event gets a `position` higher than every position
    already in the store, and every read returns events in ascending position order.
  * **Atomic conditional append.** When `c:append/3` gets a `:condition`, no concurrent
    append may slip an event matching it in between the check and the write.
  * **Isolation.** Stores built from different configs — the Postgres backend's
    `:prefix` and `:context` — share neither events nor reactor checkpoints.
  * **Checkpoints per reactor.** `c:consume/2` records how far a reactor got under that
    reactor's name, and the next call against the same config resumes there.

  ## Implementing one

  `Ariadne.Flow.Store.InMemory` is the reference implementation: it is small enough to
  read in one sitting and covers the whole contract. `Ariadne.Flow.StoreTest` is the
  conformance suite — it is parameterised over the backends and every case in it holds
  for any implementation.
  """
  alias Ariadne.Flow.ConsumeResult
  alias Ariadne.Flow.Query
  alias Ariadne.Flow.Store
  alias Ariadne.Flow.Store.AppendCondition
  alias Ariadne.Flow.Store.Event
  alias Ariadne.Flow.Store.SequencedEvent
  alias Ariadne.Flow.Store.StoredEventReactor

  @typedoc "Whatever a backend needs to reach its storage. Carried in `t:Ariadne.Flow.Store.t/0`."
  @type config :: term()

  @typedoc """
  Options for `c:read/3`:

  * `:after` — skip events at or below this position (default `0`)
  * `:limit` — return at most this many events (default: all of them)
  """
  @type read_opts :: [after: non_neg_integer(), limit: pos_integer()]

  @typedoc """
  Options for `c:append/3`:

  * `:condition` — the append fails unless the store still satisfies it
  * `:created_at` — timestamp written to every event of the append (default: now)
  * `:metadata` — metadata written to every event of the append (default `%{}`)
  """
  @type append_opts :: [
          condition: AppendCondition.t(),
          created_at: DateTime.t(),
          metadata: map()
        ]

  @typedoc "What a read and a successful append return: the events, in position order."
  @type read_result :: %{events: [SequencedEvent.t()]}

  @doc """
  Builds a store backed by this module.

  The options are the backend's own — `Ariadne.Flow.Store.Postgres.init/1` takes
  `:repo`, `:prefix` and `:context`. The returned struct carries this module and the
  config the other callbacks receive.
  """
  @callback init(opts :: keyword()) :: Store.t()

  @doc """
  Returns the events matching `query`, in ascending position order.

  A query is the union of its items, each item contributing its own matches. An item
  with `only_last_event: true` contributes one event at most — the highest-positioned
  one matching it, of those the read covers, so `:after` narrows what "last" means and
  `:limit` cuts the union that selection is part of.

  Both options in `t:read_opts/0` apply to every query, `:all` included.
  """
  @callback read(config(), Query.t(), read_opts()) :: read_result()

  @doc """
  Appends events and returns them with the positions they were written at.

  A single event and a list of events are both accepted. The whole append is atomic:
  either every event lands or none does. `{:error, :append_condition_failed}` means the
  `:condition` no longer held and nothing was written.
  """
  @callback append(config(), Event.t() | [Event.t()], append_opts()) ::
              {:ok, read_result()} | {:error, :append_condition_failed}

  @doc """
  Returns how many events the store holds.

  Counted over the same scope a `:all` read covers — the config's isolation scope, not
  the whole storage.
  """
  @callback count(config()) :: non_neg_integer()

  @doc """
  Hands the reactor its next batch of unconsumed events and records how far it got.

  Where the batch starts is the reactor's stored checkpoint, or its
  `start_after_position` when it has none yet. The handler reports back how many events
  of the batch it processed, and the new checkpoint is the position of the last of
  those — a handler that fails part-way through leaves the rest to be delivered again.
  The batch size is the backend's own choice.
  """
  @callback consume(config(), StoredEventReactor.t()) :: ConsumeResult.t()

  @doc """
  Runs `fun` inside a store transaction and returns whatever it returned.

  Everything `fun` wrote — appended events, advanced checkpoints — is rolled back if it
  raises, and the raise propagates unchanged. An error *return* is not a rollback. A
  call made inside another transaction on the same store joins it instead of committing
  on its own.
  """
  @callback transaction(config(), (-> result)) :: result when result: var

  @doc """
  Returns the metadata this backend adds to the store's telemetry events.

  It is merged under the `:backend` key `Ariadne.Flow.Store` sets, so keys identifying
  the storage — the Postgres backend's `:prefix` and `:context` — belong here.
  """
  @callback telemetry_metadata(config()) :: map()

  @doc """
  Serialises the config so `c:load/1` can rebuild it.

  What comes back has to reach the same storage rather than a copy of it: events
  appended through the loaded store are visible to the original, and the other way
  round.

  How far the dumped form travels is the backend's own property, not something this
  contract fixes. `Ariadne.Flow.Store.Postgres` dumps a map of strings and survives
  whatever a job system stores it in; `Ariadne.Flow.Store.InMemory` dumps its agent and
  loads back only on the node that built it. An engine that hands a store to another
  node is the one that needs a backend whose dumped form survives the trip.
  """
  @callback dump(config()) :: term()

  @doc "Rebuilds a config from `c:dump/1`."
  @callback load(dumped :: term()) :: config()
end
