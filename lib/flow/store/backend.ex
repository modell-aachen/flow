defmodule Ariadne.Flow.Store.Backend do
  @moduledoc false
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

  @typedoc """
  Where a reactor that has no checkpoint yet is to resume from: the name the checkpoint is
  keyed on, and the position the reactor starts after.
  """
  @type checkpoint_init :: %{name: String.t(), position: non_neg_integer()}

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

  Where the batch starts is the reactor's stored checkpoint, or the store's origin when
  it has none — a reactor is given a checkpoint by `c:init_checkpoints/2` before anything
  runs it, so the origin is the resume point of a reactor nobody ever declared. The
  handler reports back how many events of the batch it processed, and the new checkpoint
  is the position of the last of those — a handler that fails part-way through leaves the
  rest to be delivered again. The batch size is the backend's own choice.
  """
  @callback consume(config(), StoredEventReactor.t()) :: ConsumeResult.t()

  @doc """
  Creates the checkpoints of the reactors that have none, leaving every existing one where
  it stands.

  This is what decides where a reactor starts, and it is called with the events of the
  append it belongs to still uncommitted, so a reactor starting *from now* is pinned to
  the first events it is meant to see. The write therefore has to be atomic against
  concurrent appends: a backend that serialises appends on a lock held for the whole
  transaction already is, and one that does not has to make "insert the ones that are
  missing" a single atomic step.

  Moving an existing checkpoint is never right here — it is where a reactor stands, and
  the declaration only ever says where a reactor that has never run begins.
  """
  @callback init_checkpoints(config(), [checkpoint_init()]) :: :ok

  @doc """
  Returns the position the reactor's stored checkpoint stands at, `nil` when it has none.

  It is what makes a reactor's progress observable without consuming: a caller that needs
  to know whether a reactor has caught up to an appended event compares this to its
  position. `nil` says the reactor has never been declared to this store, which is the
  only thing that separates it from one parked at the origin.
  """
  @callback checkpoint(config(), name :: String.t()) :: non_neg_integer() | nil

  @doc """
  Runs `fun` inside a store transaction and returns whatever it returned.

  Everything `fun` wrote — appended events, advanced checkpoints — is rolled back if it
  raises, and the raise propagates unchanged. An error *return* is not a rollback. A
  call made inside another transaction on the same store joins it instead of committing
  on its own.
  """
  @callback transaction(config(), (-> result)) :: result when result: var

  @doc """
  Whether the calling process is already inside a transaction on this store.

  True inside `c:transaction/2` — including a transaction the caller never opened
  through this store, any transaction on the Postgres backend's repo — and false
  outside one. It answers whether a write made now would still be invisible to other
  processes, which is what a caller asking another process to observe its writes needs
  to know before it waits on them.
  """
  @callback in_transaction?(config()) :: boolean()

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
