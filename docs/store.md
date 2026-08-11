# Store

An event store is where all events live. `Ariadne.Flow.Application` uses a store for every `query/2` and `dispatch/3`. Flow ships a Postgres-backed store, and an in-memory one for tests.

## Postgres store

### Creating the tables

`Ariadne.Flow.Store.Postgres.Migration` defines the tables the Postgres store needs. Call it from a regular Ecto migration with the PostgreSQL schema prefix you want the tables to live in:

```elixir
defmodule MyApp.Repo.Migrations.AddFlowStore do
  use Ecto.Migration

  def up, do: Ariadne.Flow.Store.Postgres.Migration.up(prefix: "public")

  def down, do: Ariadne.Flow.Store.Postgres.Migration.down(prefix: "public")
end
```

`up/1` and `down/1` take a keyword list:

- `:prefix` (default `"public"`) — the PostgreSQL schema the tables live in. Must match the prefix passed to `init/1`.
- `:version` — the schema version to migrate to. `up/1` defaults to the newest version the library knows, `down/1` to the oldest, so the pair above installs the whole store and removes it again.

The store reads and writes three tables: `ariadne_flow_store`,
`ariadne_flow_store_tags` and `ariadne_flow_store_reactor_checkpoints`. Run the
migration before booting the release that reads them: a pod that starts ahead of
it fails every read, append and consume with `relation "ariadne_flow_store" does
not exist`, and migrate-on-boot races the same way.

The advisory-lock domain strings the store hashes its `pg_advisory_xact_lock`
keys from are deliberately *not* named after those tables and never will be.
They are invisible from the outside, and renaming them would let two library
versions running against the same database stop excluding each other's appends.

### Upgrading the schema

The store schema is versioned, and a release that changes it says so in the CHANGELOG. Upgrading is a new migration that calls `up/1` again: it applies every version between the one the store is on and the one you ask for, and does nothing at all when there is none to apply. Pin `version:` to stay on an older schema than the library offers, and roll a version back with `down(version: n)`, which undoes everything from the installed version down to `n` inclusive.

The installed version is recorded in the store itself, as a comment on the event table, and `Ariadne.Flow.Store.Postgres.Migration.migrated_version/1` reads it back — inside a migration, like the rest of the API. A store created before the versioning existed reports version `0`; the first `up/1` after upgrading adopts it as version 1 rather than recreating anything and then applies the remaining versions, so there is nothing to do by hand — but an unpinned `up/1` on such a store lands on the newest version in one migration rather than stopping at the next one. Pin `version:` if a later step has to be its own change window.

Running a migration more than once is safe in either direction. `change/0` is not: the versioning has to know which way it is going, so a store migration needs the explicit `up/0` and `down/0` pair. Neither is `@disable_ddl_transaction true`: version 3 renames three tables and expects the swap to be atomic, so a migration that calls `up/1` has to keep its DDL transaction.

One upgrade path is not free to skip a release: **deploy the release that installs schema version 2 everywhere before deploying the one that installs version 3.** Version 2 puts `ariadne_*` views in front of the `modac_*` tables the store started out with, so pods on either release address the same rows; version 3 renames the tables to `ariadne_*` and drops the views. Pods already on version 2 keep working across the swap — their names resolve to the renamed tables the moment it commits, and the `ALTER TABLE ... RENAME` statements cost them a moment of latency rather than an error. Pods older than that lose the tables mid-flight, so going from a pre-version-2 release straight to a version-3 one breaks every one of them that is still running.

### Initializing a store

Once the tables exist, build a store by calling `init/1`:

```elixir
iex> store = Ariadne.Flow.Store.Postgres.init(repo: MyApp.Repo)

iex> Ariadne.Flow.Application.query(store, course_capacity(42))
30
```

`init/1` accepts a keyword list:

- `:repo` (required) — the Ecto.Repo module used to run queries.
- `:prefix` (default `"public"`) — the PostgreSQL schema the tables live in. Must match the prefix passed to the migration.
- `:context` (default `"default"`) — an isolation key. Events appended through a store with `context: "courses"` are invisible to one with `context: "students"`, even when they share the same schema.

Create as many stores as you need — one per schema and context combination — and pass the right one to `Application.query/2` and `Application.dispatch/3`.

## Transactions

`Ariadne.Flow.Store.transaction/2` runs a function inside a store transaction and returns whatever the function returned:

```elixir
iex> Ariadne.Flow.Store.transaction(store, fn -> Ariadne.Flow.Application.dispatch(application, command) end)
{:ok, %{events: [...]}}
```

Everything the function wrote to the store — appended events, advanced reactor checkpoints — is rolled back if it raises. The raise propagates unchanged.

An error *return* is not a rollback. `{:error, reason}` comes back as it is, with the writes that led to it kept, the same as before there was a boundary at all: a command that refuses has written nothing anyway, and a reactor that returns an error leaves the events it failed on in the store, to be reacted to again later. Deciding that an error should undo the writes is the caller's business, and a caller who wants that can raise.

The transaction also joins an ambient one — its own `transaction/2`, or, with the Postgres store, any transaction on the same repo — rather than committing independently inside it. `Ariadne.Flow.Store.in_transaction?/1` answers whether there is one to join, which is how a caller finds out whether a write it makes now would still be invisible to everybody else. `Application.dispatch/3` asks it to decide whether a synchronous reactor's run can be [scheduled and waited for](application.html#nesting).

`Application.dispatch/3` wraps every dispatch in `transaction/2`, and what goes inside it is exactly what has to commit with the events: the append itself, the reactor checkpoints the append initialises, and whatever the [scheduler](application.html#the-scheduler) durably schedules. Reactors run *after* that transaction commits, so no reactor can undo a dispatch and none of them holds the append lock while it works.

## Backends

`Ariadne.Flow.Store` stores nothing itself. A store is a backend module paired with the config that backend needs — `%Ariadne.Flow.Store{module: Ariadne.Flow.Store.Postgres, config: ...}` — and every call to `Ariadne.Flow.Store` is dispatched to the backend, with the parts that are the same for all of them, query normalisation and telemetry, done first. `Ariadne.Flow.Store.Backend` is the contract those modules implement:

| Callback | Does |
| --- | --- |
| `init/1` | builds the store — the one backend function callers name directly |
| `read/3` | returns the events matching a query, in position order |
| `append/3` | writes events atomically, honouring the append condition it was given |
| `count/1` | how many events the store holds, over the scope an `:all` read covers |
| `consume/2` | hands a reactor its next batch of events and records how far it got |
| `checkpoint/2` | the position a reactor has consumed up to, making its progress observable |
| `init_checkpoints/2` | creates the checkpoints of the reactors that have none, leaving the rest alone |
| `transaction/2` | runs a function, rolling its writes back if it raises |
| `in_transaction?/1` | whether the calling process is already inside a transaction on this store |
| `telemetry_metadata/1` | the metadata that identifies this storage on store telemetry |
| `dump/1` and `load/1` | serialise the config, so a store can travel to a job system |

Flow ships two backends. `Ariadne.Flow.Store.Postgres` is the one to run on. `Ariadne.Flow.Store.InMemory` keeps its events in an Agent, for tests and for anything else that wants a store without a database behind it; it dumps to the agent itself, so a store built from it round-trips within a node but cannot be handed to another one.

`init_checkpoints/2` is where a reactor's [`start_after_position`](application.html#reactors) stops being a declaration and becomes a position in the store. A dispatch calls it with its events appended but not yet committed, so *from now* means the events of that dispatch and nothing later. Two things follow for a backend implementing it: it must never move a checkpoint that already exists, and creating the missing ones has to be atomic against concurrent appends — which the Postgres backend gets for free, since the append lock it took is held until the transaction commits. `consume/2` correspondingly knows nothing about declarations: it resumes from the checkpoint, or from the origin for a reactor nobody ever declared to this store.

`consume/2` runs the reactor's handler in the calling process, inside a transaction on the store, holding the reactor's name against every other consume of it. All three are things a reactor is entitled to rely on: a handler may read from, append to and [dispatch into](application.html#reactors) the very store it is consuming from, another consume of the same reactor waits rather than handing out the same events twice, and a handler that raises takes the checkpoint and everything it wrote down with it. The Postgres backend gets the serialisation from a per-reactor advisory lock held for the length of its consume transaction; the in-memory one keeps an equivalent lock in the agent's state, since its handler runs outside the agent.

A third backend is a matter of satisfying the contract, not just defining the callbacks — total order over positions, a conditional append no concurrent writer can slip past, isolation between configs, checkpoints that survive. `Ariadne.Flow.Store.Backend` spells out what each callback owes its caller, `Ariadne.Flow.Store.InMemory` is short enough to read as the reference implementation, and the store test suite is parameterised over the shipped backends, so every case in it holds for any backend.
