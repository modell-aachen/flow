# Store

An event store is where all events live. `Ariadne.Flow.Application` uses a store for every `query/2` and `dispatch/3`. Flow ships a Postgres-backed store, and an in-memory one for tests.

## Postgres store

### Creating the tables

`Ariadne.Flow.Store.Postgres.Migration.run/1` defines the tables the Postgres store needs. Call it from a regular Ecto migration with the PostgreSQL schema prefix you want the tables to live in:

```elixir
defmodule MyApp.Repo.Migrations.AddFlowStore do
  use Ecto.Migration

  def change, do: Ariadne.Flow.Store.Postgres.Migration.run("public")
end
```

The tables are named `modac_flow_store`, `modac_flow_store_tags` and
`modac_flow_store_reactor_checkpoints` — they predate the rename to `Ariadne.Flow`
and keep their names because they are live production schema.

Running the migration more than once is safe. The default `change/0` direction handles both forward and rollback.

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

The transaction also joins an ambient one — its own `transaction/2`, or, with the Postgres store, any transaction on the same repo — rather than committing independently inside it.

`Application.dispatch/3` wraps every dispatch in `transaction/2`, so a command's append and the pass over the reactors that react to it commit as one unit, and a crash part-way through cannot leave the events appended with only some of the reactors advanced.

## Backends

`Ariadne.Flow.Store` stores nothing itself. A store is a backend module paired with the config that backend needs — `%Ariadne.Flow.Store{module: Ariadne.Flow.Store.Postgres, config: ...}` — and every call to `Ariadne.Flow.Store` is dispatched to the backend, with the parts that are the same for all of them, query normalisation and telemetry, done first. `Ariadne.Flow.Store.Backend` is the contract those modules implement:

| Callback | Does |
| --- | --- |
| `init/1` | builds the store — the one backend function callers name directly |
| `read/3` | returns the events matching a query, in position order |
| `append/3` | writes events atomically, honouring the append condition it was given |
| `count/1` | how many events the store holds, over the scope an `:all` read covers |
| `consume/2` | hands a reactor its next batch of events and records how far it got |
| `transaction/2` | runs a function, rolling its writes back if it raises |
| `telemetry_metadata/1` | the metadata that identifies this storage on store telemetry |
| `dump/1` and `load/1` | serialise the config, so a store can travel to a job system |

Flow ships two backends. `Ariadne.Flow.Store.Postgres` is the one to run on. `Ariadne.Flow.Store.InMemory` keeps its events in an Agent, for tests and for anything else that wants a store without a database behind it; it dumps to the agent itself, so a store built from it round-trips within a node but cannot be handed to another one.

A third backend is a matter of satisfying the contract, not just defining the callbacks — total order over positions, a conditional append no concurrent writer can slip past, isolation between configs, checkpoints that survive. `Ariadne.Flow.Store.Backend` spells out what each callback owes its caller, `Ariadne.Flow.Store.InMemory` is short enough to read as the reference implementation, and the store test suite is parameterised over the shipped backends, so every case in it holds for any backend.
