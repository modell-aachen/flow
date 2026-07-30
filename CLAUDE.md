# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Read the guides first

`docs/` is the ExDoc guide set and covers the whole public surface — read it before changing anything, and keep it in sync when the API changes:

| Guide | Covers |
| --- | --- |
| [docs/introduction.md](docs/introduction.md) | Event sourcing with Dynamic Consistency Boundaries — the premise the design rests on |
| [docs/events.md](docs/events.md) | Event anatomy, tags, and **the rules for evolving an event** (module name, payload and tags are immutable history) |
| [docs/event_reducer.md](docs/event_reducer.md) | `Projection`, `Composite`, the `EventReducer` behaviour, filter semantics |
| [docs/application.md](docs/application.md) | `query/2`, `dispatch/3`, metadata, reactors, the engine contract, concurrency |
| [docs/store.md](docs/store.md) | Migration, `init/1`, `prefix`/`context`, transaction semantics |
| [docs/encoder.md](docs/encoder.md) | Default and custom encoders — the escape hatch for adding a field to an existing event |
| [docs/testing.md](docs/testing.md) | The `Ariadne.Flow.Test.Gwt` given/when/then DSL for reducer tests |

[README.md](README.md) covers local development and the no-copyleft dependency rule.

`lib/flow/examples/` (course, unique username, page responsible, dynamic product price) is the canonical usage reference and is excluded from the generated docs. Where a guide and the code disagree, the examples are authoritative — e.g. `docs/event_reducer.md` shows `Projection.new/1` with `handler:` in the attrs map, but the real API is `Projection.new(attrs, handler)`.

## Commands

All tooling runs inside devbox (direnv activates it on `cd`; `MIX_ENV=test` is the devbox default). `./run` boots the Postgres container and then runs any mix task:

```bash
./run test                                # full suite
./run test lib/flow/store_test.exs        # one file
./run test lib/flow/store_test.exs:35     # one test
./run test.interactive                    # watch mode
./run check                               # credo --strict, format --check-formatted, sobelow
./run ci                                  # test --max-cases=8 + check (what CI runs)
devbox run docs                           # ExDoc HTML into doc/
devbox run speedrun                       # throughput harness against Postgres
devbox run consistency                    # concurrent append/consistency checker
devbox run test.stop                      # docker compose down
```

The `test` alias recreates and re-migrates the test repo first, and appends `--warnings-as-errors` — a compiler warning fails the suite.

## Layout conventions

- **Tests live beside their source in `lib/`**, not in `test/`: `test_paths: ["./lib"]`, `test_pattern: "*_test.ex*"` (both `_test.exs` and `_test.ex` are collected), helper at `lib/test_helper.exs`.
- Directory `lib/flow/` maps to the `Ariadne.Flow` namespace — `lib/flow/store/postgres.ex` is `Ariadne.Flow.Store.Postgres`. The OTP app is `:flow`.
- Credo runs `--strict` with many opt-in checks: `SinglePipe`, `PipeChainStart`, `BlockPipe`, `StrictModuleLayout`, one alias per line, `AliasUsage` above 3 levels of nesting, no `IO.puts`/`IO.inspect` (the `.exs` scripts opt out per file). `Readability.ModuleDoc` is off, so internal modules carrying `@moduledoc false` are deliberate.

## Internals the guides don't cover

**`EventReducer.evaluate/2` is the single seam between pure reducers and the store.** It asks the reducer for its query, reads, deserialises, reduces, and returns `{result, append_condition}` — where that condition is the reducer's own query plus the last position it saw. One declaration therefore decides both what is read and what conflicts on write; everything docs/application.md says about concurrency falls out of this function.

**Store dispatch is hand-rolled, not a protocol or behaviour.** `%Store{module:, config:}` and `Store` calls `module.read/append/consume/transaction/telemetry_metadata/dump/load`. Two backends: `Store.Postgres` and `Store.InMemory` (an Agent for tests; its `dump`/`load` are node-local and don't cross nodes). `Store.read/append` wrap the backend in `:telemetry.span([:ariadne, :flow, :store, :read | :append])`, which is what `Store.SlowOperationLogger` attaches to.

**Reactors are configured as modules exporting `reactor/0`** because `ReactorRun` — reactor module, resume position, dispatch metadata — has to survive `dump/1` → job system → `load/1`. That serialisation boundary is why the module name, not the struct, is the identity.

**`Query.new/1` always optimises** (`query/optimizer.ex`): it drops tag-supersets, collapses types sharing a tag constraint, and lets an unrestricted item absorb tag-restricted ones for the same type. Read `query_test.ex` for the expected shape.

**Postgres store** (`store/postgres/query.ex`): `position` is a global `bigserial` giving total order. Tags live in a side table, so AND-semantics come from a join plus `group_by(position)` / `having(count(tag) == length(tags))`; multi-item queries become unioned subqueries ordered by position. Appends serialise on a per-`(prefix, context)` `pg_advisory_xact_lock` and reactor consumption on a per-`(prefix, context, name)` one, with keys derived by `advisory_lock_key/1` from length-prefixed parts so distinct part lists cannot collide.

**Store-backed tests** (docs/testing.md only covers pure reducers): `Sandbox.checkout(Ariadne.Flow.Test.Repo)` plus a store on `prefix: "postgres_store_test_schema"`. `store_test.exs` runs one case set against both backends via ExUnit `parameterize`.
