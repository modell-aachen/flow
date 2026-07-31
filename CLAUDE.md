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

All tooling runs inside devbox (direnv activates it on `cd`; `MIX_ENV=test` and `LOG_LEVEL=warning` are devbox defaults). The init hook stays cheap — it only exports `MIX_HOME`, `HEX_HOME` and `ERL_AFLAGS` and points `core.hooksPath` at `.githooks/` — and `devbox run setup` does the heavy work: hex, rebar, `mix deps.get`, `initdb` into `.devbox/virtenv/postgresql/data` (superuser `postgres`, trust auth, `max_connections=200`) and starting the `postgresql` plugin service on port 5544, waiting for `pg_isready`. It is idempotent, so re-run it whenever Postgres is not up; there is no Docker, and CI runs it as its own step before `devbox run ci`. Outside the direnv shell, prefix mix with `devbox run --`.

```bash
devbox run setup                          # mix tooling + Postgres, run this first
mix test                                  # full suite
mix test lib/flow/store_test.exs          # one file
mix test lib/flow/store_test.exs:35       # one test
mix test.interactive                      # watch mode
mix check                                 # credo --strict, format --check-formatted, sobelow
mix ci                                    # test --max-cases=8 + check (what CI runs)
devbox run docs                           # ExDoc HTML into doc/
devbox run speedrun                       # throughput harness against Postgres
devbox run consistency                    # concurrent append/consistency checker
devbox run test.stop                      # stop the Postgres service
```

The `test` alias recreates and re-migrates the test repo first, and appends `--warnings-as-errors` — a compiler warning fails the suite.

## Releases

Commit subjects must be Conventional Commits — work lands directly on `main`, so `.githooks/commit-msg` is the only gate before release-please reads the history. The devbox `init_hook` sets `core.hooksPath`, because git ignores hooks a clone ships with; it is deliberately not in `devbox run setup`, so the hook is live on shell entry without a setup run. `release-please.yml` keeps a release PR open on every push to `main`; merging it bumps `version:` in `mix.exs`, writes `CHANGELOG.md`, tags `vX.Y.Z` and cuts a GitHub release. Never edit `version:` or `.release-please-manifest.json` by hand. Pre-1.0 bumping is set by `bump-minor-pre-major` and `bump-patch-for-minor-pre-major` in `release-please-config.json`: `feat`/`fix` → patch, `!` → minor. The release PR is opened with the default `GITHUB_TOKEN`, so CI does not run on it.

## Layout conventions

- **Tests live beside their source in `lib/`**, not in `test/`: `test_paths: ["./lib"]`, `test_pattern: "*_test.ex*"`, helper at `lib/test_helper.exs`. Name every test file `_test.exs` — a `.ex` one is compiled into the app but never collected by `mix test`, so its cases silently never run.
- Directory `lib/flow/` maps to the `Ariadne.Flow` namespace — `lib/flow/store/postgres.ex` is `Ariadne.Flow.Store.Postgres`. The OTP app is `:flow`.
- Credo runs `--strict` with many opt-in checks: `SinglePipe`, `PipeChainStart`, `BlockPipe`, `StrictModuleLayout`, one alias per line, `AliasUsage` above 3 levels of nesting, no `IO.puts`/`IO.inspect` (the `.exs` scripts opt out per file). `Readability.ModuleDoc` is off, so internal modules carrying `@moduledoc false` are deliberate.

## Internals the guides don't cover

**`EventReducer.evaluate/2` is the single seam between pure reducers and the store, and it knows nothing about appending.** It asks the reducer for its query, reads, deserialises, reduces, and returns `%{result:, query:, events:}` — the reduced value plus the read it did to get there. `Application.query/2` takes `.result` and stops; the write path goes further, and `CommandHandler` turns `query` and `events` into an append condition with `AppendCondition.for_read/2` — the reducer's own query plus the last position it saw. One declaration therefore decides both what is read and what conflicts on write; everything docs/application.md says about concurrency falls out of `CommandHandler.handle/3`. `Store.read/3` builds no condition at all.

**Store dispatch is a behaviour, not a protocol.** `%Store{module:, config:}` and `Store` calls `module.init/read/append/consume/transaction/telemetry_metadata/dump/load`, the callbacks of `Store.Backend` — which is also where the contract behind them (total order, atomic conditional append, isolation per config, durable checkpoints) is written down. Two backends: `Store.Postgres` and `Store.InMemory` (an Agent for tests; its `dump`/`load` are node-local and don't cross nodes). `Store` normalises the query and the append condition before dispatching, so a backend only ever sees `Query.new/1` output and an `AppendCondition` struct. `Store.read/append` wrap the backend in `:telemetry.span([:ariadne, :flow, :store, :read | :append])`, which is what `Store.SlowOperationLogger` attaches to. `store_test.exs` is the conformance suite for the behaviour — it is parameterised over both backends.

**Reactors are configured as modules exporting `reactor/0`** because `ReactorRun` — reactor module, resume position, dispatch metadata — has to survive `dump/1` → job system → `load/1`. That serialisation boundary is why the module name, not the struct, is the identity.

**`Query.new/1` always optimises** (`query/optimizer.ex`): it drops tag-supersets, collapses types sharing a tag constraint, and lets an unrestricted item absorb tag-restricted ones for the same type. Items with `only_last_event: true` are exempt from all of it — every rewrite assumes a broader item covers a narrower one's events, which stops holding once only the last of them is read — so they are deduplicated and otherwise passed through. Read `query/optimizer_test.exs` for the expected shape, one case per rule.

**Postgres store** (`store/postgres/query.ex`): `position` is a global `bigserial` giving total order. Tags live in a side table, so AND-semantics come from a join plus `group_by(position)` / `having(count(tag) == length(tags))`; multi-item queries become unioned subqueries ordered by position. Appends serialise on a per-`(prefix, context)` `pg_advisory_xact_lock` and reactor consumption on a per-`(prefix, context, name)` one, with keys derived by `advisory_lock_key/1` from length-prefixed parts so distinct part lists cannot collide.

**Store-backed tests** (docs/testing.md only covers pure reducers): `Sandbox.checkout(Ariadne.Flow.Test.Repo)` plus a store on `prefix: "postgres_store_test_schema"`. `store_test.exs` runs one case set against both backends via ExUnit `parameterize`.
