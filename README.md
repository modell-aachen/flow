# Flow

An event-sourcing library for Elixir: an append-only store, event reducers that
fold events into values, and an application that dispatches commands and runs
reactors.

The modules live under the `Ariadne.Flow` namespace. The Postgres store keeps its
`modac_flow_store*` table names — they are live production schema and renaming
them is a separate data migration.

The guides in [docs/](docs/introduction.md) cover events, event reducers, the
application, the store, encoders and testing.

## Local Development

`devbox run setup` installs the mix tooling and starts Postgres. Run it after
cloning and whenever Postgres is not up — it is idempotent. Everything else
runs as a plain mix task inside the devbox shell, which direnv activates on
`cd`:

```bash
devbox run setup # Install the mix tooling and start Postgres
mix test.interactive # Run tests interactively in watch mode
mix ci # Run all tests and checks which are also performed in CI
devbox run docs # Build the HTML docs into doc/
devbox run test.stop # Stop the Postgres service
```

No Docker is involved: the cluster is created by the setup script and lives in
`.devbox/virtenv/postgresql/data`.

The test database listens on port 5544, so it does not collide with other
local Postgres instances.

## Dependency Selection

Especially in Elixir do not use/introduce copyleft licenses, e.g. GPL, AGPL, etc.

You can print the license of a dependency with:

```bash
sudo apt install xq \
&& mix archive.install --force hex sbom \
&& mix deps.get \
&& mix sbom.cyclonedx --force && cat bom.xml | yq -p xml '.bom.components.component[] | "\(.name) \(.licenses.license.id)"'
```
