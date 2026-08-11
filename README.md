# Flow

An event-sourcing library for Elixir: an append-only store, event reducers that
fold events into values, and an application that dispatches commands and runs
reactors.

The modules live under the `Ariadne.Flow` namespace. The Postgres store reads and
writes `ariadne_flow_store*`; the physical tables are still `modac_flow_store*`
with the new names as views over them, so a rolling deployment can run old and
new code against the same rows. [docs/store.md](docs/store.md) has the upgrade
rule that follows from it.

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

## Releases

release-please owns the version and the changelog. The `commit-msg` hook in
`.githooks/` rejects subjects that are not
[Conventional Commits](https://www.conventionalcommits.org):

```
feat(store): read events by tag
fix: derive advisory lock keys from SHA-256
feat!: drop the Modac.Flow wire-format shim
```

Every push to `main` refreshes a release pull request that bumps `version:` in
`mix.exs` and writes `CHANGELOG.md`. Merging it tags `vX.Y.Z` and publishes a
GitHub release. While the library is pre-1.0, `feat`/`fix` bump the patch and
`!` bumps the minor, so a breaking change goes to 0.2.0 rather than 1.0.0.

## Dependency Selection

Especially in Elixir do not use/introduce copyleft licenses, e.g. GPL, AGPL, etc.

You can print the license of a dependency with:

```bash
sudo apt install xq \
&& mix archive.install --force hex sbom \
&& mix deps.get \
&& mix sbom.cyclonedx --force && cat bom.xml | yq -p xml '.bom.components.component[] | "\(.name) \(.licenses.license.id)"'
```
