# Contributing

Flow is open source because we think the design is worth reading, forking and
depending on. It is not open contribution.

We do not take pull requests from outside the Ariadne team — the repository is
configured so that only the team can open one. Every line here is owned
by one legal entity, which is what lets us relicense, ship commercial builds and
answer provenance questions from customers with a single sentence. Keeping it
that way while accepting outside patches would mean running a CLA process, and
we currently don't want to run one — it is a question of the overhead we can
carry today, not of principle. If that changes, this file changes with it.

That leaves plenty you can do, and we want you to:

- **Open an issue.** Bug reports, reproductions, feature requests and design
  questions are welcome and get read. A reproduction tells us more than anything
  else you could send.
- **Fork it.** Apache-2.0 means your fork is yours, for any purpose, with no
  obligation to come back here.
- **Ask before you build on an internal.** Only what [docs/](docs/introduction.md)
  describes is public surface. Anything else may move without a major bump.

## For the Ariadne team

Read [CLAUDE.md](CLAUDE.md) for the layout conventions and the guide set, and
the Local Development section of [README.md](README.md) for the devbox setup.

Commit subjects must follow [Conventional Commits](https://www.conventionalcommits.org)
— `.githooks/commit-msg` enforces it, and release-please reads the history to cut
releases. Work lands on `main`, so that hook is the only gate.

Run `mix ci` before pushing. It is what CI runs, and a compiler warning fails it.
