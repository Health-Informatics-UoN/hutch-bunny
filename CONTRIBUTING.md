# Contributing

Thanks for your interest in contributing to Bunny!

Please find general guidance to contributing to Hutch in our [documentation](https://health-informatics-uon.github.io/hutch/contributing).

This page covers everything specific to contributing code to Bunny.

## Ways to contribute

- **Bugs & feature requests** — open an [issue](https://github.com/Health-Informatics-UoN/hutch-bunny/issues). Please check existing issues first to avoid duplicates.
- **New to the project?** Look for issues labelled [`good first issue`](https://github.com/Health-Informatics-UoN/hutch-bunny/labels/good%20first%20issue) or [`help wanted`](https://github.com/Health-Informatics-UoN/hutch-bunny/labels/help%20wanted).
- **Bigger changes** (a new database backend, a new query type, anything architectural) — please open an issue to discuss the approach before you start. It saves you from spending time on something that turns out not to fit. Check the [roadmap](https://github.com/orgs/Health-Informatics-UoN/projects/1/views/15) for planned work first.
- **Security vulnerabilities** — do not open a public issue. Follow the process in [`SECURITY.md`](SECURITY.md) instead.

## Development setup

See the [developer setup guide](https://hutch.health/bunny/developers/setup).

## Pre-commit hooks

This repo uses [pre-commit](https://pre-commit.com/) to run Ruff (lint + format) before each commit. Install the hooks once per clone:

```bash
uv run pre-commit install
```

## Code style & type checking

- **Ruff** lints and formats the codebase (config in `pyproject.toml`); this is enforced by pre-commit and CI ([`check.quality.yml`](.github/workflows/check.quality.yml)).
- **mypy** runs in strict mode — all new code must be fully typed, with no implicit `Any`. This isn't currently wired into pre-commit or CI, so please run it yourself before opening a PR:

  ```bash
  uv run mypy src/
  ```

- Docstrings follow the Google style convention (enforced via Ruff's pydocstyle rules).

## Testing

Tests are split by marker, declared in `pyproject.toml`:

| Marker | Covers | Requires |
|---|---|---|
| `unit` | Pure logic, mocked dependencies | Nothing |
| `integration` | Real queries via the solvers/DB layer | A running OMOP database |
| `end_to_end` | Full CLI/daemon runs | A running OMOP database |

```bash
uv run pytest -s -m unit tests/          # fast, no external deps
uv run pytest -s -m integration tests/   # needs a DB, see below
uv run pytest -s tests/                  # everything
```

`integration` and `end_to_end` tests connect using the same `.env` / environment variables as the app itself (see `Settings` in `core/settings.py`). `docker compose -f dev.compose.yml up db omop-lite` will give you a local Postgres instance seeded with synthetic OMOP data to point them at; see [`check.run-tests.yml`](.github/workflows/check.run-tests.yml) for exactly how CI provisions one via [`omop-lite`](https://github.com/Health-Informatics-UoN/omop-lite).

CI runs the full suite against a matrix of Postgres (14–18) and SQL Server (2019, 2022). If you're changing anything in `core/db/` or `core/solvers/`, keep both dialects in mind — a change that works on Postgres can still fail on SQL Server (and vice versa).

New features and bug fixes should come with tests at the appropriate level: `unit` for logic (e.g. a new `Rule` operator or result modifier), `integration` if it touches real SQL (e.g. a new distribution type or DB backend).

## Pull requests

- Open your PR against `main`. Keep PRs focused — small, single-purpose PRs are easier to review and land faster than large ones.
- PR titles must follow [Conventional Commits](https://www.conventionalcommits.org/) (e.g. `feat: ...`, `fix: ...`, `docs: ...`) — this is enforced by CI (see [`check.pr-title.yaml`](.github/workflows/check.pr-title.yaml)) and drives the release process below. We squash-merge, so the PR title becomes the commit on `main` — individual commits within your branch don't need to follow the convention.
- Link the issue your PR addresses, where there is one.
- Before requesting review, check that `ruff check`, `ruff format --check`, `uv run mypy src/`, and `uv run pytest -s tests/` all pass locally — CI will run equivalent checks, but catching issues locally is faster for everyone.
- Draft PRs are welcome if you'd like early feedback on direction before the change is finished.

## Releases

Releases are automated with `semantic-release` based on Conventional Commit PR titles merged to `main`: a `fix:` triggers a patch release, `feat:` a minor release, and a breaking change (`!` or a `BREAKING CHANGE:` footer) a major release. This also determines the container image tags published to `ghcr.io/hutch/bunny`.

