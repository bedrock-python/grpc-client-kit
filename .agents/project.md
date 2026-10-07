# Project profile

## About

- What this repository is: `grpc-client-kit`, an async gRPC client toolkit for Python (channel pool, client-side load balancing, health checking, streaming-aware interceptors, observability), published on PyPI.
- Who uses it: asyncio services that call other services over gRPC, on the caller side.
- Language, version and main frameworks: Python 3.12 and newer, on `grpc.aio`. The only runtime dependency is `grpcio`. Optional extras: `health` (`grpcio-health-checking`), `tracing` (OpenTelemetry API), `metrics` (`prometheus-client`), `deadline` (`deadline-budget`), `settings` (Pydantic) and `dishka` (Dishka providers), plus `observability` (`metrics` + `tracing`) and `all`.
- Where the documentation lives: https://bedrock-python.github.io/grpc-client-kit/, built with Zensical from `docs/` (`zensical.toml`). mkdocstrings generates the API reference from the docstrings. `docs/agents.md` is the one-page reference for coding assistants. Runnable examples are in `examples/`.

## Commands

| Task | Command |
|---|---|
| Install dependencies | `uv sync --group dev --all-extras`, as CI does. `make install` and CONTRIBUTING.md run `uv sync --group dev` without `--all-extras`, and then collection fails in both `tests/unit/` and `tests/integration/`, because they import `grpc_health`. |
| Format | `make fmt`: `uv run ruff format .`, then `uv run ruff check --fix .` |
| Lint and type-check | `make check`: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy grpc_client_kit` |
| Test | `make test-unit` (`uv run pytest -m unit`) and `make test-integration` (`uv run pytest -m integration`). `make test` runs both with `--cov-fail-under=90`. CI runs `uv run pytest -m unit --cov=grpc_client_kit --cov-report=xml --cov-fail-under=90` on Python 3.12 and 3.13, and `uv run pytest -m integration` in a job of its own. |
| Run locally | A library: nothing to run. An example: `uv run python examples/<name>.py`. Each one starts its own servers on ephemeral ports and exits on its own. The docs: `make docs-serve`. |
| Stop the local run | Ctrl+C in the terminal of `make docs-serve`. The examples exit on their own. |

## Conventions

- Default branch: `master`. The ruleset `master-rules` accepts changes only through a pull request whose `All checks passed` check is green. Locally, the pre-commit hook `no-commit-to-branch` refuses commits to `master`.
- Issue tracker: GitHub Issues of this repository. Refer to an issue as `#123`, and close it with `Closes #123` in the pull request.
- Branch names: `<type>/<short-description>`, such as `feat/my-feature` (CONTRIBUTING.md).
- Commit messages: Conventional Commits, enforced by the `conventional-pre-commit` hook (`uv run pre-commit install --hook-type pre-commit --hook-type commit-msg` installs every hook; CONTRIBUTING.md installs only the commit-message one, so ruff does not run at commit time). Pull requests are squash-merged, and release-please reads the squash title, so a pull request's title is a Conventional Commit too.
- Language of pull request titles and descriptions: English.
- Where specs and design notes go: in the pull request description. The repository has no place for them.

## Boundaries

- release-please generates `CHANGELOG.md`. Never edit it by hand, and never add an `## [Unreleased]` section, even though the pull request template's checklist asks for one. `docs/changelog.md` is a copy the docs build makes, and git ignores it.
- The version lives in `grpc_client_kit/__version__.py`, on the line marked `# x-release-please-version`, and in `.release-please-manifest.json`. release-please bumps both. Never change them by hand.
- `docs/agents.md` is part of the public API: it changes in the same pull request as the API, and a new docs page adds a row to its documentation map (CONTRIBUTING.md, "The agents page").
- Files from the engineering-assets hub (`AGENTS.md` explains how it works) include the copy-page files (`docs/assets/javascripts/copy-page.js`, `docs/assets/stylesheets/copy-page.css`, `overrides/main.html`, `scripts/emit_markdown.py`), `.github/workflows/docs.yml`, `.github/workflows/release-please.yml`, `.github/dependabot.yml`, `.editorconfig` and `CODE_OF_CONDUCT.md`. Change them in the hub; a change here makes the file this repository's own.
- Never publish to PyPI or create a release by hand: release-please creates the release, and `publish.yml` publishes it. Its manual `workflow_dispatch` is for maintainers.

## Notes

- Releases: release-please opens its pull request with the workflow's own token, so CI does not start on it. Close and reopen the release pull request to run CI, then merge it. `publish.yml` runs after every completed Release Please run on `master`, and it publishes to PyPI when a release tag such as `grpc-client-kit-v0.4.0` points at the commit. Only `feat`, `fix`, `perf` and `revert` appear in the changelog (`release-please-config.json`).
- Tests: every test carries the marker `unit` or `integration`, which `tests/conftest.py` sets from the test's directory (`--strict-markers`). The integration tests start real `grpc.aio` servers in-process on ephemeral ports and need no Docker, although CONTRIBUTING.md says they do. With `asyncio_mode = "auto"`, async tests need no decorator.
- Extras: `import grpc_client_kit` must work on a bare install. Names that need an extra resolve on first access (`__getattr__` in `grpc_client_kit/__init__.py`) and raise an `ImportError` that names the extra. `tests/unit/conftest.py` checks this in a subprocess.
- Line endings: `.editorconfig` asks for LF, but the repository has no `.gitattributes`, and some source files are committed with CRLF (`git ls-files --eol`), among them `grpc_client_kit/balancers.py` and `grpc_client_kit/utils.py`. Keep a file's line endings when you edit it.
- Code style (CONTRIBUTING.md): type hints on every function, tests included; Google-style docstrings on the public API only; lines up to 120 characters; double quotes; comments only for a non-obvious why.
