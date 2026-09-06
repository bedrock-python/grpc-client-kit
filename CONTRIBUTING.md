# Contributing to grpc-client-kit

Thank you for your interest in contributing! This document covers everything you need to get started.

## Development setup

```bash
git clone https://github.com/bedrock-python/grpc-client-kit.git
cd grpc-client-kit
uv sync --group dev
uv run pre-commit install --hook-type commit-msg
```

## Running checks

```bash
make check            # ruff lint + format check + mypy
make test-unit        # unit tests, no Docker required
make test-integration # integration tests, requires Docker
make test             # full suite with 90% coverage threshold
```

## Code style

- **Type hints** on all functions and methods, including tests
- **Docstrings** on public API only — Google style
- **Line length** — 120 characters (ruff enforced)
- **Quotes** — double quotes (ruff enforced)
- **No comments** unless the *why* is non-obvious

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/) are enforced by pre-commit:

| Prefix | Use for |
|--------|---------|
| `feat:` | New feature or behaviour |
| `fix:` | Bug fix |
| `docs:` | Documentation only |
| `test:` | Test additions or changes |
| `refactor:` | Code restructure, no behaviour change |
| `perf:` | Performance improvement |
| `chore:` | Build, tooling, CI |

Breaking changes: add `!` after the type (`feat!:`) or include a `BREAKING CHANGE:` footer.

## Pull requests

1. Fork the repository
2. Create a branch from `master`: `git checkout -b feat/my-feature`
3. Make your changes with tests
4. Run `make check && make test-unit` locally
5. Open a PR against `master`

## The agents page

`docs/agents.md` is the whole library on one page, written for a coding assistant: the
public API, the rules that break code when they are broken, the mistakes models make, and a
map of which page to fetch for the rest. People hand it to an assistant instead of the site,
which is what makes a stale one worse than none — it teaches a model an API that no longer
exists.

It is part of the public API, so it changes in the same pull request the API does: a name
added, renamed or removed, a changed default or signature, a new rule a caller has to obey.
A new docs page means a new row in the documentation map. The review check is mechanical —
if the diff changes the public surface and `docs/agents.md` is untouched, the pull request
is not finished.

## Releasing (maintainers only)

Releases are fully automated via [Release Please](https://github.com/googleapis/release-please).
Merge a PR with conventional commits → Release Please creates a release PR → merge it → PyPI publish happens automatically.
