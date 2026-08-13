.PHONY: test test-unit test-integration fmt check build install docs-serve docs-build clean

install:
	uv sync --group dev

fmt:
	uv run ruff format .
	uv run ruff check --fix .

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy grpc_client_kit

test-unit:
	uv run pytest -m unit

test-integration:
	uv run pytest -m integration

test:
	uv run pytest --cov=grpc_client_kit --cov-report=term --cov-fail-under=90 --cov-report=xml:coverage.xml

build:
	uv build

docs-serve:
	python -c "import shutil; shutil.copy('CHANGELOG.md', 'docs/changelog.md')"
	uv run --no-dev --group docs zensical serve

docs-build:
	python -c "import shutil; shutil.copy('CHANGELOG.md', 'docs/changelog.md')"
	uv run --no-dev --group docs zensical build --clean

clean:
	python -c "import shutil, os, pathlib; [shutil.rmtree(p, ignore_errors=True) for p in ['.pytest_cache', '.mypy_cache', '.ruff_cache', 'dist', 'build', 'site'] if os.path.exists(p)]; [os.remove(p) for p in ['.coverage', 'coverage.xml'] if os.path.exists(p)]; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('.').rglob('__pycache__')]"
