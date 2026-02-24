.PHONY: clean clean-test clean-pyc clean-build
.DEFAULT_GOAL := clean

clean: ## remove all build, test, coverage and Python artifacts
	@echo -----------------------------------------------------------------
	@echo CLEANING UP ...
	make clean-build clean-pyc clean-test
	@echo ALL CLEAN.
	@echo -----------------------------------------------------------------


clean-build: ## remove build artifacts
	@echo cleaning build artifacts ...
	rm -fr dist/

clean-pyc: ## remove Python file artifacts
	@echo cleaning pyc file artifacts ...
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	@echo cleaning test artifacts ...
	rm -f .coverage*
	rm -fr .pytest_cache
	rm -fr .ruff_cache
	rm -fr .mypy_cache

test:
	@echo -----------------------------------------------------------------
	@echo RUNNING TESTS...
	uv run pytest -q --cov=src/weather_etl --cov-report=term-missing
	@echo ✅ Tests have passed!
	@echo -----------------------------------------------------------------


dist: clean ## builds source and wheel package
	uv build
	ls -l dist

install: clean
	uv pip install .

install-dev-local:
	uv pip install . --group dev
	uv pip install pre-commit
