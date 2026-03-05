.PHONY: test lint format install dev clean

install:
	pip install -e .

dev:
	pip install -e ".[dev]"
	pip install -r requirements-dev.txt

test:
	pytest tests/ -v --tb=short

test-cov:
	pytest tests/ -v --cov=dbt_parser --cov-report=html

lint:
	flake8 dbt_parser/ tests/
	mypy dbt_parser/

format:
	black dbt_parser/ tests/
	isort dbt_parser/ tests/

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache .mypy_cache htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +
