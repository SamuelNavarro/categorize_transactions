sources = categorize_transactions

.PHONY: test format lint pytest coverage pre-commit clean
test: format lint pytest

format:
	isort $(sources) tests
	black $(sources) tests

lint:
	flake8 $(sources) tests
	mypy $(sources) tests

pytest:
	PYTHONPATH=./categorize_transactions/ pytest

coverage:
	PYTHONPATH=./categorize_transactions/ pytest --cov=$(sources) --cov-branch --cov-report=term-missing tests

pre-commit:
	pre-commit run --all-files

clean:
	rm -rf .mypy_cache .pytest_cache
	rm -rf *.egg-info
	rm -rf .tox dist site
	rm -rf coverage.xml .coverage
