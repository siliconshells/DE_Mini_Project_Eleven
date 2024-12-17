install:
	pip install --upgrade pip && pip install -r requirements.txt

format:
	black *.py

lint:
	ruff check *.py my_lib/*.py

test:
# Tests suspended due to difference in Databricks and Github requirements.
#	python -m pytest -vv test_*.py 

container-lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

refactor: format lint

all: install lint test format 