.PHONE: all
all: format lint type-check test

.PHONY: help
help :
	@echo
	@echo 'Commands:'
	@echo
	@echo '  make test                  run tests'
	@echo '  make lint                  run linter'
	@echo '  make format                run code formatter'
	@echo '  make type-check            run static type checker'
	@echo


.PHONY: test test-base
test :
	PYTHONPATH=src pytest -v --markdown-docs --markdown-docs-syntax=superfences .
test-base :
	PYTHONPATH=src pytest -v .

.PHONY: lint
lint :
	flake8 .

.PHONY: format
format :
	isort .
	black .

.PHONY: type-check
type-check :
	mypy .

.PHONY: docs
docs :
	mkdocs build
