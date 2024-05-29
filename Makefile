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

.PHONY: test
test :
	PYTHONPATH=src pytest -v --cov=sgn --cov-report=term-missing .

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
