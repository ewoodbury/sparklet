.DEFAULT_GOAL := help

.PHONY: lint
lint: ## Run linting and formatting
	sbt ";scalafixAll; scalafmt"

.PHONY: test-lint
test-lint: ## Run linting without fixing
	sbt ";scalafixAll --check; scalafmtCheck"

.PHONY: test
test: ## Run tests
	sbt test

.PHONY: test-one
test-one: ## Run a single suite: make test-one T=com.ewoodbury.sparklet.core.TestLocalActions
	sbt "sparklet-tests/testOnly $(T)"

.PHONY: help
help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
