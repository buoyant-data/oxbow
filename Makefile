include .base.mk

.PHONY: all build build-release check test clean deploy
all: check build test ## Perform all the checks builds and testing

check: ## Ensure that the crate meets the basic formatting and structure
	cargo fmt --check
	cargo clippy
	(cd deployment && terraform fmt -check)

build: ## Build the crate with each set of features
	./ci/build.sh

build-release: check test ## Build the release versions of Lambdas
	./ci/build-release.sh

deploy: check ## Deploy the examples
	(cd deployment && terraform apply)

test: ## Run the crate's tests with each set of features
	./ci/test.sh

clean: ## Clean up resources from build
	cargo clean
