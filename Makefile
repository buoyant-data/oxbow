
.PHONY: help
help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: all build build-release check test clean
all: check build test ## Perform all the checks builds and testing

check: ## Ensure that the crate meets the basic formatting and structure
	cargo fmt --check
	cargo clippy

build: ## Build the crate with each set of features
	cargo build

build-release: check test ## Build the release versions of Lambdas
	cargo lambda build --release --output-format zip

test: ## Run the crate's tests with each set of features
	cargo test

clean: ## Clean up resources from build
	cargo clean
