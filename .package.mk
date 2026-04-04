#
# This file is meant to be included inside of package directories, i.e. where
# there is a Cargo.toml such that there are default targets everywhere
#

build: ## Build the package
	cargo build

test: ## Run the tests
	cargo nextest r
