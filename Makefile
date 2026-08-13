# Project Makefile with Terraform support
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

perf: ## Run performance benchmarks
	(cd crates/oxbow && make bench)

# Terraform targets
.PHONY: terraform-init
terraform-init:
	@echo "=== Initializing Terraform ==="
	@make -C terraform init

.PHONY: terraform-validate
terraform-validate:
	@echo "=== Validating Terraform ==="
	@make -C terraform validate

.PHONY: terraform-test
terraform-test:
	@echo "=== Testing Terraform modules ==="
	@make -C terraform test

.PHONY: terraform-clean
terraform-clean:
	@echo "=== Cleaning Terraform ==="
	@make -C terraform clean

.PHONY: terraform-deploy-simple
tf-deploy-simple:
	@echo "=== Deploying oxbow-simple ==="
	@make -C terraform deploy-simple

.PHONY: terraform-destroy-simple
tf-destroy-simple:
	@echo "=== Destroying oxbow-simple ==="
	@make -C terraform destroy-simple

.PHONY: terraform-full-test
tf-full-test:
	@echo "=== Running full Terraform test ==="
	@make -C terraform full-test

.PHONY: terraform-all
terraform-all: terraform-init terraform-validate terraform-test

.PHONY: terraform-full-test
tf-full-test:
	@echo "=== Running full Terraform test ==="
	@make -C terraform full-test

.PHONY: build-deploy-test
build-deploy-test:
	@echo "=== Building lambdas and testing Terraform ==="
	@make build-release && make terraform-all
