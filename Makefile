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
	(cd crates/buoyant-oxbow && make bench)

# Acceptance tests run the lambdas against LocalStack. See acceptance/ and compose.yml.
# The lambda artifacts are built with the release profile (LocalStack rejects zips over 50MB).
# CI overrides ACCEPTANCE_COMPOSE (e.g. `docker compose`) and CONTAINER_SOCKET (see compose.yml).
ACCEPTANCE_COMPOSE ?= podman compose
.PHONY: acceptance acceptance-up acceptance-down acceptance-build acceptance-test acceptance-logs

acceptance: acceptance-up ## Start LocalStack, run the acceptance tests, then stop LocalStack
	@$(MAKE) acceptance-test; status=$$?; if [ $$status -ne 0 ]; then $(ACCEPTANCE_COMPOSE) logs localstack | tail -n 200; fi; $(MAKE) acceptance-down; exit $$status

acceptance-up: ## Start the LocalStack container used by the acceptance tests
	$(ACCEPTANCE_COMPOSE) up -d --wait

acceptance-down: ## Stop the LocalStack container used by the acceptance tests
	$(ACCEPTANCE_COMPOSE) down

acceptance-build: ## Build the release lambda artifacts used by the acceptance tests
	. venv/bin/activate && cargo lambda build --package oxbow-lambda --profile release --output-format zip --x86-64 --lambda-dir target/lambda/x86-64
	. venv/bin/activate && cargo lambda build --package sqs-ingest --profile release --output-format zip --x86-64 --lambda-dir target/lambda/x86-64

acceptance-test: ## Run the acceptance tests against a running LocalStack (see acceptance-up)
	./acceptance/run.sh

acceptance-logs: ## Tail the LocalStack container logs
	$(ACCEPTANCE_COMPOSE) logs -f localstack

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
