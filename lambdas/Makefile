include ../.base.mk

build: ## Build all the lambdas
	for c in $(wildcard *); do \
		if [ -d $$c ] ; then cargo build -p $$c ; fi \
	done

test: ## Run checks for all lambdas
	for c in $(wildcard *); do \
		if [ -d $$c ] ; then cargo test -p $$c ; fi \
	done
