include ../.base.mk

build: ## Build all the crates
	for c in $(wildcard *); do \
		if [ -d $$c ] ; then cargo build -p $$c ; fi \
	done

test: ## Run checks for all crates
	for c in $(wildcard *); do \
		if [ -d $$c ] ; then cargo test -p $$c ; fi \
	done
