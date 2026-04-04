include ../.base.mk

PKGS = $(dir $(wildcard */Makefile))

.PHONY: default build test
default: test
build: $(PKGS)
test: $(PKGS)

$(PKGS)::
	$(MAKE) -C $@ $(MAKECMDGOALS)
