SHELL := /bin/bash
UV ?= uv
DOCS_ADDR ?= 127.0.0.1:8000

.PHONY: help docs-build docs-serve docs-clean docs-prepare sandbox-up sandbox-down sandbox-list

help:
	@echo "Available targets:"
	@echo "  make docs-build    Build static docs site into ./site"
	@echo "  make docs-serve    Run live docs server at $(DOCS_ADDR)"
	@echo "  make docs-clean    Remove generated docs site output"
	@echo "  make docs-prepare  Generate local source-link pages for citations"
	@echo "  make sandbox-up    Bring up a dev sandbox (see docs/development/sandbox.md)"
	@echo "  make sandbox-down  Tear down all dev sandboxes owned by this user"
	@echo "  make sandbox-list  List running dev sandboxes"

docs-prepare:
	python3 scripts/docs_sources/generate_source_pages.py --repo-root .

docs-build: docs-prepare
	$(UV) run --with mkdocs --with mkdocs-material mkdocs build --config-file mkdocs.yml --strict

docs-serve: docs-prepare
	$(UV) run --with mkdocs --with mkdocs-material mkdocs serve --config-file mkdocs.yml --dev-addr $(DOCS_ADDR)

docs-clean:
	rm -rf site
	rm -rf docs/_source

sandbox-up:
	scripts/dev/sandbox/moraine-sandbox up $(SANDBOX_ARGS)

sandbox-down:
	scripts/dev/sandbox/moraine-sandbox down --all

sandbox-list:
	scripts/dev/sandbox/moraine-sandbox list
