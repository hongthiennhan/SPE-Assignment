# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM
#
# BKsyspeEvsim release
# Source Code License Grant: The authors hereby grant to Licensee
# personal permission to use and modify the Licensed Source Code
# for the sole purpose of studying while attending the course CO3007.
#
# Makefile for BkSys - BkiTevSim Simulation Framework
#

PYTHON ?= python3
PIP ?= $(PYTHON) -m pip

.PHONY: install test test-verbose scenario-coroutine app-distributed app-synchronous run-apps clean help

# Default target
help:
	@echo "BkSys - BkiTevSim Simulation Framework"
	@echo ""
	@cho "Usage: make <target>"
	@echo ""
	@echo "  samples            - Run all sample apps"
	@echo "  scenario-coroutine - Run coroutine network simulation scenario"
	@echo "  app-distributed    - Run distributed sequential dispatcher app"
	@echo "  app-synchronous    - Run simple synchronous app (no asyncio)"
	@echo "  run-apps           - Run a specific app (EXAMPLE=module_name)"
	@echo "  clean              - Clean build artifacts"


# Run samples
samples: scenario-coroutine app-distributed app-synchronous

# Run coroutine event loop example (network simulation)
scenario-coroutine:
	@echo "Running coroutine network simulation example..."
	timeout 10 $(PYTHON) -m scenarios.coroutine_network_sim || true

# Run distributed sequential dispatcher app
app-distributed:
	@echo "Running distributed sequential dispatcher app..."
	timeout 30 $(PYTHON) -m apps.distributed_sequential_app || true

# Run simple synchronous app (no asyncio)
app-synchronous:
	@echo "Running simple synchronous app..."
	timeout 10 $(PYTHON) -m apps.synchronous_sequential_app || true

# Run a specific example by module name (e.g., make run-apps EXAMPLE=coroutine_network_sim)
run-apps:
	@if [ -z "$(EXAMPLE)" ]; then \
		echo "Usage: make run-apps EXAMPLE=<module_name>"; \
		echo "Available apps: scenarios.coroutine_network_sim, apps.distributed_sequential_app, apps.synchronous_sequential_app"; \
		exit 1; \
	fi
	@echo "Running example: $(EXAMPLE)..."
	$(PYTHON) -m $(EXAMPLE)


# Clean build artifacts
clean:
	rm -rf .pytest_cache/
	rm -rf __pycache__/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
