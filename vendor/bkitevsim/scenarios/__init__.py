# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM
#
# BkiTevSim release
# Source Code License Grant: The authors hereby grant to Licensee
# personal permission to use and modify the Licensed Source Code
# for the sole purpose of studying while attending the course CO3007.
#
# Scenario simulation package for BkSys - BkiTevSim Simulation Framework

# Use lazy imports to avoid RuntimeWarning when running modules with `python -m`
# The modules are imported on first attribute access rather than at package import time.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .coroutine_network_sim import (
        CoroutineSocketLP,
        CoroutineAppLP,
        create_coroutine_network_sim,
        run_network_demo,
    )

__all__ = [
    "CoroutineSocketLP",
    "CoroutineAppLP",
    "create_coroutine_network_sim",
    "run_network_demo",
]

def __getattr__(name: str):
    if name in __all__:
        from .coroutine_network_sim import (
            CoroutineSocketLP,
            CoroutineAppLP,
            create_coroutine_network_sim,
            run_network_demo,
        )
        globals().update({
            "CoroutineSocketLP": CoroutineSocketLP,
            "CoroutineAppLP": CoroutineAppLP,
            "create_coroutine_network_sim": create_coroutine_network_sim,
            "run_network_demo": run_network_demo,
        })
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")