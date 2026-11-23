# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM
#
# BkitTEvSim release
# Source Code License Grant: The authors hereby grant to Licensee
# personal permission to use and modify the Licensed Source Code
# for the sole purpose of studying while attending the course CO3007.
#
# Core module for BkitTEvSim Simulation Framework
#
"""Core BkSys components: Events, Logical Processes, and Simulation."""
from .event import Event
from .logical_process import (
    BkSysLogicalProcess,
    BkSysLogicalProcessState,
    FIRST_COMPLETED,
    FIRST_EXCEPTION,
    ALL_COMPLETED,
)
from .simulation import Simulation, SimulationOptions
from .logical_coProcess import (
    BkSysCoLPStatus,
    BkSysCoEventLoop,
    BkSysCoLogicalProcess,
    BkSysFuture,
    TIMEOUT_ERROR,
)
from bkitevsim.queues import PriorityQueue

__all__ = [
    "Event", "BkSysLogicalProcess", "BkSysLogicalProcessState", "Simulation", "SimulationOptions",
    "BkSysCoLPStatus", "PriorityQueue",
    "BkSysCoEventLoop", "BkSysCoLogicalProcess", "BkSysFuture",
    # PDES-provided async primitives (from LogicalProcess)
    "FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED",
    "TIMEOUT_ERROR",
]