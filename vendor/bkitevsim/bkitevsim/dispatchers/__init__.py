# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM
#
# BkitTEvSim release
# Source Code License Grant: The authors hereby grant to Licensee
# personal permission to use and modify the Licensed Source Code
# for the sole purpose of studying while attending the course CO3007.
#
# Dispatcher module for BkitTEvSim Simulation Framework
#
"""Event dispatcher implementations for PDES."""
from .sequential_eventdispatcher import SequentialEventDispatcher, DistributedEventDispatcher

__all__ = ["SequentialEventDispatcher", "DistributedEventDispatcher"]