# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM
#
# BkitTEvSim release
# Source Code License Grant: The authors hereby grant to Licensee
# personal permission to use and modify the Licensed Source Code
# for the sole purpose of studying while attending the course CO3007.
#
# Logical Process module for BkitTEvSim Simulation Framework
#
"""
BkSys Logical Process and State classes for PDES.

A BkSysLogicalProcess represents a simulation entity that processes events.
BkSysLogicalProcessState encapsulates the mutable state of the LP.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from copy import deepcopy
import copy as cp
import typing as tp

from .event import Event


@dataclass
class BkSysLogicalProcessState:
    """
    Encapsulates the mutable state of a BkSys Logical Process.
    
    Users should extend this class to define their simulation-specific state.
    
    Attributes:
        timestamp: The virtual time at which this state snapshot was taken.
        data: User-defined state data dictionary.
    """
    # The virtual time at which this state snapshot was taken
    timestamp: float = 0.0
    # User-defined state data
    data: tp.Dict[str, tp.Any] = field(default_factory=dict)
    
    def clone(self) -> BkSysLogicalProcessState:
        """Create a deep copy of this state.
        
        Returns:
            A new BkSysLogicalProcessState instance with deep-copied data.
        """
        return BkSysLogicalProcessState(
            timestamp=self.timestamp,
            data=cp.deepcopy(self.data),
        )
    
    def __repr__(self) -> str:
        return f"BkSysLogicalProcessState(t={self.timestamp:.4f}, keys={list(self.data.keys())})"


class BkSysLogicalProcess(ABC):
    """
    Abstract base class for a BkSys Logical Process in PDES (synchronous/event-driven).
    
    Users must implement the required methods to define their simulation behavior.
    Each LP has a unique ID and maintains its own Local Virtual Time.
    
    This is the synchronous base class. For coroutine-based LPs, use BkSysCoLogicalProcess.
    
    Attributes:
        lp_id: Unique identifier for this logical process.
        local_virt_time: Current local virtual time of this LP.
        state: Current state of this LP.
    """
    
    # Wait return conditions
    FIRST_COMPLETED = "FIRST_COMPLETED"
    FIRST_EXCEPTION = "FIRST_EXCEPTION"
    ALL_COMPLETED = "ALL_COMPLETED"
    
    def __init__(self, lp_id: int, initial_state: tp.Optional[BkSysLogicalProcessState] = None):
        """Initialize a BkSysLogicalProcess.
        
        Args:
            lp_id: Unique identifier for this logical process.
            initial_state: Optional initial state. If not provided, a new BkSysLogicalProcessState is created.
        """
        self.lp_id = lp_id
        self.local_virt_time = 0.0  # Local Virtual Time
        self.state = initial_state if initial_state is not None else BkSysLogicalProcessState()
        self._event_handlers: tp.Dict[str, tp.Callable[[Event], None]] = {}
        
    
    @abstractmethod
    def initialize(self) -> None:
        """Called once at simulation start to initialize the LP.
        
        Override to set up initial state, schedule initial events, etc.
        """
        pass
    
    @abstractmethod
    def handle_event(self, event: Event) -> None:
        """Process an incoming event.
        
        This is the main event handling logic. The LP should:
        1. Update its state based on the event
        2. Schedule new events by calling send_event()
        3. Update local_virt_time to event.timestamp
        
        Args:
            event: The event to process.
        """
        pass
    
    def send_event(self, event: Event) -> None:
        """Send an event to another LP.
        
        This method should be called by the LP implementation to schedule
        events for other LPs.
        
        Args:
            event: The event to send.
        """
        # Default implementation - subclasses or dispatcher should override
        if hasattr(self, '_output_events'):
            self._output_events.append(event)
    
    def get_output_events(self) -> tp.List[Event]:
        """Get and clear the list of events sent since last call.
        
        Returns:
            List of events sent since last call.
        """
        events = getattr(self, '_output_events', [])
        self._output_events = []
        return events
    
    def register_handler(self, event_type: str, handler: tp.Callable[[Event], None]) -> None:
        """Register a handler for a specific event type.
        
        Args:
            event_type: The type of event to handle.
            handler: Callback function to handle the event.
        """
        self._event_handlers[event_type] = handler
    
    def get_local_virt_time(self) -> float:
        """Get the current Local Virtual Time.
        
        Returns:
            Current local virtual time.
        """
        return self.local_virt_time
    
    def set_local_virt_time(self, local_virt_time: float) -> None:
        """Set the Local Virtual Time.
        
        Args:
            local_virt_time: New local virtual time value.
        """
        self.local_virt_time = local_virt_time
    
    def elapse_virt_time(self, delta: float) -> float:
        """Elapse the Local Virtual Time by a delta.
        
        This is useful for modeling processing time within handle_event.
        The LP's local virtual time is increased by delta.
        
        Args:
            delta: Time delta to elapse (must be non-negative).
            
        Returns:
            New local virtual time.
            
        Raises:
            ValueError: If delta is negative.
        """
        if delta < 0:
            raise ValueError("Delta must be non-negative")
        self.local_virt_time += delta
        return self.local_virt_time
    
    def finalize(self) -> None:
        """Called at simulation end for cleanup."""
        pass
    
    def __repr__(self) -> str:
        return f"BkSysLP(id={self.lp_id}, local_virt_time={self.local_virt_time:.4f})"


# Export commonly used types
FIRST_COMPLETED = BkSysLogicalProcess.FIRST_COMPLETED
FIRST_EXCEPTION = BkSysLogicalProcess.FIRST_EXCEPTION
ALL_COMPLETED = BkSysLogicalProcess.ALL_COMPLETED