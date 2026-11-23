# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM
#
# BkitTEvSim release
# Source Code License Grant: The authors hereby grant to Licensee
# personal permission to use and modify the Licensed Source Code
# for the sole purpose of studying while attending the course CO3007.
#
# Event module for BkitTEvSim Simulation Framework
#
"""
Event class for Parallel Discrete Event Simulation.

An Event represents a timestamped message sent between Logical Processes.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional
import uuid


@dataclass(order=True)
class Event:
    """
    Represents a discrete event in the simulation.
    
    Events are ordered by timestamp for priority queue operations.
    The order=True parameter generates comparison methods based on timestamp.
    
    Attributes:
        timestamp: The simulation time when the event occurs.
        sender_id: ID of the LP that sent the event.
        receiver_id: ID of the LP that should receive the event.
        event_type: Type/category of the event.
        payload: Optional data payload associated with the event.
        event_id: Unique identifier for this event.
    """
    timestamp: float
    sender_id: int
    receiver_id: int
    event_type: str
    payload: Any = field(default=None, compare=False)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()), compare=False)
    
    def __post_init__(self):
        """Validate event after initialization.
        
        Raises:
            ValueError: If timestamp is negative or LP IDs are negative.
        """
        if self.timestamp < 0:
            raise ValueError("Event timestamp cannot be negative")
        if self.sender_id < 0 or self.receiver_id < 0:
            raise ValueError("LP IDs must be non-negative")
    
    def __repr__(self) -> str:
        return (f"Event(t={self.timestamp:.4f}, {self.sender_id}->{self.receiver_id}, "
                f"{self.event_type}, id={self.event_id[:8]})")


# Convenience function for creating events
def create_event(timestamp: float, sender_id: int, receiver_id: int,
                 event_type: str, payload: Any = None) -> Event:
    """Factory function to create a new event.
    
    Args:
        timestamp: The simulation time when the event occurs.
        sender_id: ID of the LP that sent the event.
        receiver_id: ID of the LP that should receive the event.
        event_type: Type/category of the event.
        payload: Optional data payload associated with the event.
        
    Returns:
        A new Event instance.
    """
    return Event(timestamp, sender_id, receiver_id, event_type, payload)