# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM
#
# BKsyspeEvsim release
# Source Code License Grant: The authors hereby grant to Licensee
# personal permission to use and modify the Licensed Source Code
# for the sole purpose of studying while attending the course CO3007.
#
# Synchronous LogicalProcess Application for Bkitevsim Simulation Framework
#
"""
BkSys Synchronous LogicalProcess App.

This application demonstrates a basic synchronous simulation using
BkSysLogicalProcess and SequentialEventDispatcher without any asyncio/coroutine code.
"""
from __future__ import annotations
from typing import Dict, Any, List
import logging
from dataclasses import dataclass, field
from enum import Enum

from bkitevsim.core import Event, BkSysLogicalProcess, BkSysLogicalProcessState, Simulation, SimulationOptions
from bkitevsim.dispatchers import SequentialEventDispatcher

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Event types for the application."""
    COMPUTE = "COMPUTE"
    MESSAGE = "MESSAGE"


@dataclass
class SimpleLPState(BkSysLogicalProcessState):
    """State for simple LP."""
    compute_count: int = 0
    messages_sent: int = 0
    messages_received: int = 0


class SimpleLP(BkSysLogicalProcess):
    """Simple Logical Process implemented synchronously."""
    
    def __init__(
        self,
        lp_id: int,
        num_lps: int,
        lookahead: float = 0.1,
        max_computes: int = 10,
        **kwargs
    ):
        initial_state = SimpleLPState()
        super().__init__(lp_id, initial_state, **kwargs)
        
        self.num_lps = num_lps
        self.lookahead = lookahead
        self.max_computes = max_computes
        self.logger = logging.getLogger(f"LP{lp_id}")
    
    def initialize(self) -> None:
        """Initialize - schedule first compute event."""
        self.logger.info(f"LP {self.lp_id} initializing")
        event = Event(
            timestamp=0.0,
            sender_id=self.lp_id,
            receiver_id=self.lp_id,
            event_type=EventType.COMPUTE.value,
            payload={"step": 0}
        )
        self.send_event(event)
    
    def handle_event(self, event: Event) -> None:
        """Process incoming event."""
        # Update local virtual time
        self.local_virt_time = event.timestamp
        
        if event.event_type == EventType.COMPUTE.value:
            self._handle_compute(event)
        elif event.event_type == EventType.MESSAGE.value:
            self._handle_message(event)
    
    def _handle_compute(self, event: Event) -> None:
        """Handle compute event - do work and model processing time."""
        step = event.payload.get("step", 0)
        self.state.compute_count += 1
        # In sync mode, GVT is event.timestamp (global time when event is processed)
        gvt = event.timestamp
        self.logger.info(f"LP {self.lp_id}: LVT={self.local_virt_time:.3f} GVT={gvt:.3f} Compute step {step} (total: {self.state.compute_count})")
        
        # Model compute processing time (e.g., 0.05 simulation time units)
        processing_time = 0.05
        self.elapse_virt_time(processing_time)
        self.logger.info(f"LP {self.lp_id}: LVT={self.local_virt_time:.3f} GVT={gvt:.3f} Processing compute for {processing_time} time units...")
        
        # Send message to next LP (ring topology)
        next_lp = (self.lp_id + 1) % self.num_lps
        msg_event = Event(
            timestamp=self.local_virt_time + self.lookahead,
            sender_id=self.lp_id,
            receiver_id=next_lp,
            event_type=EventType.MESSAGE.value,
            payload={"data": f"msg_from_{self.lp_id}_step_{step}", "seq": step}
        )
        self.send_event(msg_event)
        self.state.messages_sent += 1
        self.logger.info(f"LP {self.lp_id}: LVT={self.local_virt_time:.3f} GVT={gvt:.3f} Sent message to LP {next_lp}")
        
        # Schedule next compute for self
        if step < self.max_computes - 1:
            next_event = Event(
                timestamp=self.local_virt_time + self.lookahead,
                sender_id=self.lp_id,
                receiver_id=self.lp_id,
                event_type=EventType.COMPUTE.value,
                payload={"step": step + 1}
            )
            self.send_event(next_event)
    
    def _handle_message(self, event: Event) -> None:
        """Handle received message - model processing time."""
        self.state.messages_received += 1
        # In sync mode, GVT is event.timestamp
        gvt = event.timestamp
        self.logger.info(f"LP {self.lp_id}: LVT={self.local_virt_time:.3f} GVT={gvt:.3f} Received message: {event.payload}")
        
        # Model message processing time
        processing_time = 0.02
        self.elapse_virt_time(processing_time)
        self.logger.info(f"LP {self.lp_id}: LVT={self.local_virt_time:.3f} GVT={gvt:.3f} Processing message for {processing_time} time units...")
        
        # After processing, we could send acknowledgment or trigger next action
        self.logger.info(f"LP {self.lp_id}: LVT={self.local_virt_time:.3f} GVT={gvt:.3f} Finished processing message: {event.payload}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get LP statistics."""
        return {
            "lp_id": self.lp_id,
            "compute_count": self.state.compute_count,
            "messages_sent": self.state.messages_sent,
            "messages_received": self.state.messages_received,
            "local_virt_time": self.local_virt_time
        }


def create_sync_example_with_simulation(
    num_lps: int = 4,
    end_time: float = 5.0,
    lookahead: float = 0.1,
    max_computes: int = 10
) -> Simulation:
    """Create a synchronous simulation using the high-level Simulation API."""
    
    # Create simulation options
    options = SimulationOptions(
        end_time=end_time,
        dispatcher="sequential",
        bucket_width=1.0
    )
    
    # Create simulation with model registry
    sim = Simulation(options, models={"SimpleLP": SimpleLP})
    
    # Register LPs using the model factory
    for i in range(num_lps):
        sim.create_lp(
            lp_id=i,
            model_name="SimpleLP",
            num_lps=num_lps,
            lookahead=lookahead,
            max_computes=max_computes
        )
    
    return sim


def run_sync_demo():
    """Run the synchronous dispatcher demo."""    
    print("\n[===---BkSys Synchronous LogicalProcess Demo (No Asyncio)---===]")
    # Pattern 2: High-level Simulation API
    sim = create_sync_example_with_simulation(
        num_lps=4,
        end_time=0.3,
        lookahead=0.1,
        max_computes=5
    )
    
    stats = sim.run()
    
    print(f"\nResults:")
    print(f"  Events processed: {stats['events_processed']}")
    print(f"  Wall time: {stats['wall_time']:.4f}s")
    
    all_stats = sim.get_statistics()
    for lp_id, lp_stats in all_stats.get("lp_statistics", {}).items():
        print(f"  LP {lp_id}: computes={lp_stats['compute_count']}, "
              f"sent={lp_stats['messages_sent']}, recv={lp_stats['messages_received']}")
    
    return stats


if __name__ == "__main__":      
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    run_sync_demo()