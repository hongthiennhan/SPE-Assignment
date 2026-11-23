# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM
#
# BKsyspeEvsim release
# Source Code License Grant: The authors hereby grant to Licensee
# personal permission to use and modify the Licensed Source Code
# for the sole purpose of studying while attending the course CO3007.
#
# Coroutine LogicalProcess Application for Bkitevsim Simulation Framework
#
"""
BkSys Coroutine Distributed Sequential App 
Demonstrates coroutine-based LPs with distributed event processing.

This application shows how to use BkSysCoLogicalProcess with BkSysCoEventLoop
for distributed event processing using cooperative multitasking.

Events are handled via handle_event() to respect the abstract send_event/handle_event pattern.
"""
from __future__ import annotations
from typing import Dict, Any, List
import logging
from dataclasses import dataclass, field
from enum import Enum

from bkitevsim.core import Event, BkSysLogicalProcessState, Simulation, SimulationOptions
from bkitevsim.core.logical_coProcess import (
    BkSysCoLogicalProcess,
    BkSysCoEventLoop,
    BkSysCoLPStatus,
)
from bkitevsim.dispatchers import DistributedEventDispatcher

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


class BkSysCoSimpleLP(BkSysCoLogicalProcess):
    """Simple Logical Process implemented as a coroutine using handle_event pattern.
    
    This LP demonstrates the proper abstraction:
    - send_event() is used to send events (abstract method from BkSysLogicalProcess)
    - handle_event() receives and processes events (abstract method from BkSysLogicalProcess)
    - coroutine_run() just schedules initial events and waits for simulation end
    """
    
    def __init__(
        self,
        lp_id: int,
        num_lps: int,
        lookahead: float = 0.1,
        max_computes: int = 10,
        **kwargs
    ):
        initial_state = SimpleLPState()
        super().__init__(lp_id, initial_state, lookahead=lookahead, **kwargs)
        
        self.num_lps = num_lps
        self.lookahead = lookahead
        self.max_computes = max_computes
        self.logger = logging.getLogger(f"LP{lp_id}")
    
    def handle_event(self, event: Event) -> None:
        """Handle incoming events directly in handle_event.
        
        This is the classic PDES pattern where handle_event does all the work.
        No wait_for needed in coroutine_run.
        """
        self.logger.debug(f"LP {self.lp_id} handle_event: {event.event_type}")
        
        if event.event_type == EventType.COMPUTE.value:
            self._handle_compute(event)
        elif event.event_type == EventType.MESSAGE.value:
            self._handle_message(event)
        elif event.event_type == "MSG_PROCESSED":
            self._handle_msg_processed(event)
        else:
            # For any other events, buffer for potential wait_for users
            super().handle_event(event)
    
    async def coroutine_run(self) -> None:
        """Main coroutine - schedules initial events and waits for simulation end."""
        self.logger.info(f"BkSysCo LP {self.lp_id} starting")
        
        # Schedule initial compute event using send_event (abstract method)
        event = Event(
            timestamp=0.0,
            sender_id=self.lp_id,
            receiver_id=self.lp_id,
            event_type=EventType.COMPUTE.value,
            payload={"step": 0}
        )
        self.send_event(event)
        
        # Just wait until simulation ends - all event processing is in handle_event
        while self._event_loop and self._event_loop.current_time < self._event_loop.end_time:
            # Check if we've done enough computes
            if self.state.compute_count >= self.max_computes:
                break
            await self.advance_time(0.1)  # Yield control periodically
        
        self.logger.info(f"BkSysCo LP {self.lp_id} finished")
    
    def _handle_compute(self, event: Event) -> None:
        """Handle compute event - do work and send messages to neighbors.
        
        Note: We do NOT schedule the next compute here. The next compute is
        scheduled in _handle_msg_processed after message processing completes.
        This models the processing delay properly.
        """
        step = event.payload.get("step", 0)
        self.state.compute_count += 1
        gvt = self._event_loop.current_time if self._event_loop else 0.0
        self.logger.info(f"LP {self.lp_id}: LVT={self.local_virt_time:.3f} GVT={gvt:.3f} Compute step {step} (total: {self.state.compute_count})")

        # Model compute processing time (e.g., 0.05 simulation time units)
        processing_time = 0.05

        # Simulate some local processing time for compute
        self.elapse_virt_time(processing_time)
        gvt = self._event_loop.current_time if self._event_loop else 0.0
        self.logger.info(f"LP {self.lp_id}: LVT={self.local_virt_time:.3f} GVT={gvt:.3f} Processing compute for {processing_time} time units...")

        # Only send message if this is not the last compute step
        if step < self.max_computes - 1:
            # Send message to next LP (ring topology)
            next_lp = (self.lp_id + 1) % self.num_lps
            msg_event = Event(
                timestamp=self._event_loop.current_time + self.lookahead,
                sender_id=self.lp_id,
                receiver_id=next_lp,
                event_type=EventType.MESSAGE.value,
                payload={"data": f"msg_from_{self.lp_id}_step_{step}", "seq": step}
            )
            self.send_event(msg_event)
            self.state.messages_sent += 1
            gvt = self._event_loop.current_time if self._event_loop else 0.0
            self.logger.info(f"LP {self.lp_id}: LVT={self.local_virt_time:.3f} GVT={gvt:.3f} Sent message to LP {next_lp}")
    
    
    def _handle_message(self, event: Event) -> None:
        """Handle received message - models processing time by advancing LVT.
        
        In PDES, processing time is modeled by scheduling follow-up events at a
        future timestamp (current_time + processing_time). This simulates the
        time it takes to process the message.
        
        To properly model sequential processing (message processing delays next compute),
        we DON'T schedule the next compute here - instead we schedule it in _handle_msg_processed.
        """
        
        self.state.messages_received += 1
        gvt = self._event_loop.current_time if self._event_loop else 0.0
        self.logger.info(f"LP {self.lp_id}: LVT={self.local_virt_time:.3f} GVT={gvt:.3f} Received message: {event.payload}")
        
        # Store the step so we can schedule next compute after processing
        step = event.payload.get("seq", 0)
        self._pending_compute_step = step

        # Model message processing time (e.g., 0.05 simulation time units)
        processing_time = 0.02
        gvt = self._event_loop.current_time if self._event_loop else 0.0
        self.logger.info(f"LP {self.lp_id}: LVT={self.local_virt_time:.3f} GVT={gvt:.3f} Processing message for {processing_time} time units...")
        # Schedule a completion event to model processing time
        completion_event = Event(
            timestamp=self._event_loop.current_time + processing_time,
            sender_id=self.lp_id,
            receiver_id=self.lp_id,
            event_type="MSG_PROCESSED",
            payload=event.payload
        )
        self.send_event(completion_event)
    
    def _handle_msg_processed(self, event: Event) -> None:
        """Called after message processing is complete - now schedule next compute."""
        gvt = self._event_loop.current_time if self._event_loop else 0.0
        self.logger.info(f"LP {self.lp_id}: LVT={self.local_virt_time:.3f} GVT={gvt:.3f} Finished processing message: {event.payload}")
        
        # Now schedule next compute (after message processing delay)
        step = self._pending_compute_step if hasattr(self, '_pending_compute_step') else 0
        if step < self.max_computes - 1:
            next_event = Event(
                timestamp=self._event_loop.current_time + self.lookahead,
                sender_id=self.lp_id,
                receiver_id=self.lp_id,
                event_type=EventType.COMPUTE.value,
                payload={"step": step + 1}
            )
            self.send_event(next_event)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get LP statistics."""
        return {
            "lp_id": self.lp_id,
            "compute_count": self.state.compute_count,
            "messages_sent": self.state.messages_sent,
            "messages_received": self.state.messages_received,
            "local_virt_time": self.local_virt_time
        }


def create_bksys_distributed_example_with_simulation(
    num_lps: int = 4,
    end_time: float = 10.0,
    lookahead: float = 0.1,
    max_computes: int = 10
) -> Simulation:
    """Create a coroutine-based simulation using the high-level Simulation API with DistributedEventDispatcher."""
    
    # Create simulation options with distributed dispatcher
    options = SimulationOptions(
        end_time=end_time,
        dispatcher="distributed_sequential",
        bucket_width=1.0
    )
    
    # Create simulation with model registry
    sim = Simulation(options, models={"BkSysCoSimpleLP": BkSysCoSimpleLP})
    
    # Register LPs using the model factory
    for i in range(num_lps):
        sim.create_lp(
            lp_id=i,
            model_name="BkSysCoSimpleLP",
            num_lps=num_lps,
            lookahead=lookahead,
            max_computes=max_computes
        )
    
    return sim


def run_bksys_distributed_demo():
    """Run the BkSys co-simulation distributed dispatcher demo."""

    print("\n[===---BkSys Co-Simulation Distributed Sequential Dispatcher Demo---===]")
    # Pattern: High-level Simulation API with distributed dispatcher
    sim = create_bksys_distributed_example_with_simulation(
        num_lps=4,
        end_time=0.4,
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

    run_bksys_distributed_demo()