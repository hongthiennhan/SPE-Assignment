# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM
#
# BkiTevSim release
# Source Code License Grant: The authors hereby grant to Licensee
# personal permission to use and modify the Licensed Source Code
# for the sole purpose of studying while attending the course CO3007.
#
# Scenario simulation package for BkSys - BkiTevSim Simulation Framework

"""
BkSys Co-Simulation Network Simulation Example - Push-based Socket/App Interaction.

This example demonstrates a push-based network simulation where:
- Socket LP receives NETWORK_DATA and pushes data to App LP via APP_DATA event
- App LP processes data via handle_event (no polling needed)
- Events delivered via handle_event() to respect the abstract send_event/handle_event pattern
- No threads or locks needed
"""
from __future__ import annotations
from typing import Dict, Any, List, Optional
import logging
from dataclasses import dataclass, field
from enum import Enum

from bkitevsim.core import (
    Event,
    BkSysCoLogicalProcess,
    BkSysLogicalProcessState,
    BkSysCoEventLoop,
    BkSysCoLPStatus,
    BkSysLogicalProcess,
    Simulation,
    SimulationOptions,
    TIMEOUT_ERROR,
)
from bkitevsim.dispatchers import SequentialEventDispatcher

logger = logging.getLogger(__name__)


class SocketEventType(Enum):
    """Event types for network simulation."""
    NETWORK_DATA = "NETWORK_DATA"  # External network data arrives at socket
    APP_DATA = "APP_DATA"          # Socket pushes data to application
    APP_ACK = "APP_ACK"            # Application acknowledges processing


@dataclass
class SocketState(BkSysLogicalProcessState):
    """State for socket LP."""
    buffer: List[str] = field(default_factory=list)
    max_buffer_size: int = 10
    stats: Dict[str, int] = field(default_factory=lambda: {
        "received": 0, "delivered": 0, "dropped": 0, "pushed": 0
    })


@dataclass
class AppState(BkSysLogicalProcessState):
    """State for application LP."""
    stats: Dict[str, int] = field(default_factory=lambda: {
        "processed": 0
    })


class BkSysCoSocketLP(BkSysCoLogicalProcess):
    """Socket LP - receives network data and pushes to application.
    
    This LP demonstrates the push-based pattern:
    - Receives NETWORK_DATA via handle_event
    - Buffers data and pushes to App LP via APP_DATA event (send_event)
    - Receives APP_ACK from App LP to confirm processing
    """
    
    def __init__(
        self,
        lp_id: int,
        app_lp_id: int,
        max_buffer_size: int = 10,
        lookahead: float = 0.001,
        **kwargs
    ):
        initial_state = SocketState(max_buffer_size=max_buffer_size)
        super().__init__(lp_id, initial_state, lookahead=lookahead, **kwargs)
        self.app_lp_id = app_lp_id
        self.max_buffer_size = max_buffer_size
        self._scheduled_arrivals: List[tuple] = []
        # [FIX-5] counter shared with handle_event (was a never-incremented local)
        self.arrivals_processed = 0
    
    def add_arrival(self, timestamp: float, data: str) -> None:
        """Pre-schedule a network data arrival."""
        self._scheduled_arrivals.append((timestamp, data))
        self._scheduled_arrivals.sort(key=lambda x: x[0])
    
    def handle_event(self, event: Event) -> None:
        """Handle incoming events directly in handle_event.
        
        This is the classic PDES pattern where handle_event does all the work.
        """
        logger.debug(f"Socket LP {self.lp_id} handle_event: {event.event_type}")
        
        if event.event_type == SocketEventType.NETWORK_DATA.value:
            self._handle_network_data(event)
        elif event.event_type == SocketEventType.APP_ACK.value:
            self._handle_app_ack(event)
        else:
            # For any other events, buffer for potential wait_for users
            super().handle_event(event)
    
    async def coroutine_run(self) -> None:
        """Main socket coroutine - just schedules initial events and waits."""
        logger.info(f"Socket LP {self.lp_id} starting")
        
        # Schedule pre-configured arrivals using send_event
        for timestamp, data in self._scheduled_arrivals:
            event = Event(
                timestamp=timestamp,
                sender_id=0,  # External
                receiver_id=self.lp_id,
                event_type=SocketEventType.NETWORK_DATA.value,
                payload=data
            )
            self.send_event(event)
        
        # Track expected arrivals to know when to terminate
        expected_arrivals = sum(1 for t, _ in self._scheduled_arrivals
                        if t < self._event_loop.end_time)
        self.arrivals_processed = 0
        
        # Just wait until simulation ends - all event processing is in handle_event
        while self._event_loop and self._event_loop.current_time < self._event_loop.end_time:
            # If all scheduled arrivals have been processed and buffer is empty, we can terminate
            if self.arrivals_processed >= expected_arrivals and not self.state.buffer:
                # Give a small grace period for any pending acks
                await self.advance_time(0.01)
                if not self.state.buffer and self.arrivals_processed >= expected_arrivals:
                    logger.info(f"Socket LP {self.lp_id}: All arrivals processed and buffer empty, terminating")
                    await self.advance_time(0.01)
                    break
            
            await self.advance_time(0.1)  # Yield control periodically
        
        logger.info(f"Socket LP {self.lp_id} finished at time={self._event_loop.current_time if self._event_loop else 'N/A'}")
    
    def _handle_network_data(self, event: Event) -> None:
        """Handle incoming network data - buffer and push to app."""
        data = event.payload
        buffer = self.state.buffer
        
        if len(buffer) < self.max_buffer_size:
            buffer.append(data)
            self.state.stats['received'] += 1
            logger.info(f"Socket {self.lp_id}: Buffered '{data}' (size={len(buffer)})")
            
            # Push data to application immediately if buffer was empty
            # (or always push - here we push each packet as it arrives)
            if self.state.buffer and self.state.buffer[0] == data:
                self.state.buffer.pop(0)
            
            self.state.stats['delivered'] += 1
            self.state.stats['pushed'] += 1
            
            app_data_event = Event(
                timestamp=self._event_loop.current_time + self._lookahead,
                sender_id=self.lp_id,
                receiver_id=self.app_lp_id,
                event_type=SocketEventType.APP_DATA.value,
                payload=data
            )
            logger.info(f"Socket {self.lp_id}: Pushing data to app: '{data}'")
            self.send_event(app_data_event)
        else:
            self.state.stats['dropped'] += 1
            logger.warning(f"Socket {self.lp_id}: Buffer full, dropped '{data}'")

        # [FIX-5] packet handled (pushed or dropped) -> lets coroutine_run terminate
        self.arrivals_processed += 1
    
    def _handle_app_ack(self, event: Event) -> None:
        """Handle acknowledgment from application."""
        logger.info(f"Socket {self.lp_id}: Received APP_ACK for '{event.payload}'")
        # Could track pending pushes, retransmit on timeout, etc.


class BkSysCoAppLP(BkSysCoLogicalProcess):
    """Application LP - receives data via handle_event and processes it.
    
    This LP demonstrates the push-based pattern:
    - Receives APP_DATA via handle_event (pushed from socket)
    - Processes data and sends APP_ACK back to socket
    - No polling needed - pure event-driven
    """
    
    def __init__(
        self,
        lp_id: int,
        socket_lp_id: int,
        lookahead: float = 0.001,
        max_events: int = 10,
        **kwargs
    ):
        initial_state = AppState()
        super().__init__(lp_id, initial_state, lookahead=lookahead, **kwargs)
        self.socket_lp_id = socket_lp_id
        self.max_events = max_events
    
    def handle_event(self, event: Event) -> None:
        """Handle incoming events directly in handle_event."""
        logger.info(f"App LP {self.lp_id} handle_event: {event.event_type}")
        
        if event.event_type == SocketEventType.APP_DATA.value:
            self._handle_app_data(event)
        else:
            # For any other events, buffer for potential wait_for users
            super().handle_event(event)
    
    async def coroutine_run(self) -> None:
        """Main application coroutine - just waits for simulation end."""
        logger.info(f"App LP {self.lp_id} starting (push-based, no polling)")
        
        events_processed = 0
        
        # Just wait until simulation ends or max events processed
        while events_processed < self.max_events and self._event_loop and \
              self._event_loop.current_time < self._event_loop.end_time:
            # Yield control periodically; clamp to end_time so float drift (0.1*k)
            # never parks a timer beyond the horizon (would strand this LP)
            remaining = self._event_loop.end_time - self._event_loop.current_time
            await self.advance_time(min(0.1, remaining))
            
            # Check if we've processed enough events
            events_processed = self.state.stats['processed']
        
        logger.info(f"App LP {self.lp_id} finished: {self.state.stats}")
    
    def _handle_app_data(self, event: Event) -> None:
        """Process data pushed from socket."""
        data = event.payload
        self.state.stats['processed'] += 1
        logger.info(f"App {self.lp_id}: Processed '{data}' ({self.state.stats['processed']}/{self.max_events})")
        
        # Send acknowledgment back to socket
        ack_event = Event(
            timestamp=self._event_loop.current_time + self._lookahead,
            sender_id=self.lp_id,
            receiver_id=self.socket_lp_id,
            event_type=SocketEventType.APP_ACK.value,
            payload=data
        )
        self.send_event(ack_event)


def create_bksys_network_sim(
    socket_lp_id: int = 101,
    app_lp_id: int = 202,
    end_time: float = 1.0,
    network_arrivals: List[tuple] = None
) -> tuple:
    """
    Create a push-based network simulation with BkSys co-simulation LPs.
    
    Args:
        socket_lp_id: ID for socket LP
        app_lp_id: ID for app LP
        end_time: Maximum simulation time
        network_arrivals: List of (timestamp, payload) for network packets
        
    Returns:
        Tuple of (event_loop, socket_lp, app_lp)
    """
    # Use LadderQueue with small bucket_width for exact priority ordering
    event_loop = BkSysCoEventLoop(end_time=end_time, bucket_width=0.001)
    
    socket_lp = BkSysCoSocketLP(
        lp_id=socket_lp_id,
        app_lp_id=app_lp_id,
        max_buffer_size=10,
        lookahead=0.001
    )
    
    app_lp = BkSysCoAppLP(
        lp_id=app_lp_id,
        socket_lp_id=socket_lp_id,
        lookahead=0.001,
        max_events=10
    )
    
    # Schedule network arrivals
    if network_arrivals:
        for timestamp, payload in network_arrivals:
            socket_lp.add_arrival(timestamp, payload)
    else:
        # Default arrivals
        socket_lp.add_arrival(0.05, "HTTP_GET /index.html")
        socket_lp.add_arrival(0.12, "HTTP_GET /style.css")
        socket_lp.add_arrival(0.20, "HTTP_GET /app.js")
        socket_lp.add_arrival(0.30, "HTTP_GET /api/data")
        socket_lp.add_arrival(0.40, "HTTP_GET /api/user")
    
    event_loop.register_lp(socket_lp)
    event_loop.register_lp(app_lp)
    
    # Set event loop references
    socket_lp.set_event_loop(event_loop)
    app_lp.set_event_loop(event_loop)
    
    return event_loop, socket_lp, app_lp


def run_network_demo():
    """Run the push-based network socket/app demo."""

    print("\n[===---BkSys Co-Simulation Push Network Simulation Demo---===]")
    
    network_arrivals = [
        (0.05, "HTTP_GET /index.html"),
        (0.12, "HTTP_GET /style.css"),
        (0.20, "HTTP_GET /app.js"),
        (0.30, "HTTP_GET /api/data"),
        (0.40, "HTTP_GET /api/user"),
    ]

    print("\n--- Push-based Pattern: Socket pushes APP_DATA to App ---")
    event_loop, socket_lp, app_lp = create_bksys_network_sim(
        socket_lp_id=101,
        app_lp_id=202,
        end_time=1.0,
        network_arrivals=network_arrivals
    )
    
    stats = event_loop.run()
    
    print(f"\nResults (push-based):")
    print(f"  Events processed: {stats['events_processed']}")
    print(f"  Wall time: {stats['wall_time']:.4f}s")
    
    print(f"\nSocket LP (101) stats: {socket_lp.state.stats}")
    print(f"App LP (202) stats: {app_lp.state.stats}")
    
    return stats


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    run_network_demo()