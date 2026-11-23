# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM
#
# BkitTEvSim release
# Source Code License Grant: The authors hereby grant to Licensee
# personal permission to use and modify the Licensed Source Code
# for the sole purpose of studying while attending the course CO3007.
#
# Sequential Event Dispatcher
#
"""
Sequential Event Dispatcher.

A single-threaded event dispatcher that processes events in strict timestamp order
using a centralized priority queue. Ideal for debugging and small simulations.
"""
from __future__ import annotations
from typing import Dict, List, Optional, Type
import time
import logging

from bkitevsim.core.event import Event
from bkitevsim.core.logical_process import BkSysLogicalProcess, BkSysLogicalProcessState
from bkitevsim.queues import LadderQueue

logger = logging.getLogger(__name__)


class SequentialEventDispatcher:
    """
    Sequential (single-threaded) event dispatcher.
    
    Processes events in strict timestamp order using a central priority queue.
    No parallelism - simple and deterministic execution.
    
    Attributes:
        lps: Dictionary mapping LP ID to LogicalProcess instance.
        event_queue: Central priority queue for all events (LadderQueue).
        current_time: Global simulation time.
        end_time: Simulation end time.
        stats: Performance statistics dictionary.
    """
    
    def __init__(
        self,
        end_time: float = float('inf'),
        bucket_width: float = 1.0
    ):
        """Initialize the SequentialEventDispatcher.
        
        Args:
            end_time: Simulation end time. Defaults to infinity.
            bucket_width: Bucket width for LadderQueue. Defaults to 1.0.
        """
        self.lps: Dict[int, LogicalProcess] = {}
        self.end_time = end_time
        self.current_time = 0.0
        self.bucket_width = bucket_width
        self.stats = {
            "events_processed": 0,
            "start_time": 0.0,
            "end_time": 0.0,
            "wall_time": 0.0
        }
        
        # Initialize event queue - always use LadderQueue for parallel capability
        self.event_queue = LadderQueue[Event](bucket_width=bucket_width)
        
        # LP to queue mapping (for multi-queue variants)
        self._lp_queues: Dict[int, LadderQueue] = {}
    
    def register_lp(self, lp: LogicalProcess) -> None:
        """Register a Logical Process with the dispatcher.
        
        Args:
            lp: The LogicalProcess to register.
            
        Raises:
            ValueError: If an LP with the same ID is already registered.
        """
        if lp.lp_id in self.lps:
            raise ValueError(f"LP with ID {lp.lp_id} already registered")
        self.lps[lp.lp_id] = lp
        # Set the dispatcher's schedule_event as the LP's send_event method
        lp.send_event = self.schedule_event
        logger.debug(f"Registered LP {lp.lp_id}")
    
    def schedule_event(self, event: Event) -> None:
        """Schedule an event for processing.
        
        Args:
            event: The event to schedule.
        """
        self.event_queue.push(event)
        logger.debug(f"Scheduled: {event}")
    
    def schedule_initial_events(self) -> None:
        """Allow all LPs to schedule their initial events by calling their initialize() method."""
        for lp in self.lps.values():
            lp.initialize()
    
    def run(self) -> Dict:
        """
        Run the simulation until end_time or no more events.
        
        Returns:
            Dictionary with simulation statistics including events_processed and wall_time.
        """
        logger.info(f"Starting sequential simulation until t={self.end_time}")
        self.stats["start_time"] = time.perf_counter()
        
        # Let LPs schedule initial events
        self.schedule_initial_events()
        
        # Main simulation loop
        while not self.event_queue.is_empty():
            event = self.event_queue.pop()
            if event is None:
                break
            
            # Check simulation end condition
            if event.timestamp > self.end_time:
                logger.debug(f"Event at t={event.timestamp:.4f} exceeds end_time={self.end_time}")
                break
            
            # Advance global time
            self.current_time = event.timestamp
            
            # Deliver event to receiver LP
            receiver = self.lps.get(event.receiver_id)
            if receiver is None:
                logger.warning(f"No LP found for receiver_id={event.receiver_id}, dropping event")
                continue
            
            # Update receiver's LVT
            receiver.set_local_virt_time(event.timestamp)
            
            # Process event
            try:
                receiver.handle_event(event)
                self.stats["events_processed"] += 1
            except Exception as e:
                logger.error(f"Error processing event {event}: {e}")
                raise
        
        # Finalize all LPs
        for lp in self.lps.values():
            lp.finalize()
        
        self.stats["end_time"] = time.perf_counter()
        self.stats["wall_time"] = self.stats["end_time"] - self.stats["start_time"]
        
        logger.info(f"Simulation completed. Events processed: {self.stats['events_processed']}, "
                   f"Wall time: {self.stats['wall_time']:.4f}s, Final time: {self.current_time:.4f}")
        
        return self.stats
    
    def get_lp(self, lp_id: int) -> Optional[LogicalProcess]:
        """Get LP by ID.
        
        Args:
            lp_id: LP identifier.
            
        Returns:
            The LogicalProcess instance or None if not found.
        """
        return self.lps.get(lp_id)
    
    def get_all_lps(self) -> List[LogicalProcess]:
        """Get all registered LPs.
        
        Returns:
            List of all registered LogicalProcess instances.
        """
        return list(self.lps.values())
    
    def __repr__(self) -> str:
        return (f"SequentialEventDispatcher(lps={len(self.lps)}, "
                f"queue_size={len(self.event_queue)}, time={self.current_time:.4f})")


class DistributedEventDispatcher(SequentialEventDispatcher):
    """
    Sequential dispatcher with per-LP queues (partitioned).
    
    Each LP has its own local event queue. The dispatcher selects the LP
    with the minimum next event timestamp. This mimics the structure of
    parallel dispatchers but runs sequentially.
    
    Attributes:
        _lp_queues: Dictionary mapping LP IDs to their local LadderQueue instances.
        _lp_next_event: Dictionary mapping LP IDs to their next pending event.
    """
    
    def __init__(self, end_time: float = float('inf'), bucket_width: float = 1.0):
        """Initialize the DistributedEventDispatcher.
        
        Args:
            end_time: Simulation end time. Defaults to infinity.
            bucket_width: Bucket width for LadderQueue. Defaults to 1.0.
        """
        super().__init__(end_time, bucket_width)
        self._lp_queues: Dict[int, LadderQueue] = {}
        self._lp_next_event: Dict[int, Optional[Event]] = {}
    
    def register_lp(self, lp: LogicalProcess) -> None:
        """Register a Logical Process with the dispatcher.
        
        Args:
            lp: The LogicalProcess to register.
        """
        super().register_lp(lp)
        self._lp_queues[lp.lp_id] = LadderQueue[Event](bucket_width=self.bucket_width)
        self._lp_next_event[lp.lp_id] = None
    
    def schedule_event(self, event: Event) -> None:
        """Schedule event in the receiver's local queue.
        
        Args:
            event: The event to schedule.
        """
        queue = self._lp_queues.get(event.receiver_id)
        if queue is None:
            logger.warning(f"No queue for LP {event.receiver_id}, using central queue")
            self.event_queue.push(event)
        else:
            queue.push(event)
            # Update next event for this LP
            self._update_next_event(event.receiver_id)
    
    def _update_next_event(self, lp_id: int) -> None:
        """Update the cached next event for an LP.
        
        Args:
            lp_id: ID of the LP to update.
        """
        queue = self._lp_queues.get(lp_id)
        if queue and not queue.is_empty():
            self._lp_next_event[lp_id] = queue.peek()
        else:
            self._lp_next_event[lp_id] = None
    
    def _find_min_event(self) -> Optional[Event]:
        """Find the LP with the minimum next event timestamp.
        
        Returns:
            The event with the minimum timestamp, or None if no events pending.
        """
        min_event = None
        min_lp_id = None
        
        for lp_id, event in self._lp_next_event.items():
            if event is not None:
                if min_event is None or event.timestamp < min_event.timestamp:
                    min_event = event
                    min_lp_id = lp_id
        
        return min_event
    
    def run(self) -> Dict:
        """Run partitioned sequential simulation.
        
        Returns:
            Dictionary with simulation statistics including events_processed and wall_time.
        """
        logger.info(f"Starting partitioned sequential simulation until t={self.end_time}")
        self.stats["start_time"] = time.perf_counter()
        
        self.schedule_initial_events()
        
        # Initialize next events
        for lp_id in self._lp_queues:
            self._update_next_event(lp_id)
        
        while True:
            event = self._find_min_event()
            if event is None:
                break
            
            if event.timestamp > self.end_time:
                break
            
            # Pop from the LP's queue
            queue = self._lp_queues[event.receiver_id]
            queue.pop()  # Remove the event we're processing
            self._update_next_event(event.receiver_id)
            
            self.current_time = event.timestamp
            
            receiver = self.lps.get(event.receiver_id)
            if receiver is None:
                continue
            
            # Synchronize LP's local virtual time to global time (event timestamp)
            old_lvt = receiver.local_virt_time
            receiver.set_local_virt_time(event.timestamp)
            
            # Log synchronization for visualization
            logger.debug(f"SYNC: LP {event.receiver_id} LVT {old_lvt:.3f} -> {event.timestamp:.3f} (GVT={self.current_time:.3f})")
            
            try:
                receiver.handle_event(event)
                self.stats["events_processed"] += 1
            except Exception as e:
                logger.error(f"Error processing event {event}: {e}")
                raise
        
        for lp in self.lps.values():
            lp.finalize()
        
        self.stats["end_time"] = time.perf_counter()
        self.stats["wall_time"] = self.stats["end_time"] - self.stats["start_time"]
        
        logger.info(f"Partitioned simulation completed. Events: {self.stats['events_processed']}, "
                   f"Wall time: {self.stats['wall_time']:.4f}s")
        
        return self.stats