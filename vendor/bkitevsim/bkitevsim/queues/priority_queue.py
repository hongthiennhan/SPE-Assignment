# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM
#
# BkitTEvSim release
# Source Code License Grant: The authors hereby grant to Licensee
# personal permission to use and modify the Licensed Source Code
# for the sole purpose of studying while attending the course CO3007.
#
# PriorityQueue - A generic priority queue wrapper using LadderQueue implementation.
#
"""
PriorityQueue - A generic priority queue wrapper using LadderQueue implementation.

Provides a standard heapq-like interface using LadderQueue internally.
Supports both Event objects (using timestamp for priority) and generic items
with explicit priority values.
"""
from __future__ import annotations
from typing import Generic, TypeVar, Optional, List, Dict, Any
from collections import defaultdict
import heapq

from .ladder_queue import LadderQueue
from ..core.event import Event

T = TypeVar('T')


class PriorityQueue(Generic[T]):
    """
    Priority queue wrapper using LadderQueue implementation.
    
    Provides a standard heapq-like interface using LadderQueue internally.
    """
    
    def __init__(self, bucket_width: float = 1.0):
        self._queue = LadderQueue[Event](bucket_width=bucket_width)
        self._sequence = 0
        self._heap: List[tuple] = []  # Fallback for non-Event items
    
    def push(self, item: T, priority: float = None) -> None:
        """Push an item with optional priority."""
        if isinstance(item, Event):
            self._queue.push(item)
        else:
            # Use heapq for non-Event items
            if priority is None:
                priority = self._sequence
            heapq.heappush(self._heap, (priority, self._sequence, item))
            self._sequence += 1
    
    def pop(self) -> Optional[T]:
        """Pop the highest priority item."""
        if self._queue.size() > 0:
            return self._queue.pop()
        elif self._heap:
            return heapq.heappop(self._heap)[2]
        return None
    
    def peek(self) -> Optional[T]:
        """Peek at the highest priority item without removing."""
        if self._queue.size() > 0:
            return self._queue.peek()
        elif self._heap:
            return self._heap[0][2]
        return None
    
    def peek_priority(self) -> Optional[float]:
        """Peek at the priority of the highest priority item."""
        if self._queue.size() > 0:
            # LadderQueue has peek_timestamp() method
            return self._queue.peek_timestamp()
        elif self._heap:
            return self._heap[0][0]
        return None
    
    def is_empty(self) -> bool:
        return self._queue.size() == 0 and len(self._heap) == 0
    
    def size(self) -> int:
        return self._queue.size() + len(self._heap)
    
    def clear(self) -> None:
        self._queue.clear()
        self._heap.clear()