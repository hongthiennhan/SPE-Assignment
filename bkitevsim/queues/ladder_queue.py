# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM
#
# BkitTEvSim release
# Source Code License Grant: The authors hereby grant to Licensee
# personal permission to use and modify the Licensed Source Code
# for the sole purpose of studying while attending the course CO3007.
#
# LadderQueue - parallel discrete event simulation.
#
"""
LadderQueue - A scalable priority queue for parallel discrete event simulation.

Based on the Ladder Queue data structure optimized for PDES simulations.

# Tang, Wai Teng, Rick Siow Mong Goh, and Ian Li-Jin Thng. 
# "Ladder queue: An O (1) priority queue structure for large-scale discrete event simulation."
# ACM Transactions on Modeling and Computer Simulation (TOMACS) 15.3 (2005).
"""
from __future__ import annotations
from typing import Generic, TypeVar, List, Optional, Dict
from collections import defaultdict
import threading

# Binary search library
import bisect

from ..core.event import Event

T = TypeVar('T', bound=Event)


class LadderQueue(Generic[T]):
    """
    A Ladder Queue implementation.
       
    Attributes:
        bucket_width: Time range covered by each bucket.
        num_buckets: Number of buckets in the ladder (circular buffer).
        _buckets: A dictionary mapping bucket index to list of events
        _bucket_boundaries: A sorted list of bucket boundaries (timestamps) bucket min timestamps
        _min_bucket_idx: Current minimum bucket index for fast access
        _max_bucket_idx: Current maximum bucket index for fast access
        _entry_count: Entry counter
    """
    
    def __init__(self, bucket_width: float = 1.0, num_buckets: int = 1024):
        """Initialize the Ladder Queue.
        
        Args:
            bucket_width: Time range covered by each bucket. Defaults to 1.0.
            num_buckets: Number of buckets in the ladder (circular buffer). Defaults to 1024.
        """
        self.bucket_width = bucket_width
        self.num_buckets = num_buckets
        self._buckets: Dict[int, List[T]] = defaultdict(list)
        self._bucket_boundaries: List[float] = []  
        self._min_bucket_idx: Optional[int] = None
        self._max_bucket_idx: Optional[int] = None
        self._entry_count = 0
        self._lock = threading.RLock()
        self._size = 0
    
    def _get_bucket_index(self, timestamp: float) -> int:
        """Calculate bucket index for a given timestamp.
        
        Args:
            timestamp: The timestamp to calculate bucket index for.
            
        Returns:
            The bucket index for the given timestamp.
        """
        return int(timestamp / self.bucket_width) % self.num_buckets
    
    def _get_bucket_min_time(self, bucket_idx: int) -> float:
        """Get the minimum timestamp for a bucket index.
        
        Args:
            bucket_idx: The bucket index.
            
        Returns:
            The minimum timestamp for the given bucket index.
        """
        return bucket_idx * self.bucket_width
    
    def push(self, item: T) -> None:
        """Add an event to the appropriate bucket.
        
        Args:
            item: The event to add to the queue.
        """
        ts = item.timestamp
        if ts != ts:  # NaN check
            raise ValueError("timestamp cannot be NaN")
        
        with self._lock:
            bucket_idx = self._get_bucket_index(item.timestamp)
            bucket = self._buckets[bucket_idx]
            
            # Insert in timestamp order within bucket (with entry count tiebreaker)
            entry = (item.timestamp, self._entry_count, item)
            bisect.insort(bucket, entry)
            
            # Update min and max bucket index
            if self._min_bucket_idx is None or bucket_idx < self._min_bucket_idx:
                self._min_bucket_idx = bucket_idx
            if self._max_bucket_idx is None or bucket_idx > self._max_bucket_idx:
                self._max_bucket_idx = bucket_idx
            
            # Update the state bucket boundaries
            if bucket_idx not in self._bucket_boundaries:
                bisect.insort(self._bucket_boundaries, bucket_idx)

            self._entry_count += 1            
            self._size += 1

    def _refresh_min_max(self) -> None:
        if self._bucket_boundaries:
            self._min_bucket_idx = self._bucket_boundaries[0]
            self._max_bucket_idx = self._bucket_boundaries[-1]
        else:
            self._min_bucket_idx = None
            self._max_bucket_idx = None

    def _drop_bucket(self, idx: int) -> None:
        """Remove a bucket and refresh min/max from the boundaries.

        Args:
            index: The event to add to the queue.
        """
        self._buckets.pop(idx, None)

        i = bisect.bisect_left(self._bucket_boundaries, idx)
        if i < len(self._bucket_boundaries) and self._bucket_boundaries[i] == idx:
            del self._bucket_boundaries[i]

        # Update min/max bucket boundaries 
        self._refresh_min_max()
    
    def pop(self) -> Optional[T]:
        """Get the item.
        
        Returns:
            The returned item.
        """
        with self._lock:
            if self._size == 0:
                return None
            
            # Find the minimum non-empty bucket
            while self._min_bucket_idx is not None:
                idx = self._min_bucket_idx
                bucket = self._buckets.get(idx)
                if not bucket:
                    # Bucket is empty, remove and continue
                    self._drop_bucket(idx)
                    continue
  
                _, _, item = bucket.pop(0)
                self._size -= 1
  
                if not bucket:
                    # After pop item, bucket might empty - drop it
                    self._drop_bucket(idx)
               
                return item
            
            # Queue is empty
            return None

    def pop_events_after(self, timestamp: float) -> List[T]:
        """Get the item after timestamp.
        
        Args:
            timestamp: The timestamp threshold.
            
        Returns:
            Get item.
        """
        with self._lock:
            removed = []
            if self._size == 0:
                return removed
            
            # Find buckets that might contain events after timestamp
            target_bucket = self._get_bucket_index(timestamp)
            bounds = self._bucket_boundaries
            
            # Check current bucket and all higher buckets
            for bucket_idx in list(self._buckets.keys()):
                if bucket_idx > target_bucket:
                    # All events in this bucket are after timestamp
                    bucket = self._buckets[bucket_idx]
                    for _, _, item in bucket:
                        removed.append(item)
                    self._size -= len(bucket)
                    self._drop_bucket(bucket_idx)
                elif bucket_idx == target_bucket:
                    # Need to check individual events in this bucket
                    bucket = self._buckets[bucket_idx]
                    new_bucket = []
                    for entry in bucket:
                        if entry[0] > timestamp:
                            removed.append(entry[2])
                            self._size -= 1
                        else:
                            new_bucket.append(entry)
                    if new_bucket:
                        self._buckets[bucket_idx] = new_bucket
                    else:
                        self._drop_bucket(bucket_idx)

            return removed

    def remove_events_after(self, timestamp: float) -> List[T]:
        """Deprecated: use pop_events_after()."""
        return self.pop_events_after(timestamp)

    def peek(self) -> Optional[T]:
        """Return the event with the smallest timestamp without removing it.
        
        Returns:
            The event with the smallest timestamp, or None if queue is empty.
        """
        with self._lock:
            if self._size == 0 or self._min_bucket_idx is None:
                return None
            
            bucket = self._buckets.get(self._min_bucket_idx)
            if bucket:
                return bucket[0][2]
            return None
    
    def peek_timestamp(self) -> Optional[float]:
        """Get the item.
        
        Returns:
            The return item.
        """
        with self._lock:
            if self._size == 0 or self._min_bucket_idx is None:
                return None
            bucket = self._buckets.get(self._min_bucket_idx)
            if bucket:
                return bucket[0][0]
            return None
        
            min_ts = None
            for bucket in self._buckets.values():
                if bucket:
                    local_min = min(item[0] for item in bucket)
                    if min_ts is None or local_min < min_ts:
                        min_ts = local_min
                        
            return min_ts



    
    def is_empty(self) -> bool:
        """Check if the queue is empty.
        
        Returns:
            True if queue is empty, False otherwise.
        """
        with self._lock:
            return self._size == 0
    
    def size(self) -> int:
        """Return the number of events in the queue.
        
        Returns:
            Number of events in the queue.
        """
        with self._lock:
            return self._size
    
    def clear(self) -> None:
        """Remove all events from the queue."""
        with self._lock:
            self._buckets.clear()
            self._bucket_boundaries.clear()
            self._min_bucket_idx = None
            self._max_bucket_idx = None
            self._size = 0
            self._entry_count = 0
    
    def __len__(self) -> int:
        """Return the number of events in the queue.
        
        Returns:
            Number of events in the queue.
        """
        with self._lock:
            return self._size
    
    def __bool__(self) -> bool:
        """Check if the queue is non-empty.
        
        Returns:
            True if queue has events, False otherwise.
        """
        with self._lock:
            return self._size > 0
    
    def __repr__(self) -> str:
        with self._lock:
            return f"LadderQueue(size={self._size}, buckets={len(self._buckets)})"