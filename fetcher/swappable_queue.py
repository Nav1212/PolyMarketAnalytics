"""
Thread-safe queue wrapper with atomic swap capability for non-blocking persistence.
"""

import threading
from typing import List, Any, Optional
from collections import deque


class SwappableQueue:
    """
    A thread-safe queue that supports atomic swap operations.
    
    Workers can continuously add items while a separate thread can atomically
    swap out the buffer when it reaches a threshold, without blocking writers.
    
    Usage:
        queue = SwappableQueue(threshold=10000)
        
        # Workers add items
        queue.put(trade)
        
        # Persister checks and swaps when ready
        if queue.should_swap():
            items = queue.swap()  # Returns list, queue is now empty
            # Process items in background...
    """
    
    def __init__(self, threshold: int = 1000000):
        """
        Initialize the swappable queue.
        
        Args:
            threshold: Number of items that triggers a swap (default 1000000)
        """
        self._buffer: deque = deque()
        self._lock = threading.Lock()
        self._threshold = threshold
        self._item_count = 0
        
        # Event to signal when threshold is reached
        self._threshold_event = threading.Event()
        
        # Flag to signal shutdown
        self._shutdown = False
    
    def put(self, item: Any) -> None:
        """
        Add an item to the queue. Thread-safe.
        
        Args:
            item: Item to add to the queue
        """
        with self._lock:
            self._buffer.append(item)
            self._item_count += 1
            
            # Signal if we've hit the threshold
            if self._item_count >= self._threshold:
                self._threshold_event.set()
    
    def put_many(self, items: List[Any]) -> None:
        """
        Add multiple items to the queue. Thread-safe.
        More efficient than calling put() multiple times.
        
        Args:
            items: List of items to add
        """
        with self._lock:
            self._buffer.extend(items)
            self._item_count += len(items)
            
            if self._item_count >= self._threshold:
                self._threshold_event.set()
    
    def size(self) -> int:
        """Return current number of items in queue."""
        with self._lock:
            return self._item_count
    
    def should_swap(self) -> bool:
        """Check if queue has reached threshold."""
        with self._lock:
            return self._item_count >= self._threshold
    
    def swap(self) -> List[Any]:
        """
        Atomically swap out the current buffer and return its contents.
        The queue is immediately ready for new items.
        
        Returns:
            List of all items that were in the queue
        """
        with self._lock:
            # Swap in a fresh buffer
            old_buffer = self._buffer
            self._buffer = deque()
            self._item_count = 0
            self._threshold_event.clear()
            
            return list(old_buffer)
    
    def swap_if_ready(self) -> Optional[List[Any]]:
        """
        Atomically swap out the buffer only if threshold is reached.
        
        Returns:
            List of items if threshold was reached, None otherwise
        """
        with self._lock:
            if self._item_count >= self._threshold:
                old_buffer = self._buffer
                self._buffer = deque()
                self._item_count = 0
                self._threshold_event.clear()
                return list(old_buffer)
            return None
    
    def wait_for_threshold(self, timeout: Optional[float] = None) -> bool:
        """
        Block until the threshold is reached or timeout expires.
        
        Args:
            timeout: Maximum seconds to wait (None = wait forever)
            
        Returns:
            True if threshold was reached, False if timeout expired
        """
        return self._threshold_event.wait(timeout=timeout)
    
    def drain(self) -> List[Any]:
        """
        Drain all remaining items from the queue (for shutdown).
        
        Returns:
            List of all remaining items
        """
        with self._lock:
            old_buffer = self._buffer
            self._buffer = deque()
            self._item_count = 0
            self._threshold_event.clear()
            return list(old_buffer)
    
    def shutdown(self) -> None:
        """Signal shutdown to wake up any waiting threads."""
        self._shutdown = True
        self._threshold_event.set()
    
    @property
    def is_shutdown(self) -> bool:
        """Check if shutdown has been signaled."""
        return self._shutdown
    
    def __len__(self) -> int:
        """Return current size of queue."""
        return self.size()
