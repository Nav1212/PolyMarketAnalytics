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
    
    def empty(self) -> bool:
        """Check if queue is empty. Compatible with Queue interface."""
        with self._lock:
            return self._item_count == 0
    
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
            # Get current buffer
            items = list(self._buffer)
            
            # Reset buffer
            self._buffer = deque()
            self._item_count = 0
            self._threshold_event.clear()
            
            return items
    
    def wait_for_threshold(self, timeout: Optional[float] = None) -> bool:
        """
        Wait until the queue reaches threshold or shutdown is signaled.
        
        Args:
            timeout: Maximum time to wait in seconds
        
        Returns:
            True if threshold was reached, False if timeout or shutdown
        """
        result = self._threshold_event.wait(timeout)
        return result and not self._shutdown
    
    def shutdown(self) -> None:
        """Signal shutdown to any waiting threads."""
        self._shutdown = True
        self._threshold_event.set()
    
    def is_shutdown(self) -> bool:
        """Check if shutdown has been signaled."""
        return self._shutdown
    
    def get_all(self) -> List[Any]:
        """
        Get all items without clearing the buffer.
        
        Returns:
            Copy of all items currently in queue
        """
        with self._lock:
            return list(self._buffer)
    
    def clear(self) -> None:
        """Clear all items from the queue."""
        with self._lock:
            self._buffer = deque()
            self._item_count = 0
            self._threshold_event.clear()
