"""
Unit tests for SwappableQueue.

Tests thread safety, threshold triggering, and swap operations.
"""

import pytest
import threading
import time
from concurrent.futures import ThreadPoolExecutor


class TestSwappableQueueBasic:
    """Test basic queue operations."""
    
    def test_put_single_item(self, swappable_queue):
        """Test adding a single item to the queue."""
        swappable_queue.put("item1")
        assert swappable_queue.size() == 1
        assert not swappable_queue.empty()
    
    def test_put_many_items(self, swappable_queue):
        """Test adding multiple items at once."""
        items = ["item1", "item2", "item3", "item4", "item5"]
        swappable_queue.put_many(items)
        assert swappable_queue.size() == 5
    
    def test_empty_queue(self, swappable_queue):
        """Test empty queue state."""
        assert swappable_queue.empty()
        assert swappable_queue.size() == 0
    
    def test_size_after_operations(self, swappable_queue):
        """Test size tracking after various operations."""
        assert swappable_queue.size() == 0
        
        swappable_queue.put("a")
        assert swappable_queue.size() == 1
        
        swappable_queue.put_many(["b", "c", "d"])
        assert swappable_queue.size() == 4


class TestSwappableQueueThreshold:
    """Test threshold-based operations."""
    
    def test_should_swap_below_threshold(self, swappable_queue):
        """Test should_swap returns False below threshold."""
        # Threshold is 10
        swappable_queue.put_many(["item"] * 5)
        assert not swappable_queue.should_swap()
    
    def test_should_swap_at_threshold(self, swappable_queue):
        """Test should_swap returns True at threshold."""
        # Threshold is 10
        swappable_queue.put_many(["item"] * 10)
        assert swappable_queue.should_swap()
    
    def test_should_swap_above_threshold(self, swappable_queue):
        """Test should_swap returns True above threshold."""
        swappable_queue.put_many(["item"] * 15)
        assert swappable_queue.should_swap()
    
    def test_swap_returns_all_items(self, swappable_queue):
        """Test swap returns all items and empties queue."""
        items = ["item1", "item2", "item3", "item4", "item5"]
        swappable_queue.put_many(items)
        
        swapped = swappable_queue.swap()
        
        assert len(swapped) == 5
        assert swapped == items
        assert swappable_queue.size() == 0
        assert swappable_queue.empty()
    
    def test_swap_on_empty_queue(self, swappable_queue):
        """Test swap on empty queue returns empty list."""
        swapped = swappable_queue.swap()
        assert swapped == []
        assert swappable_queue.empty()


class TestSwappableQueueThreadSafety:
    """Test thread safety of the queue."""
    
    def test_concurrent_puts(self, swappable_queue):
        """Test concurrent put operations are thread-safe."""
        num_threads = 10
        items_per_thread = 100
        
        def put_items(thread_id):
            for i in range(items_per_thread):
                swappable_queue.put(f"thread_{thread_id}_item_{i}")
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(put_items, i) for i in range(num_threads)]
            for f in futures:
                f.result()
        
        expected_count = num_threads * items_per_thread
        assert swappable_queue.size() == expected_count
    
    def test_concurrent_put_many(self, swappable_queue):
        """Test concurrent put_many operations are thread-safe."""
        num_threads = 5
        items_per_batch = 50
        
        def put_batch(thread_id):
            items = [f"thread_{thread_id}_item_{i}" for i in range(items_per_batch)]
            swappable_queue.put_many(items)
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(put_batch, i) for i in range(num_threads)]
            for f in futures:
                f.result()
        
        expected_count = num_threads * items_per_batch
        assert swappable_queue.size() == expected_count
    
    def test_concurrent_put_and_swap(self, swappable_queue):
        """Test concurrent put and swap operations."""
        total_items = []
        total_items_lock = threading.Lock()
        
        def producer():
            for i in range(100):
                swappable_queue.put(f"item_{i}")
                time.sleep(0.001)
        
        def consumer():
            for _ in range(10):
                time.sleep(0.01)
                items = swappable_queue.swap()
                with total_items_lock:
                    total_items.extend(items)
        
        producer_thread = threading.Thread(target=producer)
        consumer_thread = threading.Thread(target=consumer)
        
        producer_thread.start()
        consumer_thread.start()
        
        producer_thread.join()
        consumer_thread.join()
        
        # Get any remaining items
        remaining = swappable_queue.swap()
        total_items.extend(remaining)
        
        # All 100 items should be accounted for
        assert len(total_items) == 100


class TestSwappableQueueSwapIfReady:
    """Test swap_if_ready method."""
    
    def test_swap_if_ready_below_threshold(self, swappable_queue):
        """Test swap_if_ready returns None below threshold."""
        swappable_queue.put_many(["item"] * 5)
        result = swappable_queue.swap_if_ready()
        assert result is None
        assert swappable_queue.size() == 5  # Items still in queue
    
    def test_swap_if_ready_at_threshold(self, swappable_queue):
        """Test swap_if_ready returns items at threshold."""
        swappable_queue.put_many(["item"] * 10)
        result = swappable_queue.swap_if_ready()
        assert result is not None
        assert len(result) == 10
        assert swappable_queue.size() == 0


class TestSwappableQueueDrain:
    """Test drain method."""
    
    def test_drain_returns_all_items(self, swappable_queue):
        """Test drain returns all items regardless of threshold."""
        swappable_queue.put_many(["item"] * 5)  # Below threshold
        
        drained = swappable_queue.drain()
        
        assert len(drained) == 5
        assert swappable_queue.empty()
    
    def test_drain_empty_queue(self, swappable_queue):
        """Test drain on empty queue returns empty list."""
        drained = swappable_queue.drain()
        assert drained == []


class TestSwappableQueueShutdown:
    """Test shutdown functionality."""
    
    def test_shutdown_flag(self, swappable_queue):
        """Test shutdown sets the flag."""
        assert not swappable_queue.is_shutdown
        swappable_queue.shutdown()
        assert swappable_queue.is_shutdown
    
    def test_put_after_shutdown(self, swappable_queue):
        """Test that put still works after shutdown (for draining)."""
        swappable_queue.shutdown()
        # Should not raise an error - shutdown just signals intent
        swappable_queue.put("item")
        assert swappable_queue.size() == 1
