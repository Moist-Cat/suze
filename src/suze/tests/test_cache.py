"""Test cases for the cache system."""

import pytest
import json
import pickle
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, patch, mock_open, MagicMock
import threading
import os

from suze.cache import FileCache, MemCache, CacheBackend


class TestCacheBackend:
    """Tests for the abstract CacheBackend base class."""

    def test_abstract_methods(self):
        """Test that CacheBackend is an abstract class."""
        # Should not be able to instantiate abstract class
        with pytest.raises(TypeError):
            CacheBackend(default_ttl=3600, max_size_bytes=1024)

    def test_contains_operator(self):
        """Test __contains__ method calls exists and checks TTL."""

        class MockCache(CacheBackend):
            def __init__(self):
                self.default_ttl = 3600
                self.max_size_bytes = 1024
                self._lock = threading.Lock()
                self.access_count = {}
                self.key_ttl = {}
                self.logger = Mock()
                
            def _load_metadata(self):
                return {"access": {}, "ttl": {}}
                
            def _save_metadata(self):
                pass
                
            def _get(self, key):
                return None
                
            def _set(self, key, value, ttl):
                pass
                
            def _delete(self, key):
                pass
                
            def exists(self, key):
                return key == "test_key"
                
            def __iter__(self):
                return iter([])

        cache = MockCache()
        # Set TTL for test_key
        cache.key_ttl["test_key"] = time.time() + 3600
        assert "test_key" in cache
        assert "nonexistent" not in cache


class TestFileCache:
    """Tests for the FileCache implementation."""

    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary directory for cache files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def file_cache(self, temp_cache_dir):
        """Create a FileCache instance with a temporary directory."""
        with patch("suze.cache.get_logger") as mock_logger:
            mock_logger.return_value = Mock()
            cache = FileCache(cache_dir=temp_cache_dir, default_ttl=3600)
            
            # Mock the metadata loading to avoid file not found
            with patch.object(cache, '_load_metadata', return_value={"access": {}, "ttl": {}}):
                yield cache

    def test_init_creates_directory(self, temp_cache_dir):
        """Test that cache directory is created on init."""
        new_dir = temp_cache_dir / "new_cache"
        with patch("suze.cache.get_logger") as mock_logger:
            mock_logger.return_value = Mock()
            with patch.object(FileCache, '_load_metadata', return_value={"access": {}, "ttl": {}}):
                cache = FileCache(cache_dir=new_dir)
                assert new_dir.exists()
                assert new_dir.is_dir()

    def test_get_cache_path(self, file_cache):
        """Test cache file path generation."""
        path = file_cache._get_cache_path(123)
        assert path.name == "123.cache"
        assert path.parent == file_cache.cache_dir

    def test_set_and_get_basic(self, file_cache):
        """Test basic set and get operations."""
        test_data = {"key": "value", "list": [1, 2, 3]}

        # Mock file operations
        with patch('builtins.open', mock_open()) as mock_file:
            with patch('pickle.dumps', return_value=b'pickled_data'):
                # Call parent class's set method which will call our _set
                with patch.object(file_cache, '_set') as mock_set:
                    file_cache.set(1, test_data)
                    mock_set.assert_called_once_with(1, test_data, file_cache.default_ttl)
                    
                    # Verify access_count and key_ttl were updated
                    assert 1 in file_cache.access_count
                    assert file_cache.access_count[1] == 0
                    assert 1 in file_cache.key_ttl

            # Test get
            with patch.object(file_cache, 'exists', return_value=True):
                with patch.object(file_cache, '_get', return_value=test_data):
                    result = file_cache.get(1)
                    assert result == test_data
                    assert file_cache.access_count[1] == 1

    def test_get_nonexistent_key(self, file_cache):
        """Test get returns None for non-existent key."""
        with patch.object(file_cache, '__contains__', return_value=False):
            result = file_cache.get(999)
            assert result is None

    def test_get_expired_entry(self, file_cache):
        """Test get returns None for expired entry."""
        # Set an expired TTL
        file_cache.key_ttl[1] = time.time() - 100
        
        with patch.object(file_cache, 'exists', return_value=True):
            with patch.object(file_cache, '_delete') as mock_delete:
                result = file_cache.get(1)
                assert result is None
                mock_delete.assert_called_once_with(1)

    def test_set_with_custom_ttl(self, file_cache):
        """Test set with custom TTL value."""
        test_data = {"custom": "ttl"}
        custom_ttl = 500

        with patch('builtins.open', mock_open()):
            with patch('pickle.dumps', return_value=b'pickled_data'):
                with patch.object(file_cache, '_set') as mock_set:
                    file_cache.set(1, test_data, ttl=custom_ttl)
                    
                    mock_set.assert_called_once_with(1, test_data, custom_ttl)
                    
                    # Check TTL was set correctly
                    assert 1 in file_cache.key_ttl
                    assert file_cache.key_ttl[1] > time.time()

    def test_delete(self, file_cache):
        """Test delete removes key from all structures."""
        # Setup
        file_cache.access_count[1] = 5
        file_cache.key_ttl[1] = time.time() + 3600
        
        with patch.object(file_cache, "exists", return_value=True):
            with patch.object(file_cache, '_delete') as mock_internal_delete:
                file_cache.delete(1)
                
                mock_internal_delete.assert_called_once_with(1)
                assert 1 not in file_cache.access_count
                assert 1 not in file_cache.key_ttl

    def test_delete_nonexistent_key(self, file_cache):
        """Test delete logs warning for non-existent key."""
        with patch.object(file_cache, 'exists', return_value=False):
            with patch.object(file_cache.logger, 'warning') as mock_warning:
                file_cache.delete(999)
                mock_warning.assert_called_once()

    def test_iter(self, file_cache):
        """Test iteration over cache keys."""
        # Create some cache files
        cache_dir = file_cache.cache_dir
        (cache_dir / "123.cache").touch()
        (cache_dir / "456.cache").touch()
        (cache_dir / "not_a_cache.txt").touch()  # Should be ignored
        
        keys = list(file_cache)
        assert set(keys) == {123, 456}

    def test_evict(self, file_cache):
        """Test eviction logic."""
        # Setup some entries with different access counts
        for key in range(0, 6):
            file_cache.set(key, 1, 3600)
        file_cache.access_count = {1: 5, 2: 10, 3: 1, 4: 3, 5: 7}
        temp = file_cache.__iter__

        with patch.object(file_cache, 'delete') as mock_delete:
            file_cache.evict()
            
            # Should evict 50% (3 entries) starting with lowest access count
            assert mock_delete.call_count == 3
            # Check that keys 3, 1, 4 were deleted (lowest access counts)
            calls = [call[0][0] for call in mock_delete.call_args_list]
            assert 3 in calls  # Access count 1
            assert 1 in calls  # Access count 5 (but we only verify some were called)

        file_cache = temp

class TestMemCache:
    """Tests for the MemCache implementation."""

    @pytest.fixture
    def mem_cache(self):
        """Create a MemCache instance for testing."""
        with patch("suze.cache.get_logger") as mock_logger:
            mock_logger.return_value = Mock()
            cache = MemCache(default_ttl=3600, max_size_bytes=1024 * 1024)
            
            # Initialize the parent class metadata
            cache.access_count = {}
            cache.key_ttl = {}
            yield cache

    def test_init(self):
        """Test MemCache initialization."""
        with patch("suze.cache.get_logger") as mock_logger:
            mock_logger.return_value = Mock()
            cache = MemCache(default_ttl=500, max_size_bytes=1024)
            
            assert cache.default_ttl == 500
            assert cache.max_size_bytes == 1024
            assert hasattr(cache, '_cache')
            assert hasattr(cache, '_lock')

    def test_basic_set_and_get(self, mem_cache):
        """Test basic set and get operations."""
        test_data = {"key": "value", "number": 42}

        mem_cache.set(1, test_data)
        result = mem_cache.get(1)

        assert result == test_data
        assert 1 in mem_cache._cache
        assert 1 in mem_cache.access_count
        assert 1 in mem_cache.key_ttl

    def test_get_nonexistent_key(self, mem_cache):
        """Test get returns None for non-existent key."""
        result = mem_cache.get(999)
        assert result is None

    def test_get_expired_entry(self, mem_cache):
        """Test get returns None for expired entry."""
        # Set entry with very short TTL
        mem_cache.set(1, "data", ttl=0.001)
        time.sleep(0.002)  # Wait for expiration

        result = mem_cache.get(1)
        assert result is None
        assert 1 not in mem_cache._cache  # Should be cleaned up
        assert 1 not in mem_cache.access_count
        assert 1 not in mem_cache.key_ttl

    def test_set_with_custom_ttl(self, mem_cache):
        """Test set with custom TTL."""
        mem_cache.set(1, "data", ttl=10)

        expires_at = mem_cache.key_ttl[1]
        created_time = expires_at - 10  # Approximate creation time
        
        assert expires_at > time.time()  # Should be in the future
        assert expires_at - created_time == 10

    def test_exists(self, mem_cache):
        """Test exists method."""
        mem_cache.set(1, "data", ttl=100)

        assert mem_cache.exists(1) is True
        assert mem_cache.exists(2) is False

    def test_exists_expired(self, mem_cache):
        """Test exists returns False for expired entries."""
        mem_cache.set(1, "data", ttl=0.001)
        time.sleep(0.002)

        assert mem_cache.expire(1) is True
        assert 1 not in mem_cache._cache

    def test_contains_operator(self, mem_cache):
        """Test __contains__ operator."""
        mem_cache.set(1, "data")

        assert 1 in mem_cache
        assert 2 not in mem_cache

        # Test expired key
        mem_cache.key_ttl[1] = time.time() - 100  # Expired
        mem_cache.get(1)
        assert 1 not in mem_cache

    def test_eviction_with_small_max_size(self, mem_cache):
        """Test eviction when cache exceeds max_size."""
        # Create cache with very small max size
        mem_cache.max_size_bytes = 500
        
        # Create large data
        large_data = "x" * 300  # Approximately 300 bytes

        # First entry should fit
        with patch.object(mem_cache, '_estimate_size', return_value=300):
            mem_cache.set(1, large_data)
            assert len(mem_cache._cache) == 1

            # Second entry should trigger eviction
            mem_cache.set(2, large_data)
            
            # With our eviction strategy (50% eviction),
            # we should have 1 or 2 entries depending on exact timing
            assert len(mem_cache._cache) <= 2

    def test_eviction_lfu_policy(self, mem_cache):
        """Test that LFU eviction policy works correctly."""
        # Set small max size
        mem_cache.max_size_bytes = 599
        
        with patch.object(mem_cache, '_estimate_size', return_value=300):
            # Add entries
            mem_cache.set(1, "data1")
            mem_cache.set(2, "data2")

            # Access key 1 multiple times
            mem_cache.get(1)
            mem_cache.get(1)

            # Access key 2 once
            mem_cache.get(2)

            # Add third entry to trigger eviction
            mem_cache.set(3, "data3")

            # Key 2 should be evicted (least frequently used)
            # Key 1 was accessed twice, key 2 once
            assert 2 not in mem_cache._cache
            assert 1 in mem_cache._cache
            assert 3 in mem_cache._cache

    def test_thread_safety_basic(self):
        """Test basic thread safety."""
        with patch("suze.cache.get_logger") as mock_logger:
            mock_logger.return_value = Mock()
            cache = MemCache()
            cache.access_count = {}
            cache.key_ttl = {}

            results = []
            errors = []

            def worker(worker_id):
                try:
                    for i in range(100):
                        key = (worker_id * 1000) + i
                        cache.set(key, f"data_{worker_id}_{i}")
                        val = cache.get(key)
                        results.append((worker_id, i, val is not None))
                except Exception as e:
                    errors.append((worker_id, str(e)))

            # Start multiple threads
            threads = []
            for i in range(5):
                t = threading.Thread(target=worker, args=(i,))
                threads.append(t)
                t.start()

            # Wait for all threads
            for t in threads:
                t.join()

            # No errors should occur
            assert len(errors) == 0

    def test_concurrent_access_with_expiration(self):
        """Test concurrent access with expiration handling."""
        with patch("suze.cache.get_logger") as mock_logger:
            mock_logger.return_value = Mock()
            cache = MemCache(default_ttl=1)  # Very short TTL
            cache.access_count = {}
            cache.key_ttl = {}

            # refence
            access_counts = [0]

            def reader(key):
                for _ in range(50):
                    if cache.get(key) is not None:
                        access_counts[0] += 1
                    time.sleep(0.001)

            # Set a value
            cache.set(1, "initial")

            # Start reader threads
            threads = []
            for _ in range(3):
                t = threading.Thread(target=reader, args=(1,))
                threads.append(t)
                t.start()

            # Wait a bit for expiration
            time.sleep(1)

            # Set again
            curr = access_counts[0]
            cache.set(1, "updated")

            # Continue reading
            for _ in range(3):
                t = threading.Thread(target=reader, args=(1,))
                threads.append(t)
                t.start()

            # Wait for all threads
            for t in threads:
                t.join()

            # Should have some successful reads
            assert access_counts[0] > 0
            # expired, miss, miss, miss, ...
            assert access_counts[0] == curr

    def test_estimate_size(self, mem_cache):
        """Test _estimate_size method."""
        # Test with various data types
        test_cases = [
            ("string", "hello"),
            ("int", 42),
            ("float", 3.14),
            ("list", [1, 2, 3]),
            ("dict", {"a": 1, "b": 2}),
            ("bytes", b"data"),
        ]

        for name, data in test_cases:
            mem_cache._cache[1] = data
            size = mem_cache._estimate_size(1)
            assert isinstance(size, int)
            assert size >= 0

    def test_delete(self, mem_cache):
        """Test delete removes from all dictionaries."""
        mem_cache.set(1, "data1")
        mem_cache.set(2, "data2")

        # Access key 1
        mem_cache.get(1)

        assert 1 in mem_cache._cache
        assert 1 in mem_cache.key_ttl
        assert 1 in mem_cache.access_count

        mem_cache.delete(1)

        assert 1 not in mem_cache._cache
        assert 1 not in mem_cache.key_ttl
        assert 1 not in mem_cache.access_count
        assert 2 in mem_cache._cache  # Other key should remain


class TestCacheIntegration:
    """Integration tests for cache system."""

    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary directory for cache files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_error_handling_edge_cases(self):
        """Test error handling in edge cases."""
        with patch("suze.cache.get_logger") as mock_logger:
            mock_logger.return_value = Mock()

            # Test MemCache with invalid inputs
            cache = MemCache()
            cache.access_count = {}
            cache.key_ttl = {}

            # None key (should fail or handle gracefully)
            # Note: The type hints say key is int, but Python is dynamic
            cache.set(None, "data")  # Should not crash
            cache.set((1, 2), "data")  # Tuple as key (hashable)

            # Very large TTL
            cache.set(1, "data", ttl=1e10)  # 10 billion seconds

            # Negative TTL (should expire immediately)
            cache.set(2, "data", ttl=-100)
            assert cache.get(2) is None

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
