"""Test cases for the merge_loader function."""

import pytest
import asyncio
from typing import Dict, List, Any
from unittest.mock import patch

from suze.loaders import merge_loader


class TestMergeLoader:
    """Test suite for merge_loader function."""

    # Helper method to run async tests
    @pytest.fixture
    def event_loop(self):
        """Create event loop for async tests."""
        loop = asyncio.get_event_loop_policy().new_event_loop()
        yield loop
        loop.close()

    # Test 1: Basic merging with common target field
    @pytest.mark.asyncio
    async def test_basic_merge_two_sources(self):
        """Test basic merge operation with two data sources."""
        data_dict = {
            "users": [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
            ],
            "scores": [{"id": 1, "score": 95}, {"id": 2, "score": 88}],
        }

        result = await merge_loader(data_dict, target="id")

        expected = [
            {"id": 1, "name": "Alice", "age": 30, "score": 95},
            {"id": 2, "name": "Bob", "age": 25, "score": 88},
        ]
        assert result == expected
        assert len(result) == 2

    # Test 2: Merge with multiple sources (more than 2)
    @pytest.mark.asyncio
    async def test_merge_multiple_sources(self):
        """Test merge with three different data sources."""
        data_dict = {
            "base": [{"id": 1, "name": "Item1"}, {"id": 2, "name": "Item2"}],
            "details": [{"id": 1, "color": "red"}, {"id": 2, "color": "blue"}],
            "pricing": [{"id": 1, "price": 100}, {"id": 2, "price": 200}],
        }

        result = await merge_loader(data_dict, target="id")

        expected = [
            {"id": 1, "name": "Item1", "color": "red", "price": 100},
            {"id": 2, "name": "Item2", "color": "blue", "price": 200},
        ]
        assert result == expected

    # Test 3: No target field (should flatten)
    @pytest.mark.asyncio
    async def test_merge_no_target_flatten(self):
        """Test merge without target field - should flatten all records."""
        data_dict = {"source1": [{"a": 1}, {"b": 2}], "source2": [{"c": 3}, {"d": 4}]}

        result = await merge_loader(data_dict, target=None)

        expected = [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]
        assert result == expected
        assert len(result) == 4

    # Test 4: Merge with selector parameter
    @pytest.mark.asyncio
    async def test_merge_with_selector(self):
        """Test merge using selector to include only specific sources."""
        data_dict = {
            "users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            "scores": [{"id": 1, "score": 95}, {"id": 2, "score": 88}],
            "logs": [{"id": 1, "log": "active"}, {"id": 2, "log": "inactive"}],
        }

        result = await merge_loader(
            data_dict, target="id", selector={"users", "scores"}
        )

        # Should only include users and scores, not logs
        expected = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 88},
        ]
        assert result == expected
        assert "log" not in result[0]

    # Test 5: Different merge strategies
    @pytest.mark.asyncio
    async def test_merge_strategies(self):
        """Test different merge strategies for field conflicts."""

        # Test aggregate strategy (default)
        data_dict_aggregate = {
            "source1": [{"id": 1, "status": "active"}],
            "source2": [{"id": 1, "status": "pending"}],
        }

        result_aggregate = await merge_loader(
            data_dict_aggregate, target="id", merge_strategy="aggregate"
        )
        assert result_aggregate[0]["status"] == ["active", "pending"]

        # Test overwrite strategy
        result_overwrite = await merge_loader(
            data_dict_aggregate, target="id", merge_strategy="overwrite"
        )
        assert result_overwrite[0]["status"] == "pending"  # Last source wins

        # Test ignore strategy
        result_ignore = await merge_loader(
            data_dict_aggregate, target="id", merge_strategy="ignore"
        )
        assert result_ignore[0]["status"] == "active"  # First source preserved

    # Test 6: Custom merge strategy (callable)
    @pytest.mark.asyncio
    async def test_custom_merge_strategy(self):
        """Test merge with custom callable strategy."""

        def custom_merge(old_val, new_val, merged_record, new_record):
            """Custom strategy: concatenate string values."""
            if isinstance(old_val, str) and isinstance(new_val, str):
                return f"{old_val},{new_val}"
            return new_val  # fallback to overwrite

        data_dict = {
            "source1": [{"id": 1, "tags": "python"}],
            "source2": [{"id": 1, "tags": "async"}],
        }

        result = await merge_loader(data_dict, target="id", merge_strategy=custom_merge)

        assert result[0]["tags"] == "python,async"

    # Test 7: Auto-index behavior
    @pytest.mark.asyncio
    async def test_auto_index(self):
        """Test auto_index when target field is missing in some records."""
        data_dict = {
            "source1": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            "source2": [
                {"value": 100},  # Missing 'id' field
                {"value": 200},  # Missing 'id' field
            ],
        }

        result = await merge_loader(data_dict, target="id", auto_index=True)

        # Should match records by position when auto_index=True
        assert len(result) == 2
        assert result[0]["name"] == "Alice"
        assert result[0]["value"] == 100
        assert result[1]["name"] == "Bob"
        assert result[1]["value"] == 200

    # Test 8: Strict mode
    @pytest.mark.asyncio
    async def test_strict_mode(self):
        """Test strict mode behavior."""
        # With strict=True (default), only merge records that exist in master table
        data_dict = {
            "master": [{"id": 1, "name": "Alice"}],  # First source is master
            "other": [
                {"id": 1, "score": 95},  # This will merge
                {"id": 2, "score": 88},  # This won't merge (not in master)
            ],
        }

        # Strict mode (default)
        result_strict = await merge_loader(data_dict, target="id", strict=True)
        assert len(result_strict) == 1  # Only one merged record
        assert result_strict[0]["id"] == 1

        # With strict=False
        result_not_strict = await merge_loader(data_dict, target="id", strict=False)
        assert len(result_not_strict) == 2  # Both records included

    # Test 9: Non-dict records (should be skipped)
    @pytest.mark.asyncio
    async def test_non_dict_records(self):
        """Test handling of non-dictionary records."""
        data_dict = {
            "source1": [{"id": 1, "name": "Alice"}, "not a dict", 123],
            "source2": [{"id": 1, "score": 95}],
        }

        result = await merge_loader(data_dict, target="id")

        # Only dict records should be processed
        assert len(result) == 1
        assert result[0] == {"id": 1, "name": "Alice", "score": 95}

    # Test 10: Missing target field without auto_index
    @pytest.mark.asyncio
    async def test_missing_target_field(self):
        """Test behavior when target field is missing and auto_index=False."""
        data_dict = {
            "source1": [{"id": 1, "name": "Alice"}],
            "source2": [{"name": "Bob"}],  # Missing 'id' field
        }

        with patch("suze.loaders.logger") as mock_logger:
            result = await merge_loader(data_dict, target="id", auto_index=False)

            # Should log error for missing target field
            mock_logger.error.assert_called()

        # Only first record should be in result
        assert len(result) == 1
        assert result[0]["id"] == 1

    # Test 11: Target field value is None
    @pytest.mark.asyncio
    async def test_target_field_none(self):
        """Test handling when target field value is None."""
        data_dict = {
            "source1": [{"id": None, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            "source2": [{"id": 2, "score": 95}],
        }

        result = await merge_loader(data_dict, target="id")

        # Record with id=None should be skipped
        assert len(result) == 1
        assert result[0]["id"] == 2
        assert result[0]["name"] == "Bob"
        assert result[0]["score"] == 95

    # Test 12: Target field value is a list (should raise ValueError)
    @pytest.mark.asyncio
    async def test_target_field_is_list(self):
        """Test that ValueError is raised when target field contains a list."""
        data_dict = {
            "source1": [{"id": [1, 2], "name": "Invalid"}],
            "source2": [{"id": [1, 2], "score": 100}],
        }

        with pytest.raises(ValueError, match="Invalid key value"):
            await merge_loader(data_dict, target="id")

    # Test 13: Empty data_dict
    @pytest.mark.asyncio
    async def test_empty_data_dict(self):
        """Test merge with empty input dictionary."""
        result = await merge_loader({}, target="id")
        assert result == {}

        result_no_target = await merge_loader({}, target=None)
        assert result_no_target == {}

    # Test 14: Single source (no actual merge needed)
    @pytest.mark.asyncio
    async def test_single_source(self):
        """Test merge with only one data source."""
        data_dict = {"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}

        result = await merge_loader(data_dict, target="id")

        # Should return the same data (no merging needed)
        assert result == data_dict["users"]

    # Test 15: Mixed data types (list and single dict)
    @pytest.mark.asyncio
    async def test_mixed_data_types(self):
        """Test merge when sources contain both lists and single dictionaries."""
        data_dict = {
            "source1": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            "source2": {"id": 1, "score": 95},  # Single dict, not a list
        }

        result = await merge_loader(data_dict, target="id")

        # Single dict should be converted to list with one element
        assert len(result) == 2  # One merged, one unmerged
        merged_record = next(r for r in result if r["id"] == 1)
        assert merged_record["name"] == "Alice"
        assert merged_record["score"] == 95

    # Test 16: Nested field conflicts
    @pytest.mark.asyncio
    async def test_nested_field_conflicts(self):
        """Test merge with multiple conflicting fields."""
        data_dict = {
            "source1": [{"id": 1, "name": "Alice", "status": "active"}],
            "source2": [{"id": 1, "name": "Alicia", "status": "pending", "score": 95}],
        }

        result = await merge_loader(data_dict, target="id", merge_strategy="aggregate")

        assert result[0]["id"] == 1
        assert result[0]["name"] == ["Alice", "Alicia"]
        assert result[0]["status"] == ["active", "pending"]
        assert result[0]["score"] == 95

    # Test 19: Unicode and special characters
    @pytest.mark.asyncio
    async def test_unicode_and_special_chars(self):
        """Test merge with unicode and special characters in data."""
        data_dict = {
            "source1": [
                {"id": 1, "name": " café", "emoji": "🎉"},
                {"id": 2, "name": "crème brûlée", "emoji": "🚀"},
            ],
            "source2": [
                {"id": 1, "description": "café with 🎉"},
                {"id": 2, "description": "crème brûlée 🚀"},
            ],
        }

        result = await merge_loader(data_dict, target="id")

        assert len(result) == 2
        assert result[0]["name"] == " café"
        assert result[0]["emoji"] == "🎉"
        assert result[0]["description"] == "café with 🎉"
        assert result[1]["name"] == "crème brûlée"

    # Test 20: Complex nested structures in values
    @pytest.mark.asyncio
    async def test_complex_nested_values(self):
        """Test merge with complex nested data structures as values."""
        data_dict = {
            "source1": [
                {"id": 1, "metadata": {"tags": ["python", "async"], "priority": 1}}
            ],
            "source2": [
                {
                    "id": 1,
                    "metadata": {"status": "active", "priority": 2},
                    "history": [{"date": "2023-01-01", "action": "create"}],
                }
            ],
        }

        result = await merge_loader(data_dict, target="id", merge_strategy="aggregate")

        # Metadata field should be aggregated
        assert isinstance(result[0]["metadata"], list)
        assert len(result[0]["metadata"]) == 2
        assert "history" in result[0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
