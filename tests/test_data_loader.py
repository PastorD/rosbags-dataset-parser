import pytest
import numpy as np
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from rosbags_parser.run_loader import RosbagDataLoader, get_nested_attr


class TestGetNestedAttr:
    """Tests for the get_nested_attr helper function."""

    def test_simple_attribute(self):
        obj = Mock()
        obj.field = 42
        assert get_nested_attr(obj, "field") == 42

    def test_nested_attribute(self):
        obj = Mock()
        obj.parent.child = "value"
        assert get_nested_attr(obj, "parent.child") == "value"

    def test_deeply_nested_attribute(self):
        obj = Mock()
        obj.a.b.c.d = 100
        assert get_nested_attr(obj, "a.b.c.d") == 100

    def test_list_index_access(self):
        obj = Mock()
        obj.items = [10, 20, 30]
        assert get_nested_attr(obj, "items[1]") == 20

    def test_list_all_elements(self):
        obj = Mock()
        obj.items = [1, 2, 3]
        assert get_nested_attr(obj, "items[:]") == [1, 2, 3]

    def test_nested_with_list_index(self):
        obj = Mock()
        obj.data = [Mock(value=5), Mock(value=10)]
        result = get_nested_attr(obj, "data[0].value")
        assert result == 5

    def test_list_attribute_access_on_all_elements(self):
        obj = Mock()
        item1 = Mock(name="a")
        item2 = Mock(name="b")
        obj.items = [item1, item2]
        result = get_nested_attr(obj, "items[:].name")
        assert result == ["a", "b"]


class TestRosbagDataLoaderInit:
    """Tests for RosbagDataLoader initialization."""

    def test_init_with_valid_params(self):
        rosbags = [Path("/path/to/bag1"), Path("/path/to/bag2")]
        topics = {"/topic1": ["field1", "field2"]}
        
        loader = RosbagDataLoader(rosbags, topics)
        
        assert loader.rosbags == rosbags
        assert loader.topics == topics
        assert loader.data_dict is None

    def test_init_with_empty_rosbags(self):
        loader = RosbagDataLoader([], {})
        
        assert loader.rosbags == []
        assert loader.topics == {}


class TestRosbagDataLoaderHelpers:
    """Tests for RosbagDataLoader helper methods."""

    def test_make_topic_field_dict_flat(self):
        loader = RosbagDataLoader([], {})
        data_dict = {
            "/topic1": {
                "time": [1.0, 2.0, 3.0],
                "field1": [10, 20, 30],
                "field2": [100, 200, 300],
            },
            "/topic2": {
                "time": [1.0, 2.0],
                "value": [5, 6],
            },
        }
        
        result = loader.make_topic_field_dict_flat(data_dict)
        
        assert "/topic1.field1" in result
        assert "/topic1.field2" in result
        assert "/topic2.value" in result
        assert result["/topic1.field1"] == [10, 20, 30]
        assert result["/topic2.value"] == [5, 6]
        # time should not be included
        assert "/topic1.time" not in result
        assert "/topic2.time" not in result

    def test_get_common_time(self):
        loader = RosbagDataLoader([], {})
        data_dict = {
            "/topic1": {"time": [0.0, 0.5, 1.0, 1.5, 2.0]},
            "/topic2": {"time": [0.5, 1.0, 1.5]},
        }
        
        common_time = loader._get_common_time(data_dict, timestep=0.25)
        
        # Should start at max(0.0, 0.5) = 0.5
        # Should end at min(2.0, 1.5) = 1.5
        assert common_time[0] == 0.5
        assert common_time[-1] < 1.5
        assert np.allclose(np.diff(common_time), 0.25)

    def test_interpolate_given_time(self):
        loader = RosbagDataLoader([], {})
        data_dict = {
            "/topic1": {
                "time": [0.0, 1.0, 2.0],
                "value": [0.0, 10.0, 20.0],
            },
        }
        desired_time = np.array([0.5, 1.5])
        
        result = loader._interpolate_given_time(data_dict, desired_time)
        
        assert np.array_equal(result["time"], desired_time)
        assert np.allclose(result["/topic1.value"], [5.0, 15.0])

    def test_create_dataframe_from_interpolated_data(self):
        loader = RosbagDataLoader([], {})
        interpolated_data = {
            "time": np.array([1.0, 2.0, 3.0]),
            "/topic.field": np.array([10, 20, 30]),
        }
        
        df = loader._create_dataframe_from_interpolated_data(interpolated_data)
        
        assert isinstance(df, pd.DataFrame)
        assert "time" in df.columns
        assert "/topic.field" in df.columns
        assert len(df) == 3


class TestRosbagDataLoaderWithMockedRosbags:
    """Tests that require mocking rosbag reading."""

    @patch("rosbags_parser.data_loader.AnyReader")
    @patch("rosbags_parser.data_loader.get_typestore")
    def test_get_rosbag_data_no_connections(self, mock_get_typestore, mock_reader):
        mock_reader_instance = MagicMock()
        mock_reader_instance.connections = []
        mock_reader_instance.__enter__ = Mock(return_value=mock_reader_instance)
        mock_reader_instance.__exit__ = Mock(return_value=False)
        mock_reader.return_value = mock_reader_instance
        
        loader = RosbagDataLoader([Path("/fake/bag")], {"/topic": ["field"]})
        result = loader.get_rosbag_data([Path("/fake/bag")], {"/topic": ["field"]})
        
        # Should return empty data structure when no connections found
        assert result == {"/topic": {"time": [], "field": []}}

    @patch("rosbags_parser.data_loader.AnyReader")
    @patch("rosbags_parser.data_loader.get_typestore")
    def test_load_data_returns_false_on_error(self, mock_get_typestore, mock_reader):
        mock_reader.side_effect = Exception("Failed to read")
        
        loader = RosbagDataLoader([Path("/fake/bag")], {"/topic": ["field"]})
        result = loader.load_data()
        
        assert result is False

    def test_get_interpolated_common_time_dataframe_without_data(self):
        loader = RosbagDataLoader([], {})
        loader.data_dict = {
            "/topic": {
                "time": [0.0, 0.1, 0.2, 0.3],
                "value": [0.0, 1.0, 2.0, 3.0],
            }
        }
        
        df = loader.get_interpolated_common_time_dataframe(timestep=0.05)
        
        assert df is not None
        assert isinstance(df, pd.DataFrame)
        assert "time" in df.columns
        assert "datetime" in df.columns
        assert "/topic.value" in df.columns

    def test_get_dict_dataframes_with_preloaded_data(self):
        loader = RosbagDataLoader([], {})
        loader.data_dict = {
            "/topic1": {"time": [1.0, 2.0], "field": [10, 20]},
            "/topic2": {"time": [1.0], "value": [100]},
        }
        
        result = loader.get_dict_dataframes()
        
        assert isinstance(result, dict)
        assert "/topic1" in result
        assert "/topic2" in result
        assert isinstance(result["/topic1"], pd.DataFrame)
        assert len(result["/topic1"]) == 2

    def test_get_flat_dict_with_preloaded_data(self):
        loader = RosbagDataLoader([], {})
        loader.data_dict = {
            "/topic": {"time": [1.0, 2.0], "field": [10, 20]}
        }
        
        result = loader.get_flat_dict()
        
        assert "/topic.field" in result
        assert result["/topic.field"] == [10, 20]

    def test_get_data_dict_with_preloaded_data(self):
        loader = RosbagDataLoader([], {})
        loader.data_dict = {
            "/topic": {"time": [1.0], "field": [10]}
        }
        
        result = loader.get_data_dict()
        
        assert result == loader.data_dict


class TestRosbagDataLoaderEdgeCases:
    """Edge case tests."""

    def test_interpolate_with_single_topic(self):
        loader = RosbagDataLoader([], {})
        data_dict = {
            "/single": {
                "time": [0.0, 1.0, 2.0, 3.0, 4.0],
                "data": [0.0, 2.0, 4.0, 6.0, 8.0],
            }
        }
        
        result = loader._interpolate_common_time(data_dict, timestep=0.5)
        
        assert "time" in result
        assert "/single.data" in result

    def test_empty_data_dict_handling(self):
        loader = RosbagDataLoader([], {})
        loader.data_dict = {}
        
        result = loader.get_flat_dict()
        
        assert result == {}