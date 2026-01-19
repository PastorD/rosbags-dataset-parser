from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table
from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore
from tqdm import tqdm

console = Console()

class RunLoader():
    def __init__(self, rosbags: list[Path], topics: dict[str, list[str]]) -> None:
        self.rosbags: list[Path] = rosbags
        self.topics: dict[str, list[str]] = topics
        self.data_dict: dict[str, dict[str, list[float]]] | None = None

    def check_rosbags(self) -> None:
        pass

    def find_topics_in_rosbags(self) -> None:
        # find which topics are in which rosbags
        topics_locations: dict[str, dict[Path, bool]] = dict()
        for topic_name, topic_contents in self.topics.items():
            topics_locations[topic_name] = dict()
            for rosbag in self.rosbags:
                print(f"Checking rosbag {rosbag}")
                typestore = get_typestore(Stores.ROS2_HUMBLE)
                with AnyReader([rosbag], default_typestore=typestore) as reader:
                    topics_locations[topic_name][rosbag] = False
                    connections = [x for x in reader.connections if x.topic == topic_name]
                    if len(connections) == 0:
                        print(f" - Topic {topic_name} not found in rosbag {rosbag}")
                        continue
                    connection = connections[0]
                    if connection.msgcount == 0:
                        print(f" - Topic {topic_name} has no messages in rosbag {rosbag}")
                        continue
                    topics_locations[topic_name][rosbag] = True
                    print(f"Found topic {topic_name} in rosbag {rosbag}")
                    print(f"Fields in topic {topic_name}: {connection.msgtype.fields}")
                    for field in topic_contents:
                        if field not in connection.msgtype.fields:
                            print(f"Field {field} not found in topic {topic_name}")
                        else:
                            print(f"Field {field} found in topic {topic_name}")

        self.print_topics_in_rosbags(topics_locations)

    def print_topics_in_rosbags(self, topics_locations: dict[str, dict[Path, bool]]) -> None:
        # print a table of which topics are in which rosbags
        table = Table(title="Topics in rosbags")
        table.add_column("Topic")
        for rosbag in self.rosbags:
            table.add_column(rosbag.name)
        for topic_name, topic_contents in topics_locations.items():
            row: list[str] = [topic_name]
            for rosbag in self.rosbags:
                row.append("X" if topic_contents[rosbag] else "")
            table.add_row(*row)
        console = Console()
        console.print(table)


    def get_rosbag_data(self, rosbags: list[Path], topics: dict[str, list[str]]) -> dict[str, dict[str, list[float]]] | None:
        data: dict[str, dict[str, list[float]]] = dict()
        for topic_name, topic_contents in topics.items():
            data[topic_name] = {"time": []}
            for field in topic_contents:
                data[topic_name][field] = []
        # Create a type store to use if the bag has no message definitions.
        typestore = get_typestore(Stores.ROS2_HUMBLE)
        # Create reader instance and open for reading.
        try:
            with AnyReader(rosbags, default_typestore=typestore) as reader:
                connections = [x for x in reader.connections if x.topic in topics]
                if len(connections) == 0:
                    print(f"No connections found for topics {topics}")
                    return data
                # check every topic has a connection with messages in the rosbag
                for topic in topics:
                    if topic not in [connection.topic for connection in connections]:
                        console.print(f"Topic {topic} not found in rosbags", style="red")
                        return None
                    connection_topic = [connection for connection in connections if connection.topic == topic][0]
                    if connection_topic.msgcount == 0:
                        console.print(f"Topic {topic} has no messages in rosbags", style="red")
                        return None
                nmsgs = sum([connection.msgcount for connection in connections])
                for connection, timestamp, rawdata in tqdm(reader.messages(connections=connections), total=nmsgs):
                    msg = reader.deserialize(rawdata, connection.msgtype)
                    data[connection.topic]["time"].append(float(timestamp/1e9)) # convert to seconds
                    # check if there is a header.stamp field in the msg
                    if hasattr(msg, "header") and hasattr(msg.header, "stamp"):
                        if "header_time" not in data[connection.topic]:
                            data[connection.topic]["header_time"] = []
                        data[connection.topic]["header_time"].append(float(msg.header.stamp.sec) + float(msg.header.stamp.nanosec)/1e9)
                    for field in topics[connection.topic]:
                        data[connection.topic][field].append(get_nested_attr(msg, field))
        except Exception as e:
            console.print(f"Error reading rosbags: {e}", style="red")
            return None
        return data
    
    def make_topic_field_dict_flat(self, data_dict: dict[str, dict[str, list[float]]]) -> dict[str, list[float]]:
        """
        Convert the data dictionary where each topic has fields inside to a flat dictionary, 
        where each key is the topic/field and the value is the list of data. This includes the time field.
        """
        flat_data: dict[str, list[float]] = dict()
        for topic in data_dict:
            for field in data_dict[topic]:
                if field == "time":
                    continue
                topic_field_name = f"{topic}.{field}"
                flat_data[topic_field_name] = data_dict[topic][field]
        return flat_data
    
    def _interpolate_given_time(self, data_dict: dict[str, dict[str, list[float]]], desired_time: np.ndarray, time_name: str = "time") -> dict[str, np.ndarray]:
        interpolated_data: dict[str, np.ndarray] = {"time": desired_time}
        for topic_name, topic_content in data_dict.items():
            topic_time = topic_content[time_name]
            for field_name, field_data in topic_content.items():
                if field_name == "time" or field_name == "header_time":
                    continue
                topic_field_name = f"{topic_name}.{field_name}"
                interpolated_data[topic_field_name] = np.interp(desired_time, topic_time, field_data)
        return interpolated_data
    
    def _get_common_time(self, data_dict: dict[str, dict[str, list[float]]], timestep: float, time_name: str = "time") -> np.ndarray:
        # get all the start times
        start_times: list[float] = [data_dict[topic][time_name][0] for topic in data_dict]
        start_time: float = max(start_times)
        # get all the end times
        end_times: list[float] = [data_dict[topic][time_name][-1] for topic in data_dict]
        end_time: float = min(end_times)
        # print(f"Total common time: {end_time - start_time}")

        # get the common time
        common_time: np.ndarray = np.arange(start_time, end_time, timestep)
        return common_time

    def _interpolate_common_time(self, data_dict: dict[str, dict[str, list[float]]], timestep: float = 0.050, desired_time: np.ndarray | None = None) -> dict[str, np.ndarray]:
        # Get the common time
        common_time: np.ndarray = self._get_common_time(data_dict, timestep)
        return self._interpolate_given_time(data_dict, common_time)
    
    def _create_dataframe_from_interpolated_data(self, interpolated_data: dict[str, np.ndarray]) -> pd.DataFrame:
        return pd.DataFrame(interpolated_data)
    
    def check_print_topics(self) -> None:
        topics_locations = self._check_topics(self.topics)
        self._print_topics_existence(topics_locations)

    def _check_topics(self, topics: dict[str, list[str]]) -> dict[str, str]:
        # check if the topics are in the rosbags
        topics_locations: dict[str, str] = dict()
        typestore = get_typestore(Stores.ROS2_HUMBLE)
        with AnyReader(self.rosbags, default_typestore=typestore) as reader:
            for topic in topics:
                connections = [x for x in reader.connections if x.topic == topic]
                if len(connections) == 0:
                    print(f"Topic {topic} not found in rosbags")
                    topics_locations[topic] = "not found"
                    continue
                connection = connections[0]
                if connection.msgcount == 0:
                    print(f"Topic {topic} has no messages in rosbags")
                    topics_locations[topic] = "no messages"
                    continue
                print(f"Found topic {topic} in rosbags")
                topics_locations[topic] = "found"
        return topics_locations

    def _print_topics_existence(self, topics_locations: dict[str, str]) -> None:
        # print a table of which topics are in the rosbags
        table = Table(title="Topics in rosbags")
        table.add_column("Topic")
        table.add_column("Status")
        for topic, status in topics_locations.items():
            table.add_row(topic, status)
        console = Console()
        console.print(table)

    def load_data(self) -> bool:
        self.data_dict = self.get_rosbag_data(self.rosbags, self.topics)
        if self.data_dict is None:
            return False
        else:
            return True

    def get_interpolated_common_time_dataframe(self, timestep: float = 0.050) -> pd.DataFrame | None:
        if self.data_dict is None:
            self.data_dict = self.get_rosbag_data(self.rosbags, self.topics)
            if self.data_dict is None:
                return None
        interpolated_data = self._interpolate_common_time(self.data_dict, timestep)
        dataframe = self._create_dataframe_from_interpolated_data(interpolated_data)
        # add datetime column
        dataframe["datetime"] = pd.to_datetime(dataframe["time"], unit="s")
        return dataframe
    
    def get_interpolated_given_time_dataframe(self, time_to_interpolate: np.ndarray) -> pd.DataFrame | None:
        if self.data_dict is None:
            self.data_dict = self.get_rosbag_data(self.rosbags, self.topics)
            if self.data_dict is None:
                return None
        interpolated_data = self._interpolate_given_time(self.data_dict, time_to_interpolate)
        dataframe = self._create_dataframe_from_interpolated_data(interpolated_data)
        # add datetime column
        dataframe["datetime"] = pd.to_datetime(dataframe["time"], unit="s")
        return dataframe

    
    def get_dict_dataframes(self) -> dict[str, pd.DataFrame] | None:
        if self.data_dict is None:
            self.data_dict = self.get_rosbag_data(self.rosbags, self.topics)
            if self.data_dict is None:
                return None
        return {topic: pd.DataFrame(data) for topic, data in self.data_dict.items()}
    
    def get_flat_dict(self) -> dict[str, list[float]] | None:
        if self.data_dict is None:
            self.data_dict = self.get_rosbag_data(self.rosbags, self.topics)
            if self.data_dict is None:
                return None
        return self.make_topic_field_dict_flat(self.data_dict)
    
    def get_data_dict(self) -> dict[str, dict[str, list[float]]] | None:
        if self.data_dict is None:
            self.data_dict = self.get_rosbag_data(self.rosbags, self.topics)
            if self.data_dict is None:
                return None
        return self.data_dict
    
    def save_to_hdf5(self) -> None:
        pass

    def discover_topics_and_fields(self, max_depth: int = 5) -> dict[str, list[str]]:
        """
        Discover all topics in the rosbags and list all possible fields for each topic.
        Expands nested message types using dot notation (e.g., pose.position.x).

        Args:
            max_depth: Maximum depth to expand nested fields.

        Returns:
            Dictionary mapping topic names to lists of field paths.
        """
        topics_fields: dict[str, list[str]] = {}
        typestore = get_typestore(Stores.ROS2_HUMBLE)

        with AnyReader(self.rosbags, default_typestore=typestore) as reader:
            for connection in reader.connections:
                topic_name = connection.topic
                msgtype = connection.msgtype

                # Get the message class from typestore
                try:
                    msg_class = typestore.types[msgtype]
                    fields = self._expand_message_fields(msg_class, typestore, max_depth)
                    topics_fields[topic_name] = fields
                except KeyError:
                    console.print(f"[yellow]Warning: Could not find type {msgtype}[/yellow]")
                    topics_fields[topic_name] = []

        return topics_fields

    def _expand_message_fields(
        self,
        msg_class: type,
        typestore: Any,
        max_depth: int,
        current_depth: int = 0,
        prefix: str = ""
    ) -> list[str]:
        """
        Recursively expand message fields using dot notation.

        Args:
            msg_class: The message class to expand.
            typestore: The rosbags typestore.
            max_depth: Maximum recursion depth.
            current_depth: Current recursion depth.
            prefix: Current field path prefix.

        Returns:
            List of expanded field paths.
        """
        if current_depth >= max_depth:
            return [prefix.rstrip(".")] if prefix else []

        fields: list[str] = []

        # Get field annotations from the dataclass
        if not hasattr(msg_class, "__dataclass_fields__"):
            return [prefix.rstrip(".")] if prefix else []

        for field_name, field_info in msg_class.__dataclass_fields__.items():
            field_path = f"{prefix}{field_name}" if prefix else field_name
            field_type = field_info.type

            # Handle string type annotations
            if isinstance(field_type, str):
                field_type_str = field_type
            else:
                field_type_str = str(field_type)

            # Check if it's a primitive type
            if self._is_primitive_type(field_type_str):
                fields.append(field_path)
            # Check if it's an array type
            elif self._is_array_type(field_type_str):
                # Add array access patterns
                fields.append(f"{field_path}[:]")  # All elements
                fields.append(f"{field_path}[0]")  # First element example

                # Try to expand the element type
                element_type = self._get_array_element_type(field_type_str)
                if element_type and not self._is_primitive_type(element_type):
                    nested_class = self._get_type_class(element_type, typestore)
                    if nested_class:
                        nested_fields = self._expand_message_fields(
                            nested_class, typestore, max_depth,
                            current_depth + 1, f"{field_path}[:]."
                        )
                        fields.extend(nested_fields)
            else:
                # Try to expand as nested message
                nested_class = self._get_type_class(field_type_str, typestore)
                if nested_class:
                    nested_fields = self._expand_message_fields(
                        nested_class, typestore, max_depth,
                        current_depth + 1, f"{field_path}."
                    )
                    if nested_fields:
                        fields.extend(nested_fields)
                    else:
                        fields.append(field_path)
                else:
                    fields.append(field_path)

        return fields

    def _is_primitive_type(self, type_str: str) -> bool:
        """Check if a type string represents a primitive type."""
        primitives = {
            "int", "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "float", "float32", "float64", "double",
            "bool", "str", "string", "char",
            "byte", "octet",
            "numpy.float64", "numpy.float32",
            "numpy.int64", "numpy.int32", "numpy.int16", "numpy.int8",
            "numpy.uint64", "numpy.uint32", "numpy.uint16", "numpy.uint8",
        }
        # Clean up the type string
        clean_type = type_str.replace("'", "").replace("<class ", "").replace(">", "").strip()
        return any(p in clean_type.lower() for p in primitives)

    def _is_array_type(self, type_str: str) -> bool:
        """Check if a type string represents an array type."""
        array_indicators = ["list[", "sequence[", "array[", "ndarray", "numpy.ndarray"]
        return any(indicator in type_str.lower() for indicator in array_indicators)

    def _get_array_element_type(self, type_str: str) -> str | None:
        """Extract the element type from an array type string."""
        import re
        # Match patterns like list[SomeType], sequence<SomeType>, etc.
        patterns = [
            r"list\[([^\]]+)\]",
            r"sequence\[([^\]]+)\]",
            r"array\[([^\]]+)\]",
        ]
        for pattern in patterns:
            match = re.search(pattern, type_str, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None

    def _get_type_class(self, type_str: str, typestore: Any) -> type | None:
        """Get the class for a type string from the typestore."""
        # Clean up the type string
        clean_type = type_str.replace("'", "").strip()

        # Try direct lookup
        if clean_type in typestore.types:
            return typestore.types[clean_type]

        # Try with common ROS2 prefixes
        for prefix in ["", "builtin_interfaces/msg/", "std_msgs/msg/", "geometry_msgs/msg/", "sensor_msgs/msg/", "nav_msgs/msg/"]:
            full_name = f"{prefix}{clean_type}"
            if full_name in typestore.types:
                return typestore.types[full_name]

        return None

    def print_discovered_topics(self, max_depth: int = 5) -> None:
        """
        Discover and print all topics and their fields in a formatted table.

        Args:
            max_depth: Maximum depth to expand nested fields.
        """
        topics_fields = self.discover_topics_and_fields(max_depth)

        for topic_name, fields in topics_fields.items():
            table = Table(title=f"Topic: {topic_name}")
            table.add_column("Field Path", style="cyan")

            for field in sorted(fields):
                table.add_row(field)

            console.print(table)
            console.print()

    
def get_nested_attr(obj: Any, attr_string: str) -> Any:
    """
    Get a nested attribute from an object using dot notation.

    Supports accessing nested attributes and list indexing using bracket notation.
    For example: "pose.position.x" or "points[0].x" or "data[:]".

    Args:
        obj: The object to get the attribute from.
        attr_string: A dot-separated string representing the attribute path.
            Supports list indexing with [index] or [:] for all elements.

    Returns:
        The value of the nested attribute. If [:] is used, returns a list of values.

    Examples:
        >>> get_nested_attr(msg, "header.stamp.sec")
        >>> get_nested_attr(msg, "points[0].x")
        >>> get_nested_attr(msg, "data[:]")
    """
    attrs: list[str] = attr_string.split(".")
    for attr in attrs:
        if "[" in attr and "]" in attr:  # Check if the attribute includes list indexing
            attr_name, index = attr[:-1].split("[")  # Split into attribute name and index
            obj = getattr(obj, attr_name)  # Get the list
            if index == ":":  # Include all elements as a list
                obj = list(obj)
            else:
                obj = obj[int(index)]  # Access the specific index
        else:
            if isinstance(obj, list):
                obj = [getattr(item, attr) for item in obj]  # Apply to each element if obj is a list
            else:
                obj = getattr(obj, attr)  # Regular attribute access
    return obj