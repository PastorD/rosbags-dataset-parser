
### Data Formats

#### Raw Data Formats

The raw data indicates the data as it is loaded from the rosbag files, without any processing or filtering. Use this format when you need the original data for analysis or processing. There are multiple formats to represent the raw data, each with its own advantages and use cases:

- Nested Raw Data: The structure is dictionary where the key is the experiment name, and the value is the experiment data. This data is another dictionary where the key is the topic name and the value is the topic data. The topic data is dictionary where each field is a key, and the value is a timeseries, including time. Use property `raw_nested` to get the data in this format. For example, for the experiment `2025-03-10-06-18-09`, and `2025-03-10-06-18-10` the raw data structure will be:


```python
{
    '2025-03-10-06-18-09': {
        '/vehicle_inputs/controller': 
        [
            'time': np.ndarray,
            'acc_pedal_cmd': np.ndarray,
            'f_brake_pressure_cmd': np.ndarray,
            'steering_motor_ang_cmd': np.ndarray,
        ]
        '/state/odom': 
        [
            'time': np.ndarray
            'twist.twist.linear.x': np.ndarray,
            'twist.twist.linear.y': np.ndarray, 
            'twist.twist.angular.z': np.ndarray,
        ]
    },
    '2025-03-10-06-18-10': {
        ...
    },
}
```

This is the basic data format that all other data formats are derived from.

- Flat Raw Data: same as with data data structure, but the fields are flattened with the topic name. Use property `raw_flat` to get the data in this format. For example, for the experiment `2025-03-10-06-18-09`, and `2025-03-10-06-18-10` the raw data structure will be:

```python
{
    '2025-03-10-06-18-09': 
    [
        '/vehicle_inputs/controller.time': np.ndarray, 
        '/vehicle_inputs/controller.acc_pedal_cmd': np.ndarray, 
        '/vehicle_inputs/controller.f_brake_pressure_cmd':np.ndarray, 
        '/vehicle_inputs/controller.steering_motor_ang_cmd':np.ndarray,
        '/state/odom.time':np.ndarray,
        '/state/odom.twist.twist.linear.x':np.ndarray, 
        '/state/odom.twist.twist.linear.y':np.ndarray, 
        '/state/odom.twist.twist.angular.z':np.ndarray,
    ],
    '2025-03-10-06-18-10': [
        ...
    ],
}
```


- Dataframe Raw Data: same as wtih raw data structure, but the topic data is a pandas data frame. Use property `raw_dataframe` to get the data in this format. For example, for the experiment `2025-03-10-06-18-09`, and `2025-03-10-06-18-10` the raw data structure will be:

```python
{
    '2025-03-10-06-18-09': {
        '/vehicle_inputs/controller': pd.DataFrame(columns=
            [
                'time',
                'acc_pedal_cmd', 
                'f_brake_pressure_cmd', 
                'steering_motor_ang_cmd'
            ]
        ),
        '/state/odom': pd.DataFrame(columns=
            [
                'time',
                'twist.twist.linear.x', 
                'twist.twist.linear.y', 
                'twist.twist.angular.z'
            ]
        ),
    },
    '2025-03-10-06-18-10': {
        ...
    },
}
```

- Combined per topic Dataframe Raw Data: same as with raw data structure, but the data is a single dataframe with all data, and a new column `experiment_name` to identify the experiment. Use property `raw_combined` to get the data in this format. For example, for the experiments `2025-03-10-06-18-09` and `2025-03-10-06-18-10`, the raw data structure will be:

```python
{
    '/vehicle_inputs/controller': pd.DataFrame(columns=
        [
            'experiment_name',
            'time',
            'acc_pedal_cmd', 
            'f_brake_pressure_cmd', 
            'steering_motor_ang_cmd'
        ]
    ),
    '/state/odom': pd.DataFrame(columns=
        [
            'experiment_name',
            'time',
            'twist.twist.linear.x', 
            'twist.twist.linear.y', 
            'twist.twist.angular.z'
        ]
    ),
}
```
#### Interpolated Data Formats

The interpolated data indicates the data after processing and interpolation to a common time vector. Use this format when you need to compare multiple topics or experiments. 

- Dataframe per run: Each field is interpolated to a common time. The output is a pandas dataframe where each row is a list of values with all fields, including time. Use property `interpolated_<time format>`, where `<time format>` can be `common`, `gps`, or `track`. For example, for the experiment `2025-03-10-06-18-09`, the interpolated data structure will be:
```python
{
    '2025-03-10-06-18-09': pd.DataFrame(columns=
        [
            'time',
            'acc_pedal_cmd', 
            'f_brake_pressure_cmd', 
            'steering_motor_ang_cmd',
            'twist.twist.linear.x', 
            'twist.twist.linear.y', 
            'twist.twist.angular.z'
        ]
    ),
    '2025-03-10-06-18-10': pd.DataFrame(columns=
        [
            'time',
            'acc_pedal_cmd', 
            'f_brake_pressure_cmd', 
            'steering_motor_ang_cmd',
            'twist.twist.linear.x', 
            'twist.twist.linear.y', 
            'twist.twist.angular.z'
        ]
    ),
}
```

- Combined Dataframe: a single dataframe with all data from all experiments, and a new column `experiment_name` to identify the experiment. Use property `interpolated_<time format>_combined`. For example, for the experiments `2025-03-10-06-18-09` and `2025-03-10-06-18-10`, the interpolated data structure will be:

```python
pd.DataFrame(columns=
    [
        'experiment_name',
        'time',
        'acc_pedal_cmd', 
        'f_brake_pressure_cmd', 
        'steering_motor_ang_cmd',
        'twist.twist.linear.x', 
        'twist.twist.linear.y', 
        'twist.twist.angular.z'
    ]
)
```