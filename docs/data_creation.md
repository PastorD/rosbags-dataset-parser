
# Data Creation and Organization

When creating and organizing data from rosbag recordings, it is important to follow a consistent structure to facilitate data loading and processing. Below are some recommended practices for organizing rosbag data for both single-run and multi-run scenarios.

## Single-Run Data Organization

- Directly a rosbag directory:
```bash
run_name/
├── run_name.mcap
└── metadata.yaml
```
where run_name is the name of the experiment/run, by default is is `rosbag2_YYYY-MM-DD-HH-MM-SS_0.mcap`, such as `rosbag2_2025-01-04-10-08-53_0.mcap`, but it can be specified with the `-o` option when recording the rosbag. Rosbags can be splitted in multiple files using the `--max-bag-size` option. The directory will contain multiple files such as:
```bash
run_name/
├── run_name_0.mcap
├── run_name_1.mcap
├── run_name_2.mcap
└── metadata.yaml
```

Multiple rosbags for a single run: in case multiple rosbags are recorded for a single run, e.g., to separate different sensors or data sources, the structure can be:
```bash
run_name/
├── sensors/
│   ├── sensors_0.mcap
│   ├── sensors_1.mcap
│   └── metadata.yaml
└── control/
    ├── control_0.mcap
    ├── control_1.mcap
    └── metadata.yaml
```

Sometimes it is convenient to move all the rosbags of a run into a single folder, e.g. "rosbags", and have additional folders lik configuration files or derived data:
```bash
run_name/
├── rosbags/
│   ├── sensors/
│   │   ├── sensors_0.mcap
│   │   ├── sensors_1.mcap
│   │   └── metadata.yaml
│   └── control/
│       ├── control_0.mcap
│       ├── control_1.mcap
│       └── metadata.yaml
├── config/
│   └── ...
└── derived_data/
    └── ...
```


## Multi-Run Data Organization

The simplest way to organize multiple runs is to have a parent folder containing multiple run folders:
```bash
all_runs/
├── run_name_1/
│   └── ...
├── run_name_2/
│   └── ...
└── ...
```
where each `run_name_X` folder follows one of the single-run structures described above. It is common to organize runs by date and field trip/compaign name, for example:
```bash
project_name/
├── 2025-May/
│   ├── field_trip_1/
│   │   ├── run_name_1/
│   │   │   └── ...
│   │   ├── run_name_2/
│   │   │   └── ...
│   │   └── ...
│   └── field_trip_2/
│       └── ...
├── 2025-June/
│   └── ...
└── ...
```
Rosbags-parsers can recursively search for run folders within a list of root folders.

Another common pattern is to extract the map file of each run into a separate folder for easier access. For example:
```bash
project_name/
├── run_01.mcap
├── run_02.mcap
├── run_03.mcap
└── ...
```
This way we can have all the map files in a single folder for easy loading and processing.

About run_names: the only requirement is to have an unique name per run folder. By default, rosbags-parser will look for folders matching the pattern `[prefix_]YYYY-MM-DD-HH-MM-SS[_suffix]`, but this can be customized in the `MultiRunLoader` class by modifying the regex pattern used to identify run folders. This includes the default pattern used by rosbag2 when recording, and it allows to add prefixes or suffixes to the run folder names as needed. A common usage is to use the preffix to denote the robot name, and the suffix to denote the test name. The test name can be further decomposed into a test ID and a description, e.g., `robot1_2025-01-04-10-08-53_obstacle_course_01`.
