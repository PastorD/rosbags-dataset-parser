from abc import ABC, abstractmethod
from pathlib import Path
import pandas as pd

import re

class DatasetLoader():
    """Abstract base class for handling multiple run folders containing rosbags."""

    def __init__(self):
        pass

    def get_run_folders(self, root_folders: list[Path]) -> list[Path]:
        """
        Traverse the given root folders and return a list of run folders.

        Args:
            root_folders: List of root folders to traverse.

        Returns:
            List of paths to run folders.
        """
        # search in the data_folders for folders that has format  
        # 
        # e.g. for YYYY-MM-DD-HH-MM-SS, we use  
        # r'\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}$' that matches
        # 2025-01-04-10-08-53 is 4th of January 2025 at 10:08:53
        # 
        # Input is a list of paths to the root folders. Each root path will be travresed to find the experiment folders
        # Output is a list of paths to the experiment folders

        # search for the folders and subfolders
        # date_regex = re.compile(r'\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}$')
        # regex matches: preffix_YYYY-MM-DD-HH-MM-SS_suffix, but also 
        # YYYY-MM-DD-HH-MM-SS_suffix, preffix_YYYY-MM-DD-HH-MM-SS, YYYY-MM-DD-HH-MM-SS

        date_regex = re.compile(r'(.*_)?\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}(_.*)?$')
        experiments_folders = []
        for root_path in root_folders:
            # check the root path itself
            if root_path.is_dir() and date_regex.fullmatch(root_path.name):
                experiments_folders.append(root_path)
                continue
            # check the subfolders
            for path in root_path.rglob('*'):
                if path.is_dir() and date_regex.fullmatch(path.name):
                    experiments_folders.append(path)
        return experiments_folders
    


    def get_rosbags_in_run(self, run_folder: Path, include_filters: list[str] | None = None, exclude_filters: list[str] | None = None) -> list[Path]:
        """
        Get all rosbag files within a given run folder.

        Args:
            run_folder: Path to the run folder.
            include_filters: List of patterns to include (rosbag must match at least one).
            exclude_filters: List of patterns to exclude (rosbag must not match any).

        Returns:
            List of paths to rosbag files.
        """
        rosbags = []
        for path in run_folder.rglob('*'):
            if path.is_dir() and path.suffix == '.mcap' or path.name == 'metadata.yaml':
                # Check for ROS2 bag format (directory with metadata.yaml)
                rosbag_path = path.parent if path.name == 'metadata.yaml' else path
            # elif path.is_file() and path.suffix == '.mcap':
                # Check for ROS1 bag format (.bag file)
                # rosbag_path = path
            else:
                continue

            rosbag_name = rosbag_path.name

            # Apply include filters (must match at least one if filters are provided)
            if include_filters:
                if not any(re.search(pattern, rosbag_name) for pattern in include_filters):
                    continue

            # Apply exclude filters (must not match any)
            if exclude_filters:
                if any(re.search(pattern, rosbag_name) for pattern in exclude_filters):
                    continue

            if rosbag_path not in rosbags:
                rosbags.append(rosbag_path)

        return rosbags

    def get_combined_interpolated_common_time(self, timestep = 0.05) -> pd.DataFrame:
        experiment_dict_common_df = {name: loader.get_interpolated_common_time_dataframe(timestep) for name, loader in self.experiment_data_loaders_dict.items()}
        return self.combine_experiment_dataframes(experiment_dict_common_df)
    
    def get_combined_interpolated_topic_time(self, topic: str) -> pd.DataFrame:
        experiment_dict_topic_df = {}
        for experiment_folder, loader in self.experiment_data_loaders_dict.items():
            time_filtered_topic = loader.get_dict_dataframes()[topic]["time"]
            experiment_dict_topic_df[experiment_folder] = loader.get_interpolated_given_time_dataframe(time_filtered_topic)
            
        return self.combine_experiment_dataframes(experiment_dict_topic_df)
    
    def get_raw_expdict_dataframes(self) -> dict[str, pd.DataFrame]:
        experiment_dict_common_df = {name: loader.get_dict_dataframes() for name, loader in self.experiment_data_loaders_dict.items()}
        return experiment_dict_common_df
    
    def get_raw_flat_dict(self) -> dict[str, pd.DataFrame]:
        experiment_dict_common_df = {name: loader.get_flat_dict() for name, loader in self.experiment_data_loaders_dict.items()}
        return experiment_dict_common_df
    
    def get_raw_data_dict(self) -> dict[str, pd.DataFrame]:
        experiment_dict_common_df = {name: loader.get_data_dict() for name, loader in self.experiment_data_loaders_dict.items()}
        return experiment_dict_common_df

    def save_to_csv(self, df: pd.DataFrame, output_path: Path | str) -> None:
        """
        Save a DataFrame to a CSV file.

        Args:
            df: DataFrame to save.
            output_path: Path to the output CSV file.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)

    def save_to_parquet(self, df: pd.DataFrame, output_path: Path | str) -> None:
        """
        Save a DataFrame to a Parquet file.

        Args:
            df: DataFrame to save.
            output_path: Path to the output Parquet file.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)

    def save_to_hdf5(self, df: pd.DataFrame, output_path: Path | str, key: str = "data") -> None:
        """
        Save a DataFrame to an HDF5 file.

        Args:
            df: DataFrame to save.
            output_path: Path to the output HDF5 file.
            key: Key/group name under which to store the data in the HDF5 file.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_hdf(output_path, key=key, mode="w")

    def save_combined_data(
        self,
        output_dir: Path | str,
        filename: str = "combined_data",
        formats: list[str] | None = None,
        timestep: float = 0.05,
        hdf5_key: str = "data",
    ) -> dict[str, Path]:
        """
        Save combined interpolated data to multiple formats.

        Args:
            output_dir: Directory to save the output files.
            filename: Base filename (without extension).
            formats: List of formats to save. Options: 'csv', 'parquet', 'hdf5'.
                     If None, saves to all formats.
            timestep: Timestep for interpolation.
            hdf5_key: Key/group name for HDF5 storage.

        Returns:
            Dictionary mapping format names to output file paths.
        """
        if formats is None:
            formats = ["csv", "parquet", "hdf5"]

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        df = self.get_combined_interpolated_common_time(timestep)
        saved_files = {}

        if "csv" in formats:
            csv_path = output_dir / f"{filename}.csv"
            self.save_to_csv(df, csv_path)
            saved_files["csv"] = csv_path

        if "parquet" in formats:
            parquet_path = output_dir / f"{filename}.parquet"
            self.save_to_parquet(df, parquet_path)
            saved_files["parquet"] = parquet_path

        if "hdf5" in formats:
            hdf5_path = output_dir / f"{filename}.h5"
            self.save_to_hdf5(df, hdf5_path, key=hdf5_key)
            saved_files["hdf5"] = hdf5_path

        return saved_files