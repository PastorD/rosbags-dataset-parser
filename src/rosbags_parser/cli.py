"""Command-line interface for rosbags-parser."""

from pathlib import Path

import click
from rich.console import Console

from rosbags_parser.dataset_loader import DatasetLoader
from rosbags_parser.run_loader import RunLoader

console = Console()


@click.group()
@click.version_option()
def cli():
    """Rosbags Parser - Process and convert rosbag data to various formats."""
    pass


@cli.command()
@click.argument("root_folders", nargs=-1, type=click.Path(exists=True, path_type=Path), required=True)
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    required=True,
    help="Output file path (extension determines format if --format not specified).",
)
@click.option(
    "--format", "-f",
    "formats",
    type=click.Choice(["csv", "parquet", "hdf5"], case_sensitive=False),
    multiple=True,
    default=["parquet"],
    help="Output format(s). Can be specified multiple times. Default: parquet.",
)
@click.option(
    "--timestep", "-t",
    type=float,
    default=0.05,
    show_default=True,
    help="Timestep for interpolation in seconds.",
)
@click.option(
    "--include", "-i",
    "include_filters",
    multiple=True,
    help="Include filter pattern (regex). Rosbag must match at least one.",
)
@click.option(
    "--exclude", "-e",
    "exclude_filters",
    multiple=True,
    help="Exclude filter pattern (regex). Rosbag must not match any.",
)
@click.option(
    "--topics-file",
    type=click.Path(exists=True, path_type=Path),
    help="YAML file specifying topics and fields to extract.",
)
@click.option(
    "--filename",
    type=str,
    default="combined_data",
    show_default=True,
    help="Base filename for output files (without extension).",
)
@click.option(
    "--hdf5-key",
    type=str,
    default="data",
    show_default=True,
    help="Key/group name for HDF5 storage.",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Enable verbose output.",
)
def convert(
    root_folders: tuple[Path, ...],
    output: Path,
    formats: tuple[str, ...],
    timestep: float,
    include_filters: tuple[str, ...],
    exclude_filters: tuple[str, ...],
    topics_file: Path | None,
    filename: str,
    hdf5_key: str,
    verbose: bool,
):
    """
    Convert rosbags to specified formats.

    ROOT_FOLDERS are the directories to search for rosbag run folders.
    """
    loader = DatasetLoader()

    # Find run folders
    root_folders_list = list(root_folders)
    if verbose:
        console.print(f"[blue]Searching for run folders in: {root_folders_list}[/blue]")

    run_folders = loader.get_run_folders(root_folders_list)

    if not run_folders:
        console.print("[red]No run folders found.[/red]")
        raise SystemExit(1)

    if verbose:
        console.print(f"[green]Found {len(run_folders)} run folder(s):[/green]")
        for folder in run_folders:
            console.print(f"  - {folder}")

    # Find rosbags in each run folder
    include_list = list(include_filters) if include_filters else None
    exclude_list = list(exclude_filters) if exclude_filters else None

    all_rosbags: list[Path] = []
    for run_folder in run_folders:
        rosbags = loader.get_rosbags_in_run(run_folder, include_list, exclude_list)
        all_rosbags.extend(rosbags)

    if not all_rosbags:
        console.print("[red]No rosbags found matching the criteria.[/red]")
        raise SystemExit(1)

    if verbose:
        console.print(f"[green]Found {len(all_rosbags)} rosbag(s):[/green]")
        for rosbag in all_rosbags:
            console.print(f"  - {rosbag}")

    # Load topics configuration
    topics = _load_topics_config(topics_file)

    if not topics:
        console.print("[yellow]No topics specified. Use --topics-file to specify topics to extract.[/yellow]")
        console.print("[yellow]Listing available topics in the first rosbag...[/yellow]")
        _list_topics_in_rosbag(all_rosbags[0])
        raise SystemExit(1)

    # Create run loaders for each experiment
    experiment_data_loaders_dict: dict[str, RunLoader] = {}
    for run_folder in run_folders:
        rosbags = loader.get_rosbags_in_run(run_folder, include_list, exclude_list)
        if rosbags:
            run_loader = RunLoader(rosbags, topics)
            data = run_loader.get_rosbag_data(rosbags, topics)
            if data:
                run_loader.data_dict = data
                experiment_data_loaders_dict[str(run_folder)] = run_loader

    if not experiment_data_loaders_dict:
        console.print("[red]Failed to load data from rosbags.[/red]")
        raise SystemExit(1)

    loader.experiment_data_loaders_dict = experiment_data_loaders_dict

    # Save to specified formats
    output_dir = output if output.is_dir() else output.parent
    formats_list = list(formats)

    console.print(f"[blue]Saving data to {output_dir} in formats: {formats_list}[/blue]")

    saved_files = loader.save_combined_data(
        output_dir=output_dir,
        filename=filename,
        formats=formats_list,
        timestep=timestep,
        hdf5_key=hdf5_key,
    )

    console.print("[green]Successfully saved files:[/green]")
    for fmt, path in saved_files.items():
        console.print(f"  - {fmt}: {path}")


@cli.command()
@click.argument("root_folders", nargs=-1, type=click.Path(exists=True, path_type=Path), required=True)
@click.option(
    "--include", "-i",
    "include_filters",
    multiple=True,
    help="Include filter pattern (regex).",
)
@click.option(
    "--exclude", "-e",
    "exclude_filters",
    multiple=True,
    help="Exclude filter pattern (regex).",
)
def list_runs(
    root_folders: tuple[Path, ...],
    include_filters: tuple[str, ...],
    exclude_filters: tuple[str, ...],
):
    """List all run folders and rosbags found in ROOT_FOLDERS."""
    loader = DatasetLoader()
    root_folders_list = list(root_folders)

    run_folders = loader.get_run_folders(root_folders_list)

    if not run_folders:
        console.print("[yellow]No run folders found.[/yellow]")
        return

    console.print(f"[green]Found {len(run_folders)} run folder(s):[/green]")

    include_list = list(include_filters) if include_filters else None
    exclude_list = list(exclude_filters) if exclude_filters else None

    for run_folder in run_folders:
        console.print(f"\n[bold]{run_folder}[/bold]")
        rosbags = loader.get_rosbags_in_run(run_folder, include_list, exclude_list)
        if rosbags:
            for rosbag in rosbags:
                console.print(f"  - {rosbag.name}")
        else:
            console.print("  [dim]No rosbags found[/dim]")


@cli.command()
@click.argument("rosbag_path", type=click.Path(exists=True, path_type=Path))
def list_topics(rosbag_path: Path):
    """List all topics in a rosbag."""
    _list_topics_in_rosbag(rosbag_path)


@cli.command()
@click.argument("rosbag_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--max-depth", "-d",
    type=int,
    default=5,
    show_default=True,
    help="Maximum depth to expand nested fields.",
)
@click.option(
    "--topic", "-t",
    "topics",
    multiple=True,
    help="Filter to specific topic(s). Can be specified multiple times.",
)
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    help="Output YAML file for topics configuration.",
)
def discover_fields(
    rosbag_path: Path,
    max_depth: int,
    topics: tuple[str, ...],
    output: Path | None,
):
    """
    Discover all topics and their fields in a rosbag.

    Expands nested message types using dot notation (e.g., pose.position.x).
    Useful for creating topics configuration files.
    """
    run_loader = RunLoader([rosbag_path], {})
    
    try:
        topics_fields = run_loader.discover_topics_and_fields(max_depth)
    except Exception as e:
        console.print(f"[red]Error discovering topics: {e}[/red]")
        raise SystemExit(1)

    if not topics_fields:
        console.print("[yellow]No topics found in rosbag.[/yellow]")
        return

    # Filter to specific topics if requested
    if topics:
        topics_fields = {k: v for k, v in topics_fields.items() if k in topics}
        if not topics_fields:
            console.print(f"[yellow]None of the specified topics found. Available topics:[/yellow]")
            all_topics = run_loader.discover_topics_and_fields(max_depth)
            for topic in all_topics:
                console.print(f"  - {topic}")
            return

    # Output to YAML file if requested
    if output:
        _save_topics_config(topics_fields, output)
        console.print(f"[green]Topics configuration saved to {output}[/green]")
    else:
        # Print to console
        from rich.table import Table
        
        for topic_name, fields in sorted(topics_fields.items()):
            table = Table(title=f"Topic: {topic_name}")
            table.add_column("Field Path", style="cyan")
            
            for field in sorted(fields):
                table.add_row(field)
            
            console.print(table)
            console.print()


def _save_topics_config(topics_fields: dict[str, list[str]], output_path: Path) -> None:
    """Save topics configuration to a YAML file."""
    try:
        import yaml
    except ImportError:
        console.print("[red]PyYAML is required to save topics file. Install with: pip install pyyaml[/red]")
        raise SystemExit(1)

    # Create a config structure suitable for the convert command
    config = {
        "topics": {topic: fields for topic, fields in topics_fields.items()}
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def _load_topics_config(topics_file: Path | None) -> dict[str, list[str]]:
    """Load topics configuration from a YAML file."""
    if topics_file is None:
        return {}

    try:
        import yaml
        with open(topics_file) as f:
            config = yaml.safe_load(f)
            return config.get("topics", {})
    except ImportError:
        console.print("[red]PyYAML is required to load topics file. Install with: pip install pyyaml[/red]")
        raise SystemExit(1)
    except Exception as e:
        console.print(f"[red]Error loading topics file: {e}[/red]")
        raise SystemExit(1)


def _list_topics_in_rosbag(rosbag_path: Path) -> None:
    """List all topics in a rosbag file."""
    from rosbags.highlevel import AnyReader
    from rosbags.typesys import Stores, get_typestore

    typestore = get_typestore(Stores.ROS2_HUMBLE)

    try:
        with AnyReader([rosbag_path], default_typestore=typestore) as reader:
            console.print(f"[bold]Topics in {rosbag_path.name}:[/bold]")
            for connection in reader.connections:
                console.print(f"  - {connection.topic} ({connection.msgtype}): {connection.msgcount} msgs")
    except Exception as e:
        console.print(f"[red]Error reading rosbag: {e}[/red]")


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
