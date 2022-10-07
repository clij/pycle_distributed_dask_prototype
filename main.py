import json
import sys

from distributed import Client

from cluster.slurm import build_slurm_cluster
from image.formats import validate_or_enforce_zarr
from workflow.workflows import process_workflow, run_workflow


def dask_setup(execution_config_path: str = None) -> Client | None:
    """
    This function takes an execution configuration file and processes it in such a way that dask can work in a distributed
    manner where required. In the case where no file is provided, default dask scheduling occurs.

    :param execution_config_path: path to the execution config

    :return:
    """

    """
    One of the downsides of this implementation is that it permanently (until the workflow is finished executing) spins
    up its own distributed cluster type thing. This can be inefficient in terms of resources.
    """
    client = None
    if not execution_config_path:
        return client

    # Load our json file and bail if it doesn't work
    try:
        with open(execution_config_path, 'r') as file:
            execution_config = json.load(file)
    except Exception:
        print("Unable to read provided execution config: " + execution_config_path)
        return client

    # Match our execution type
    match execution_config["type"]:
        case "SLURM":
            return build_slurm_cluster(execution_config)

        # TODO: Slurm multi-gpu, local cluster cuda, threads, single thread
        # TODO: Cannot currently support multi-gpu cluster?

        case _:
            return client


def main(data_path: str, workflow_json: dict, tile_arrangement: str = None, execution_config_path: str = None):
    # Munge tile arrangement
    if tile_arrangement:
        tile_arrangement = [int(val) for val in tile_arrangement.split(",")]
        if len(tile_arrangement) == 1:
            tile_arrangement = [tile_arrangement[0], tile_arrangement[0], 1]
        elif len(tile_arrangement) == 2:
            tile_arrangement.append(1)
    else:
        tile_arrangement = None

    # Check file format and alter to zarr if required
    data_path, data, tile_arrangement = validate_or_enforce_zarr(data_path, chunk_formats=tile_arrangement)

    # Setup dask to understand our environment
    client = dask_setup(execution_config_path)

    # Process our workflow
    directives_map = process_workflow(workflow_json)

    # Run the directives against the data
    run_workflow(data_path, data, directives_map, tile_arrangement)


def process_input_args(arguments: list):
    data = ""
    workflow = ""
    tile_arrangement = ""
    execution_config = ""

    # TODO

    return data, workflow, tile_arrangement, execution_config


if __name__ == '__main__':
    data, workflow, tile_arrangement, execution_config = process_input_args(sys.argv)
    main(data, workflow, tile_arrangement, execution_config)
