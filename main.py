import json
import os.path
import sys

from distributed import Client
from skimage.io import imsave

from cluster.slurm import build_slurm_cluster
from image.formats import validate_or_enforce_zarr, output_zarr_to_directory
from operations.delegated import run_delegated
from workflow.workflows import process_workflow, run_workflow


def dask_setup(execution_config_path: str = None) -> Client:
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
    # match execution_config["type"]:
    #     case "SLURM":
    if execution_config["type"] == "SLURM":
        return build_slurm_cluster(execution_config)

        # TODO: Slurm multi-gpu, local cluster cuda, threads, single thread
        # TODO: Cannot currently support multi-gpu cluster?

        # case _:
    else:
        return client


def run(data_path: str, workflow, tile_arrangement: str = None, execution_config_path: str = None, defer_workflow_handling: bool = False):
    # Check file format and alter to zarr if required
    data_path, data = validate_or_enforce_zarr(data_path, chunk_formats=tile_arrangement)

    # Setup dask to understand our environment
    client = dask_setup(execution_config_path)

    # Old style handling 'fake' workflows
    if not defer_workflow_handling:
        # Process our workflow
        directives_map = process_workflow(workflow)

        # Run the directives against the data
        run_workflow(data_path, data, directives_map)

    # Defer workflow handling to napari-workflows
    else:
        # Delegate workflow processing
        output = run_delegated(workflow, data_path, data)
        imsave(os.path.join(data_path, "workflow_output.tiff"), output)
        # output_zarr_to_directory(data_path, "workflow_output.zarr", output)

    # Close out the client
    client.close()


def process_input_args(arguments: list):
    # Terrible arg parsing simply for hackathon
    tile_arrangement = ""
    is_delegate = False

    if len(arguments) >= 4:
        print("Not enough input arguments, expect data_path, workflow_path, execution_config_path as a minimum")

    data = arguments[1]
    if not os.path.isfile(data):
        print("Data file not valid")
        data = None

    workflow_file = arguments[2]
    if workflow_file.endswith(".json"):
        with open(workflow_file, 'r') as file:
            workflow = json.load(workflow_file)
    elif workflow_file.endswith(".yml"):
        workflow = workflow_file
        is_delegate = True
    else:
        print("Workflow file not valid")
        workflow = None

    execution_config = arguments[3]
    if not os.path.isfile(execution_config):
        print("Execution Config file not valid")
        execution_config = None

    if len(arguments) == 5:
        tile_arrangement = arguments[4]

    return data, workflow, tile_arrangement, execution_config, is_delegate


if __name__ == '__main__':
    data, workflow, tile_arrangement, execution_config, is_delegate = process_input_args(sys.argv)
    if data is None or workflow is None or tile_arrangement is None or execution_config is None:
        print("One of the required input arguments is missing")
        exit(1)

    run(data, workflow, tile_arrangement, execution_config, defer_workflow_handling=is_delegate)
