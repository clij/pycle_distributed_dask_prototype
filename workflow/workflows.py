from collections import OrderedDict

from image.formats import output_zarr_to_directory
from workflow.processes import run_process


def process_workflow(workflow: dict):
    """
    This function is responsible for interpreting a workflow and determining the functions that need to be run.
    The reason this is required is to allow for multiple entry points from different GUI implementations to use this.

    :param workflow: path to the workflow
    :return: an ordered map of directives that can be iterated over with parameters as values
    """
    # TODO: This needs to be worked out with Robert - it's only a 'fake' for now
    max_key = max(workflow.keys())
    directives_map = OrderedDict()

    # Ensure that our workflow is entered into the correct order
    for i in range(max_key):
        if i in workflow:
            # We expect only one entry here
            directive = workflow[i].keys()[0]
            props = workflow[i].values()[0]

            # Save
            directives_map[directive] = props
    return directives_map


def run_workflow(data_path, data, directives, tile_settings, save_intermediates=False):
    """
    Run the list of ordered directives against the provided dask_array (backed by a zarr)

    :param data: dask.array backed by zarr
    :param directives: an ordered map of directives to their properties
    :param tile_settings: the user specified tile sizes, when left as None, no tiling will be used

    :return: the final data step in the workflow for saving
    """
    counter = 0
    for directive, properties in directives.items():
        # TODO: Non-linear processing? Again this type of DAG deconvolution is best supported by dedicated development
        data = run_process(data, directive, properties, tile_config=tile_settings)

        # Optionally save the intermediates
        if save_intermediates:
            output_zarr_to_directory(data_path, "directive_" + directive + "_" + str(counter) + "_intermediate.zarr", data)

    # Final output
    # TODO: Better name chaining
    output_zarr_to_directory(data_path, "workflow_output.zarr", data)
