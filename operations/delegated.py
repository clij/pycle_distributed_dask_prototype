import dask.array

from napari_workflows._io_yaml_v1 import load_workflow


def run_tiled_workflow(data_as_tile, workflow_file):
    workflow = load_workflow(workflow_file)

    # Add in our tile section as the input
    workflow.set("input", data_as_tile)

    # How do we know the name of the final operation? Assume output for now
    return workflow.get("output")


def run_delegated(workflow, source_data_path, source_data, tile_config, depth=None):
    if depth is None:
        tile_map = dask.array.map_blocks(run_tiled_workflow, source_data, workflow=workflow)
    else:
        tile_map = dask.array.map_overlap(run_tiled_workflow, source_data, depth=depth, boundary='nearest', workflow=workflow)
    return tile_map
