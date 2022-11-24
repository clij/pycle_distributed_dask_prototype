# def run_connected_components_test():
#     data = "./test_data/blobs.tiff"
#     workflow = {0: {"connected_components", None}}
#     tile_arrangement = "3,3,1"
#     execution_config = "./test_data/slurm_execution_config.json"
#
#     from main import run
#     run(data, workflow, tile_arrangement, execution_config)
#
#
# def run_gaussian_filter_test():
#     data = "./test_data/blobs.tiff"
#     props = {"sigma": 10.0}
#     workflow = {0: {"gaussian_filter": props}}
#     tile_arrangement = "3,3,1"
#         execution_config = "./test_data/slurm_execution_config.json"
#
#     from main import run
#     run(data, workflow, tile_arrangement, execution_config)
from napari_workflows import Workflow
from napari_workflows._io_yaml_v1 import save_workflow
from pyclesperanto_prototype import threshold_otsu
from skimage._shared.filters import gaussian


def run_workflow_test():
    data = "./test_data/blobs.tiff"
    workflow = Workflow()
    workflow.set("g1", gaussian, "input", sigma=2, preserve_range=True)
    workflow.set("output", threshold_otsu, "g1")
    workflow_file_path = "test.yml"
    save_workflow(workflow_file_path, workflow)
    tile_config = ":,:,1"
    execution_config = "./test_data/slurm_execution_config.json"

    from main import run
    run(data, workflow_file_path, tile_config, execution_config, defer_workflow_handling=True)


def run_workflow_tiled_test():
    data = "./test_data/blobs.tiff"
    workflow = Workflow()
    workflow.set("g1", gaussian, "input", sigma=2)
    workflow.set("output", threshold_otsu, "g1")
    workflow_file_path = "test.yml"
    save_workflow(workflow_file_path, workflow)
    tile_config = "5,5,1"
    execution_config = "./test_data/slurm_execution_config.json"

    from main import run
    run(data, workflow_file_path, tile_config, execution_config, defer_workflow_handling=True)

