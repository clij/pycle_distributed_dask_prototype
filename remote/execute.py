from napari_workflows import Workflow
from napari_workflows._io_yaml_v1 import load_workflow, save_workflow


def execute_on_slurm_cluster(workflow: str, execution_config: dict):
    # TODO output execution profile to flatfile for deployment
    execution_configuraiton_path = "./test_data/slurm_execution_config.json"


def execute_workflow_on_remote(workflow: str, ui_pointer):
    # TODO build an execution profile using the ui
    execution_config = dict()
    execution_config["type"] = "SLURM"
    # TODO additional props

    # Allow 'remote' execution on a variety of deployment types
    if execution_config["type"] == "SLURM":
        execute_on_slurm_cluster(workflow, execution_config)

    else:
        return


def execute_current_workflow_on_remote(ui_pointer):
    # TODO Retrieve workflow from active UI
    workflow = Workflow()

    # Save the workflow to disk for remote deployment
    local_workflow_path = "workflow_to_deploy.yml"
    save_workflow(local_workflow_path, workflow)

    # Run remote workflow
    execute_workflow_on_remote(local_workflow_path, ui_pointer)
