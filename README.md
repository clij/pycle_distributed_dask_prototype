# CLIJ Distributed Dask
This is a demo application to validate a distributed variant of CLIJ that can leverage 
multiple GPUs / nodes.

## Design Brief:
User provides data, a description of the work they want to perform, an optional tile splitting, and an optional execution profile.
We then run the data through the workflow, splitting where appropriate, using the resources as declared in the execution_profile.

### Required Inputs:
* 2d/3d image data - preferably zarr format but we can inline convert if required
* Workflow: A list of tasks to perform to the 'image'.

### Optional Inputs:
* Tile config: vector of values determining number of tiles to subdivide the parent image into.
TODO: Evaluate if this is more flexibly described as number of pixel.
* Execution config: A configuration file that allows dask to understand how to scale operations up and out. 
If this file is not provided, single threaded local execution will be performed
* Temp folder: where to intermediaries and output zarr files

### Setup and use:
Provide a conda environment.yml that allows someone to build the required environment (promotes cluster deployment)

#### TODOs:
* Evaluate Performance on cluster
* Intermediates saving directory
* Flesh out workflow design / processing
* Evaluate overlap constraints for all implemented CLIJ functions
* Implement all CLIJ functions
* Implement a CUDACluster when it is eventually created (or evaluate dask-cuda-worker)