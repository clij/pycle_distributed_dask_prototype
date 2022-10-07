from dask_image import ndmeasure
from skimage import measure


def run_connected_components_parallel(source_data, tile_config):
    results, count = ndmeasure.label(source_data)
    return results


def run_connected_components_single(source_data):
    results = measure.label(source_data)
    return results
