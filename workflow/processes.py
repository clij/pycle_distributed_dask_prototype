import dask.array as da

from operations.connected_components import run_connected_components_parallel, run_connected_components_single
from operations.guassian_filter import run_gaussian_filter_parallel, run_gaussian_filter_single


def run_process_untiled(source_data: da, directive: str, properties: list) -> da:
    """
    Run a directive with properties (that should map to a pyclesperanto function)

    :param source_data: dask.array backed by zarr
    :param directive: the directive key to run (mapped internally to a pyclesperanto function)
    :param properties: the properties to supply to the function

    :return: the data output by the run
    """

    # This should be a flat 1:1 mapping of directive to pyclesperanto function...
    # TODO: This implementation could change based on how workflows are declared
    # match directive:
    #     case "connected_components":
    if directive == "connected_components":
        return run_connected_components_single(source_data)
        # case "gaussian_filter":
    elif directive == "gaussian_filter":
        return run_gaussian_filter_single(properties, source_data)
        # case _:
    else:
        return


def run_process(source_data: da, directive, properties, tile_config: None) -> da:
    """
    Run a directive with properties. In the case where tile settings are provided, we will run this in a tile based
    manner with an included aggregation step

    :param source_data: dask.array backed by zarr
    :param directive: the directive key to run (mapped internally to a pyclesperanto function)
    :param properties: the properties to supply to the function
    :param tile_config: the sub array sizes to use for a tile

    :return: the data output by the run
    """
    if not tile_config:
        return run_process_untiled(source_data, directive, properties)

    # Directive dependent execution - this is because there isn't a 1:1 mapping of tile aggregation solutions
    # match directive:
    #     case "connected_components":
    if directive == "connected_components":
        return run_connected_components_parallel(source_data, tile_config)
        # case "gaussian_filter":
    elif directive == "gaussian_filter":
        return run_gaussian_filter_parallel(properties, source_data, tile_config)
        # case _:
    else:
        return None
