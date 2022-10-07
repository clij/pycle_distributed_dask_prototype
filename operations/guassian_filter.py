import dask.array
import pyclesperanto_prototype


def filter(sigma_x, sigma_y, sigma_z, image):
    pyclesperanto_prototype.set_wait_for_kernel_finish(True)
    pyclesperanto_prototype.select_device("RTX")  # Fallback is okay, but we should have this globally configured as a singleton somewhere based on our execution config?

    # Do the actual operation
    return pyclesperanto_prototype.gaussian_blur(image, None, sigma_x=sigma_x, sigma_y=sigma_y, sigma_z=sigma_z)


def run_gaussian_filter_parallel(parameters, source_data, tile_config):
    # Get optional parameters
    sigma_x = sigma_y = sigma_z = float(parameters.get("sigma", 1.0))
    if "sigma_x" in parameters:
        sigma_x = float(parameters.get("sigma_x"))

    if "sigma_y" in parameters:
        sigma_y = float(parameters.get("sigma_y"))

    if "sigma_z" in parameters:
        sigma_z = float(parameters.get("sigma_z"))

    # Calculate our overlaps based on Robert's 'rule of thumb' - this is likely to be a value that needs to be assessed for every operation
    depth = (sigma_x * 4, sigma_y * 4, sigma_z * 4)
    tile_map = dask.array.map_overlap(filter, sigma_x, sigma_y, sigma_z, source_data, depth=depth, boundary='nearest')
    return tile_map.compute()


def run_gaussian_filter_single(parameters, source_data):
    # Get optional parameters
    sigma_x = sigma_y = sigma_z = float(parameters.get("sigma", 1.0))
    if "sigma_x" in parameters:
        sigma_x = float(parameters.get("sigma_x"))

    if "sigma_y" in parameters:
        sigma_y = float(parameters.get("sigma_y"))

    if "sigma_z" in parameters:
        sigma_z = float(parameters.get("sigma_z"))

    return filter(sigma_x, sigma_y, sigma_z, source_data)
