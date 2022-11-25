import math
import os.path

import dask.array as da
import zarr
from numcodecs import Blosc

from skimage.io import imread


def calculate_chunks(source_image, target_tile_sizes: str):
    # Handle incomplete information
    if target_tile_sizes:
        target_tile_sizes = target_tile_sizes.split(",")
        if len(target_tile_sizes) == 1:
            target_tile_sizes = [target_tile_sizes[0], target_tile_sizes[0], 1]
        elif len(target_tile_sizes) == 2:
            target_tile_sizes.append("1")
    else:
        target_tile_sizes = None

    image_size = source_image.shape
    if len(image_size) == 2:
        image_x_size = image_size[0]
        image_y_size = image_size[1]
        image_z_size = 1
    else:
        image_z_size = image_size[0]
        image_x_size = image_size[1]
        image_y_size = image_size[2]

    # Optional chunk size
    if not target_tile_sizes:
        if len(image_size) == 2:
            zarr_chunk_formats = (100, 100)
            array_chunk_formats = (image_x_size, image_y_size)
        else:
            zarr_chunk_formats = (1, 100, 100)
            array_chunk_formats = (1, image_x_size, image_y_size)  # Default don't operate on 3d chunks
    else:
        x_size = image_x_size
        y_size = image_y_size

        if len(image_size) == 3:
            z_size = image_z_size

            array_chunk_formats = zarr_chunk_formats = (
                x_size,
                y_size,
                z_size
            )
        else:
            array_chunk_formats = zarr_chunk_formats = (
                x_size,
                y_size,
            )

    return zarr_chunk_formats, array_chunk_formats


def validate_or_enforce_zarr(source: str, save_path: str = None, chunk_formats: str = None) -> [str, da]:
    """
    Take a path to a file, check if it is in zarr format, if not convert and save

    :param source: a string representation of the path to the source data
    :param save_path: optional parameter that allows you to specify where to save any zarr files (can be used as a tmp file if desired)
    :param chunk_formats: optional parameter that specifies the number of chunks to break the image up into.

    :return: the zarr file as a dask array, the actual pixel sizes of the chunks
    """
    if not os.path.isfile(source):
        raise ValueError("The source provided [" + source + "] was not a valid path.")

    # Use the current file's directory to save the new zarr file (if required)
    if not save_path:
        save_path = os.path.dirname(source)

    file_name, file_extension = os.path.splitext(os.path.basename(source))
    zarr_filename = os.path.join(save_path, file_name + ".zarr")
    if not file_extension == ".zarr":
        image = imread(source)
        zarr_chunks, array_chunks = calculate_chunks(image, chunk_formats)

        # Create a compressor for the zarr image and save it to disk
        compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)
        zarray = zarr.array(image, chunks=zarr_chunks, compressor=compressor)
        zarr.convenience.save(zarr_filename, zarray)
        zarray = da.from_zarr(zarr_filename, chunks=array_chunks)

    else:
        zarray = da.from_zarr(zarr_filename, chunks=chunk_formats)
        zarr_chunks, array_chunks = calculate_chunks(zarray, chunk_formats)

        # No need to rechunk if its already correct
        if zarr_chunks != zarray.chunks:
            zarray.rechunk(chunk_formats)

    return save_path, zarray


def output_zarr_to_directory(folder, name, data):
    zarr.convenience.save(os.path.join(folder, name), data)
