import math
import os.path

import dask.array as da
import zarr
from imagecodecs.numcodecs import Blosc

from skimage.io import imread


def validate_or_enforce_zarr(source: str, save_path: str = None, chunk_formats: list = None) -> da:
    """
    Take a path to a file, check if it is in zarr format, if not convert and save

    :param source: a string representation of the path to the source data
    :param save_path: optional parameter that allows you to specify where to save any zarr files (can be used as a tmp file if desired)
    :param chunk_formats: optional parameter that specifies the number of chunks to break the image up into.

    :return: the zarr file as a dask array, the actual pixel sizes of the chunks
    """
    if not os.path.isfile(source):
        raise ValueError("The source provided was not a valid path.")

    # Use the current file's directory to save the new zarr file (if required)
    if not save_path:
        save_path = os.path.dirname(source)

    file_name, file_extension = os.path.splitext(source)
    zarr_filename = os.path.join(save_path, file_name + ".zarr")
    if not file_extension == ".zarr":
        image = imread(source)

        # Optional chunk size
        if not chunk_formats:
            chunk_formats = (100, 100, 1)
        else:
            image_size = image.shape
            if len(image_size) == 2:
                chunk_formats = (
                    math.ceil(image_size[0] / chunk_formats[0]),
                    math.ceil(image_size[1] / chunk_formats[1]),
                    1
                )
            else:
                chunk_formats = (
                    math.ceil(image_size[0] / chunk_formats[0]),
                    math.ceil(image_size[1] / chunk_formats[1]),
                    math.ceil(image_size[2] / chunk_formats[2])
                )

        # Create a compressor for the zarr image and save it to disk
        compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)
        zarray = zarr.array(image, chunks=chunk_formats, compressor=compressor)
        zarr.convenience.save(zarr_filename, zarray)

    else:
        zarray = da.from_zarr(zarr_filename, chunks=chunk_formats)

        # Optional chunk size
        if chunk_formats:
            image_size = zarray.shape
            if len(image_size) == 2:
                chunk_formats = (
                    math.ceil(image_size[0] / chunk_formats[0]),
                    math.ceil(image_size[1] / chunk_formats[1]),
                    1
                )
            else:
                chunk_formats = (
                    math.ceil(image_size[0] / chunk_formats[0]),
                    math.ceil(image_size[1] / chunk_formats[1]),
                    math.ceil(image_size[2] / chunk_formats[2])
                )

            # No need to rechunk if its already correct
            if chunk_formats != zarray.chunks:
                zarray.rechunk(chunk_formats)

    return save_path, zarray, chunk_formats


def output_zarr_to_directory(folder, name, data):
    zarr.convenience.save(os.path.join(folder, name), data)
