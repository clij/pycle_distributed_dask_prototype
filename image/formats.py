import math
import os.path

import dask.array as da
import zarr
from numcodecs import Blosc

from skimage.io import imread


def validate_or_enforce_zarr(source: str, save_path: str = None, chunk_formats: list = None) -> da:
    """
    Take a path to a file, check if it is in zarr format, if not convert and save

    :param source: a string representation of the path to the source data
    :param save_path: optional parameter that allows you to specify where to save any zarr files (can be used as a tmp file if desired)
    :param chunk_formats: optional parameter that specifies the number of chunks to break the image up into.

    :return: the zarr file as a dask array, the actual pixel sizes of the chunks
    """
    print(os.listdir("."))

    if not os.path.isfile(source):
        raise ValueError("The source provided [" + source + "] was not a valid path.")

    # Use the current file's directory to save the new zarr file (if required)
    if not save_path:
        save_path = os.path.dirname(source)

    file_name, file_extension = os.path.splitext(source)
    zarr_filename = os.path.join(save_path, file_name + ".zarr")
    if not file_extension == ".zarr":
        image = imread(source)
        image_size = image.shape

        # Optional chunk size
        if not chunk_formats:
            if len(image_size) == 2:
                chunk_formats = (100, 100)
            else:
                chunk_formats = (100, 100, 1)
        else:
            if chunk_formats[0] == ":":
                x_size = image_size[0]
            else:
                x_size = math.ceil(image_size[0] / int(chunk_formats[0]))

            if chunk_formats[1] == ":":
                y_size = image_size[1]
            else:
                y_size = math.ceil(image_size[1] / int(chunk_formats[1]))

            if len(image_size) == 3:
                if chunk_formats[2] == ":":
                    z_size = image_size[2]
                else:
                    z_size = math.ceil(image_size[2] / int(chunk_formats[2]))

                chunk_formats = (
                    x_size,
                    y_size,
                    z_size
                )
            else:
                chunk_formats = (
                    x_size,
                    y_size,
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
            if chunk_formats[0] == ":":
                x_size = image_size[0]
            else:
                x_size = math.ceil(image_size[0] / int(chunk_formats[0]))

            if chunk_formats[1] == ":":
                y_size = image_size[1]
            else:
                y_size = math.ceil(image_size[1] / int(chunk_formats[1]))

            if len(image_size) == 3:
                if chunk_formats[2] == ":":
                    z_size = image_size[2]
                else:
                    z_size = math.ceil(image_size[2] / int(chunk_formats[2]))

                chunk_formats = (
                    x_size,
                    y_size,
                    z_size
                )
            else:
                chunk_formats = (
                    x_size,
                    y_size,
                )

            # No need to rechunk if its already correct
            if chunk_formats != zarray.chunks:
                zarray.rechunk(chunk_formats)

    return save_path, zarray, chunk_formats


def output_zarr_to_directory(folder, name, data):
    zarr.convenience.save(os.path.join(folder, name), data)
