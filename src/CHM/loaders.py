import fsspec
from zarr.storage import FsspecStore
import os
import xarray as xr

def load_kerchunk_zarr(path):
    """ Load a zarr store from a kerchunked .json"""
    ref_fs = fsspec.filesystem(
        "reference",
        fo=os.path.abspath(path),
        remote_protocol="file",
    )

    store = FsspecStore(fs=ref_fs, path="", read_only=True)
    ds = xr.open_zarr(
                    store,
                    consolidated=False,
                    decode_times=True,

                    # only version 2 are written by the CHM tooling
                    zarr_format=2,
                )
    return ds