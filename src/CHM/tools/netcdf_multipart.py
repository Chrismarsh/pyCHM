"""Generate CHM forcing metadata JSON for multi-part meteorological NetCDF files."""

import argparse
import glob
import json
from pathlib import Path
import natsort
import xarray as xr
from tqdm import tqdm

def build_multipart(folder: str):
    """
    Scan a directory of NetCDF parts and write a CHM multipart metadata JSON.

    The output file is named ``metdata-<foldername>.json`` and written to the
    current working directory. Each entry records the time coverage and absolute
    path for a single NetCDF part, which CHM can consume sequentially.

    Parameters
    ----------
    folder : str
        Absolute or relative path to the directory containing ``*.nc`` files.
    """
    base = Path(folder)
    file_paths = natsort.natsorted(glob.glob(str(base / "*.nc")))
    metadata_list = []
    for file_path in tqdm(file_paths):
        # print(file_path)
        ds = xr.open_dataset(file_path)

        metadata = {
            "start_time": str(ds.time.min().dt.strftime("%Y%m%dT%H%M%S").values),  # Convert to string for JSON serialization
            "end_time": str(ds.time.max().dt.strftime("%Y%m%dT%H%M%S").values),
            "file_name": str(Path(file_path).resolve())
        }

        metadata_list.append(metadata)

        ds.close()

    out_path = Path.cwd() / f"metdata-{base.name}.json"
    with open(out_path, 'w') as f:
        json.dump(metadata_list, f, indent=4)

def main():
    """
    CLI entry point for generating multipart forcing metadata JSON for CHM.

    Run with the directory of NetCDF chunks, for example::

        python -m CHM.tools.netcdf_multipart /path/to/1980_2023_netcdf

    The tool writes ``metdata-1980_2023_netcdf.json`` to the current working
    directory, which CHM can use in place of a single monolithic forcing file.

        $ cat metdata-1980_2023_netcdf.json
        [
        {
            "start_time": "19800101T130000",
            "end_time": "19800102T120000",
            "file_name": "/path/to/1980_2023_netcdf/19800101.nc"
        },
        {
            "start_time": "19800102T130000",
            "end_time": "19800103T120000",
            "file_name": "/path/to/1980_2023_netcdf/19800102.nc"
        }
        ]

    which can be used in CHM in place of a netcdf

        "forcing":
        {
             "file":"metdata-1980_2023_netcdf.json"
        }

    """
    parser = argparse.ArgumentParser(description="Generate CHM multipart NetCDF metadata JSON.")
    parser.add_argument("folder", help="Folder containing NetCDF parts (absolute or relative).")
    args = parser.parse_args()
    build_multipart(args.folder)

if __name__ == "__main__":
    main()


