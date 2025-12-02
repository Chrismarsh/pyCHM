"""Utilities for working with VTU/UGRID datasets through xarray/uxarray."""

import xarray as xr
import uxarray as ux
import numpy as np
import geopandas as gp
import os

import fnmatch

UGRID_PATTERNS = [
    "*connectivity*",      # face_node_connectivity, edge_*_connectivity, etc.
    "grid_topology",
    "n_nodes_per_face",
    "node_*",              # node_lon, node_lat, node_x/y/z
    "edge_*",
]

def _match_any(name: str, patterns) -> bool:
    return any(fnmatch.fnmatch(name, pat) for pat in patterns)

def _is_numeric(da: xr.DataArray) -> bool:
    return np.issubdtype(da.dtype, np.number)

def _face_centered_vars_only(ds: xr.Dataset) -> list[str]:
    keep = []
    for name, da in ds.data_vars.items():
        if 'n_face' not in da.dims:
            continue
        if 'n_node' in da.dims:
            continue  # node-centered: skip
        if _match_any(name, UGRID_PATTERNS):
            continue  # ugrid scaffolding: skip
        if not _is_numeric(da):
            continue
        keep.append(name)
    return keep

def _ugrid_vars_only(ds: xr.Dataset) -> list[str]:
    keep = []
    for name, da in ds.data_vars.items():
        if _match_any(name, UGRID_PATTERNS):
            keep.append(name) # ugrid scaffolding
    return keep


@xr.register_dataset_accessor("chm")
class GeoAccessor:
    """xarray accessor for CHM convenience helpers, available via `.chm` on a Dataset."""
    def __init__(self, xarray_obj):
        self._obj = xarray_obj

    def uxgrid_to_netcdf(self, outpath: str) -> None:
        """Export mesh topology and global IDs from an uxarray dataset to NetCDF. No variables
        are written. To do so, use `vars_to_netcdf`.

        Parameters
        ----------
        outpath : str
            Destination path for the mesh-only NetCDF file.

        Notes
        -----
        Only mesh scaffolding variables and ``global_id`` are written. Time
        dimensions are dropped to keep a static mesh file for downstream tools.
        """
        ds = self._obj.uxgrid.to_xarray().drop_dims("time", errors="ignore")
        face_vars = _ugrid_vars_only(ds)
        ds = ds[face_vars]

        ds = ds.rename({"grid_topology": "Mesh2"})
        ds["Mesh2"].attrs["face_coordinates"] = "Mesh2_face_x Mesh2_face_y"

        ds.to_netcdf(outpath+".tmp")
        ds = None

        # copy global_id from source into target
        global_id = self._obj["global_id"].to_dataset().drop_dims("time", errors="ignore")

        tgt = xr.open_mfdataset(outpath+".tmp")
        tgt = xr.merge([tgt, global_id])
        tgt.to_netcdf(outpath)
        tgt = None

        os.remove(outpath+".tmp")

    def vars_to_netcdf(self, outpath: str) -> None:
        """Write all variables from the dataset (excluding mesh scaffolding) to NetCDF. To load,
        use the mesh written with `uxgrid_to_netcdf`.

        Parameters
        ----------
        outpath : str
            Destination path for the NetCDF file.
        """
        self._obj.to_xarray().to_netcdf(outpath)

    def clip(self, lat=None, lon=None, shp_file_path=None) -> ux.UxDataset:
        """Subset face-centered variables to a lat/lon bounding box or a geometry extent.

        Parameters
        ----------
        lat : list[float], optional
            Two-element list defining latitude bounds [min, max].
        lon : list[float], optional
            Two-element list defining longitude bounds [min, max].
        shp_file_path : str, optional
            Shapefile/GeoJSON path used to derive bounds when lat/lon are omitted.

        Returns
        -------
        ux.UxDataset
            Subset dataset retaining uxgrid metadata and coordinates.
        """
        ds = self._obj
        d_time = []
        d_notime = []

        if lat is None and lon is None and shp_file_path is None:
            raise Exception("Requires bounding box given by lat/lon or shpfile.")

        if lat is None and lon is None:
            shp = gp.read_file(shp_file_path)
            shp = shp.to_crs('epsg:4326')
            lon = [shp.bounds.values.flatten()[0], shp.bounds.values.flatten()[2]]
            lat = [shp.bounds.values.flatten()[1], shp.bounds.values.flatten()[3]]

        # uxgrid grid
        uxg = None

        for name, da in ds.data_vars.items():
            if 'n_face' not in da.dims:
                continue
            if 'n_node' in da.dims:
                continue  # node-centered: skip
            if _match_any(name, UGRID_PATTERNS):
                continue  # ugrid scaffolding: skip
            if not _is_numeric(da):
                continue
            # print(name)
            tmp = da.subset.bounding_box(lon, lat)
            if 'time' in da.dims:
                d_time.append(tmp)

                if uxg is None:
                    uxg = tmp.isel(time=0).uxgrid

            else:
                d_notime.append(tmp)
                if uxg is None:
                    uxg = tmp.uxgrid

        t = xr.merge(d_time)
        nt = xr.merge(d_notime)
        ds = xr.merge([t, nt])

        return ux.UxDataset(ds, uxgrid=uxg)

    def regrid(
        self,
        dxdy: float = 0.01,
        round_decimals: int = 6,
        duplicate_reducer: str = "mean",
        extra_exclude: list[str] | None = None,   # user overrides, fnmatch patterns
    ) -> xr.Dataset:
        """Regrid face-centered variables to a structured grid via uxarray bilinear remap. Suitable for
        meshes that comfortably fit on a single compute node as the regridding uses Uxarray's multi-threaded,
        non-Dask Cython regridder.

        Parameters
        ----------
        dxdy : float, optional
            Target grid spacing (decimal degrees) in both lat and lon directions.
        round_decimals : int, optional
            Number of decimal places to round resulting lat/lon coordinates.
        duplicate_reducer : {"mean", "first", "median", "max", "min"}, optional
            Reduction applied when multiple faces collapse to the same grid cell. Default is mean.
        extra_exclude : list[str], optional
            Additional fnmatch patterns to exclude from regridding.

        Returns
        -------
        xr.Dataset
            Structured grid dataset with latitude/longitude coordinates and selected variables.
        """
        obj = self._obj
        # --- normalize to Dataset and pick vars ---
        if isinstance(obj, xr.DataArray):
            if 'n_face' not in obj.dims or 'n_node' in obj.dims:
                raise ValueError("DataArray must be face-centered (has 'n_face' dim and not 'n_node').")
            name = obj.name or "var"
            ds_in = obj.to_dataset(name=name)
        else:
            ds_in = obj

        face_vars = _face_centered_vars_only(ds_in)
        if extra_exclude:
            face_vars = [v for v in face_vars if not _match_any(v, extra_exclude)]
        if not face_vars:
            raise ValueError("No face-centered data variables found after filtering.")

        # --- uxgrid handles / target grid ---
        uxg = ds_in.uxgrid
        xmin, xmax = float(uxg.node_lon.min()), float(uxg.node_lon.max())
        ymin, ymax = float(uxg.node_lat.min()), float(uxg.node_lat.max())

        xspan = max(abs(xmax - xmin), dxdy)
        yspan = max(abs(ymax - ymin), dxdy)
        numX = max(int(round(xspan / dxdy)), 2)
        numY = max(int(round(yspan / dxdy)), 2)

        x_center = np.linspace(xmin, xmax, numX)
        y_center = np.linspace(ymin, ymax, numY)
        target_grid = ux.Grid.from_structured(lat=y_center, lon=x_center)

        # --- remap only the face-centered vars we kept ---
        tmp = ds_in[face_vars]
        remap = tmp.remap.bilinear(destination_grid=target_grid, remap_to="nodes")

        pieces = [remap.uxgrid.to_xarray()]  # target topology + coords

        # aggregate back to faces; skip any stubborn variables defensively
        for v in face_vars:
            try:
                pieces.append(remap[v].topological_mean(destination="face").to_xarray())
            except Exception as e:
                print(f"[regrid_to_rect] Skipping {v} (aggregate-to-face failed): {e}")

        df = xr.merge(xr.align(*pieces, join="override", exclude=["time"]), join="exact")

        # --- build rect (lat, lon) coords from face centroids ---
        ROUND = int(round_decimals)
        conn  = df["face_node_connectivity"]
        valid = conn >= 0
        face_lon = df["node_lon"].isel(n_node=conn).where(valid).mean("n_max_face_nodes")
        face_lat = df["node_lat"].isel(n_node=conn).where(valid).mean("n_max_face_nodes")
        face_lon = xr.DataArray(np.round(face_lon.values, ROUND), dims=("n_face",), name="lon")
        face_lat = xr.DataArray(np.round(face_lat.values, ROUND), dims=("n_face",), name="lat")

        # attach and unstack for everything we actually produced
        produced = [v for v in face_vars if v in df]
        rect = df[produced].assign_coords(
            lat=("n_face", face_lat.data),
            lon=("n_face", face_lon.data),
        ).set_index(n_face=["lat", "lon"])

        if duplicate_reducer == "mean":
            rect = rect.groupby("n_face").mean()
        elif duplicate_reducer == "first":
            rect = rect.groupby("n_face").first()
        elif duplicate_reducer == "median":
            rect = rect.groupby("n_face").median()
        elif duplicate_reducer == "max":
            rect = rect.groupby("n_face").max()
        elif duplicate_reducer == "min":
            rect = rect.groupby("n_face").min()
        else:
            raise ValueError("duplicate_reducer must be one of: mean, first, median, max, min")

        rect = rect.unstack("n_face").sortby(["lat", "lon"]).assign_coords(
            lat=lambda d: d["lat"].assign_attrs(standard_name="latitude", units="degrees_north", axis="Y"),
            lon=lambda d: d["lon"].assign_attrs(standard_name="longitude", units="degrees_east", axis="X"),
        )
        return rect
