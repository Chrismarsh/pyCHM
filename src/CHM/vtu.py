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
    """
    xarray extension. Accessed via `.chm` on a dataframe. E.g., ``df.chm.to_raster(...)``
    """
    def __init__(self, xarray_obj):
        self._obj = xarray_obj

    def uxgrid_to_netcdf(self, outpath: str) -> None:
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
        """
        Outputs all variables to netcdf
        """
        self._obj.to_xarray().to_netcdf(outpath)

    def subset_to_boundingbox(self, lat: list, lon: list) -> ux.UxDataset:
        """
        Subsets all variables to the bounding box given by lat and lon min/max bounds
        """
        ds = self._obj
        d_time = []
        d_notime = []

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
                # print(f"Time in {name}")
            else:
                d_notime.append(tmp)
                # print(f"No time in {name}")

        t = xr.merge(d_time)
        nt = xr.merge(d_notime)
        ds = xr.merge([t, nt])
        uxg = d_time[0].isel(time=0).uxgrid

        return ux.UxDataset(ds, uxgrid=uxg)

    def subset_to_geo_boundingbox(self, shp_file_path: str) -> ux.UxDataset:
        """
        Subsets all variables to a bounding box derived from the extern of the geofile,
        e.g., shapefile, geojson
        """
        shp = gp.read_file(shp_file_path)
        shp = shp.to_crs('epsg:4326')
        lon = [shp.bounds.values.flatten()[0], shp.bounds.values.flatten()[2]]
        lat = [shp.bounds.values.flatten()[1], shp.bounds.values.flatten()[3]]

        return self.subset_to_boundingbox(lat, lon)

    def regrid(
        self,
        dxdy: float = 0.01,
        round_decimals: int = 6,
        duplicate_reducer: str = "mean",
        extra_exclude: list[str] | None = None,   # user overrides, fnmatch patterns
    ) -> xr.Dataset:
        """
        Regrids all variables to a structured grid. Uses the Uxarray cython regrid implementation so only
        appropriate for single-node sized meshes.
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
        print("starting remap")
        remap = tmp.remap.bilinear(destination_grid=target_grid, remap_to="nodes")
        print("finished")

        pieces = [remap.uxgrid.to_xarray()]  # target topology + coords

        # aggregate back to faces; skip any stubborn variables defensively
        print("aggregate")
        for v in face_vars:
            try:
                pieces.append(remap[v].topological_mean(destination="face").to_xarray())
            except Exception as e:
                print(f"[regrid_to_rect] Skipping {v} (aggregate-to-face failed): {e}")
        print("finished")

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