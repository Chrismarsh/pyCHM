import sys
import numpy as np
import esmpy as ESMF
import rasterio
import zarr
from rasterio.crs import CRS
import xarray as xr
import rioxarray  # for xarray.rio
import os
import shutil
from mpi4py import MPI
import osgeo_utils.gdal_merge
import glob
import itertools
import argparse
from pathlib import Path

from osgeo import gdal
gdal.UseExceptions()



class UnsafePathError(RuntimeError):
    """Raised when a destructive operation is requested on a dangerous path."""
    pass


def _is_parent_or_same(parent: str, child: str) -> bool:
    """Return True if `parent` is the same as or an ancestor of `child`."""
    parent = os.path.realpath(os.path.abspath(parent)).rstrip(os.sep)
    child = os.path.realpath(os.path.abspath(child)).rstrip(os.sep)
    if parent == child:
        return True
    return child.startswith(parent + os.sep)


def safe_rmtree(
    path: str,
    *,
    protect_cwd: bool = True,
    protect_home: bool = True,
    protect_root: bool = True,
    allow_symlink: bool = False,
) -> None:
    """
    Safely remove a directory tree.

    Guards against catastrophes like:
      - '.', '..'
      - current working directory
      - home directory
      - filesystem root
      - ancestors of CWD / $HOME (by default)

    Parameters
    ----------
    path : str
        Path to remove.
    protect_cwd : bool
        Refuse to remove the current working directory or any of its ancestors.
    protect_home : bool
        Refuse to remove the user's home directory or any of its ancestors.
    protect_root : bool
        Refuse to remove the filesystem root '/'.
    allow_symlink : bool
        If False, refuse to operate on symlinks to avoid surprises.
    """
    if not path:
        raise UnsafePathError("Refusing to remove empty path string.")

    # Normalize and resolve symlinks
    path = os.path.realpath(os.path.abspath(path))

    if not os.path.exists(path):
        # Nothing to do; silently return
        return

    # Symlink protection
    if os.path.islink(path) and not allow_symlink:
        raise UnsafePathError(f"Refusing to remove symlink path: {path}")

    # Collect reference points
    cwd = os.path.realpath(os.getcwd())
    home = os.path.realpath(os.path.expanduser("~"))
    root = os.path.realpath(os.path.abspath(os.sep))

    # Root protection
    if protect_root and path == root:
        raise UnsafePathError("Refusing to remove filesystem root '/'.")

    # CWD protection: don't remove cwd or any ancestor of cwd
    if protect_cwd and _is_parent_or_same(path, cwd):
        raise UnsafePathError(f"Refusing to remove path that is cwd or its ancestor: {path}")

    # HOME protection: don't remove home or any ancestor of home
    if protect_home and _is_parent_or_same(path, home):
        raise UnsafePathError(f"Refusing to remove path that is home or its ancestor: {path}")

    # All checks passed: actually delete
    shutil.rmtree(path)


def _prepare_coord_array(data):
    """Normalize coordinate arrays for storage."""
    arr_data = np.asarray(data)
    attrs = {}
    if np.issubdtype(arr_data.dtype, np.datetime64):
        arr_data = arr_data.astype('datetime64[ns]').astype('int64')
        attrs['units'] = 'nanoseconds since 1970-01-01T00:00:00'
        attrs['calendar'] = 'proleptic_gregorian'
    return arr_data, attrs


GRID_MAPPING_VAR = 'spatial_ref'


def _crs_metadata():
    crs = CRS.from_epsg(4326)
    epsg_code = crs.to_epsg() or 4326
    semi_major = 6378137.0
    inverse_flattening = 298.257223563
    ellipsoid = getattr(crs, 'ellipsoid', None)
    if ellipsoid is not None:
        semi_major = getattr(ellipsoid, 'semi_major_metre', semi_major)
        inverse_flattening = getattr(ellipsoid, 'inverse_flattening', inverse_flattening)

    return {
        'spatial_ref': crs.to_wkt(),
        'grid_mapping_name': 'latitude_longitude',
        'epsg_code': f'EPSG:{epsg_code}',
        'semi_major_axis': semi_major,
        'inverse_flattening': inverse_flattening,
        'longitude_of_prime_meridian': 0.0,
        'latitude_of_prime_meridian': 0.0
    }


def _write_crs_variable(root):
    attrs = _crs_metadata()
    if GRID_MAPPING_VAR in root:
        crs_array = root[GRID_MAPPING_VAR]
    else:
        crs_array = root.create_array(
            GRID_MAPPING_VAR,
            shape=(),
            dtype='int8',
            chunks=(),
            dimension_names=[],
            compressors=[],
        )
    crs_array[...] = 0
    crs_array.attrs['_ARRAY_DIMENSIONS'] = []
    for key, value in attrs.items():
        crs_array.attrs[key] = value


def _initialize_tiff_path(path, overwrite=False):

    if os.path.isdir(path):
        if not overwrite:
            raise FileExistsError(f"Tiff path '{path}' already exists. Use --zarr-overwrite to replace it.")
        safe_rmtree(path)

    os.makedirs(path)

def _initialize_zarr_store(path, variables, time_values, y_coords, x_coords,
                           chunk_time=1, chunk_y=512, chunk_x=512, overwrite=False):
    """Create an empty Zarr store compatible with xarray region writes."""
    if os.path.isdir(path):
        if not overwrite:
            raise FileExistsError(f"Zarr path '{path}' already exists. Use --zarr-overwrite to replace it.")
        safe_rmtree(path)

    root = zarr.open_group(path, mode='w')
    root.attrs.setdefault('Conventions', 'CF-1.8, GeoZarr-1.0')
    root.attrs.setdefault('geospatial_crs', 'EPSG:4326')

    coord_datasets = {
        'time': (time_values, ['time']),
        'latitude': (y_coords, ['latitude']),
        'longitude': (x_coords, ['longitude'])
    }
    for name, (data, dims) in coord_datasets.items():
        arr_data, coord_attrs = _prepare_coord_array(data)
        arr = root.create_array(
            name,
            shape=arr_data.shape,
            dtype=arr_data.dtype,
            chunks=arr_data.shape,
            dimension_names=dims,
            compressors=[],
        )
        arr[...] = arr_data
        arr.attrs['_ARRAY_DIMENSIONS'] = dims
        for key, value in coord_attrs.items():
            arr.attrs[key] = value

    chunks = (max(chunk_time, 1), max(chunk_y, 1), max(chunk_x, 1))

    for var in variables:
        dims = ['time', 'latitude', 'longitude']
        arr = root.create_array(
            var,
            shape=(len(time_values), len(y_coords), len(x_coords)),
            dtype='f4',
            chunks=chunks,
            fill_value=np.nan,
            dimension_names=dims,
            compressors=[],
        )
        arr.attrs['_ARRAY_DIMENSIONS'] = dims
        arr.attrs['grid_mapping'] = GRID_MAPPING_VAR

    _write_crs_variable(root)


def _write_zarr_chunk(zarr_path, var_name, time_index, time_value, time_dtype, data_chunk,
                      y_coords, x_coords, y_slice, x_slice):
    """Write a chunk for a single variable/time step into the Zarr store."""
    y_len = max(0, y_slice.stop - y_slice.start)
    x_len = max(0, x_slice.stop - x_slice.start)
    if y_len == 0 or x_len == 0 or data_chunk.size == 0:
        return

    if data_chunk.shape != (y_len, x_len):
        raise ValueError(
            f"Data chunk shape {data_chunk.shape} does not match slice lengths {(y_len, x_len)}"
        )

    chunk_ds = xr.Dataset(
        {
            var_name: (('time', 'latitude', 'longitude'), data_chunk[np.newaxis, ...])
        },
        coords={
            'time': xr.DataArray(np.array([time_value], dtype=time_dtype), dims='time'),
            'latitude': xr.DataArray(y_coords[y_slice], dims='latitude'),
            'longitude': xr.DataArray(x_coords[x_slice], dims='longitude')
        }
    )
    chunk_ds[var_name].attrs['grid_mapping'] = GRID_MAPPING_VAR

    region = {
        'time': slice(time_index, time_index + 1),
        'latitude': y_slice,
        'longitude': x_slice
    }
    chunk_ds.to_zarr(zarr_path, mode='r+', region=region, consolidated=False)



def log(message):
    print(f'[{ESMF.local_pet()}] {message}')

def ugrid2grid(ugrid_nc, dxdy=0.01, mesh_topology_nc=None, method='conservative', save_weights_file=None,
               load_weights_file=None, variables=None, time_offsets=None, zarr_path=None,
               overwrite=False, zarr_chunk_y=512, zarr_chunk_x=512, tiff_path=None):
    """
    Convert a ugrid file to tiff. The ugrid file needs to come from the pvd to ugrid conversion

    df = pc.open_pvd('output_FSM_rhod600/SC.pvd')
    df=df.set_index('datetime')['2017-11-01':'2018-04-03'].reset_index()
    df = df.iloc[[0,30,60,90,120]] # every 30days in this period
    pc.vtu_to_ugrid(df, 'test2.nc')

    :param ugrid_nc:
    :param dxdy:
    :param mesh_topology_nc:
    :param method:
    :param save_weights_file:
    :param load_weights_file:
    :param variables:
    :param time_offsets:
    :param zarr_path: Optional path to write the structured grid dataset as Zarr.
    :param overwrite: Overwrite an existing Zarr store if True.
    :param zarr_chunk_y: Chunk height to use for Zarr variables.
    :param zarr_chunk_x: Chunk width to use for Zarr variables.
    :param write_tiffs: Disable GeoTIFF emission when set to False.
    :return:
    """
    # mg = ESMF.Manager(debug=True)
    comm = MPI.COMM_WORLD

    zarr_enabled = zarr_path is not None
    tiff_enabled = tiff_path is not None

    if not tiff_enabled and not zarr_enabled:
        raise ValueError('At least one output target (TIFF or Zarr) must be enabled')

    if save_weights_file is not None and load_weights_file is not None:
        raise Exception("Cannot have both save_weights_file and load_weights_file set")

    # we might be loading a seperate mesh topology
    mnc = ugrid_nc if mesh_topology_nc is None else mesh_topology_nc

    mesh = ESMF.Mesh(filename=mnc,
                            filetype=ESMF.api.constants.FileFormat.UGRID,
                            meshname='Mesh2'
                     )

    nodes, elements = (0, 1)
    u, v = (0, 1)

    # communicate accross all the ranks to figure out the bounds of our mesh
    xmin_m = np.array([mesh.coords[nodes][u].min()])
    xmax_m = np.array([mesh.coords[nodes][u].max()])
    ymin_m = np.array([mesh.coords[nodes][v].min()])
    ymax_m = np.array([mesh.coords[nodes][v].max()])

    xmin = np.empty(1, dtype=np.float64)
    xmax = np.empty(1, dtype=np.float64)
    ymin = np.empty(1, dtype=np.float64)
    ymax = np.empty(1, dtype=np.float64)

    comm.Allreduce(xmin_m, xmin, op=MPI.MIN)
    comm.Allreduce(ymin_m, ymin, op=MPI.MIN)
    comm.Allreduce(xmax_m, xmax, op=MPI.MAX)
    comm.Allreduce(ymax_m, ymax, op=MPI.MAX)

    xmin = xmin[0]
    ymin = ymin[0]
    xmax = xmax[0]
    ymax = ymax[0]

    x = np.abs(xmin-xmax)
    y = np.abs(ymin-ymax)

    numX, numY = int(x/dxdy), int(y/dxdy)

    # log(f'numX, numY = {numX}, {numY}')

    # log(f'umin={mesh.coords[nodes][u].min()} umax={mesh.coords[nodes][u].max()} vmin={mesh.coords[nodes][v].min()} vmax={mesh.coords[nodes][v].max()} ')

    # cell centres
    dxdy2 = dxdy/2.
    x_center = np.linspace(start=xmin + dxdy2,
                           stop=xmax - dxdy2, num=numX)

    y_center = np.linspace(start=ymin + dxdy2,
                           stop=ymax - dxdy2, num=numY)

    # node coords
    x_corner = np.linspace(start=xmin, stop=xmax, num=numX + 1)
    y_corner = np.linspace(start=ymin, stop=ymax, num=numY + 1)


    max_index = np.array([len(x_center), len(y_center)])
    # log( f' max_index={max_index}')


    grid = ESMF.Grid(max_index, staggerloc=[ESMF.StaggerLoc.CENTER, ESMF.StaggerLoc.CORNER], coord_sys=ESMF.CoordSys.SPH_DEG)

    # RLO: access Grid center coordinates
    gridXCenter = grid.get_coords(0)
    gridYCenter = grid.get_coords(1)

    # RLO-v2: adjust coordinate array to bounds of the current PET (rank)
    center_lb = grid.lower_bounds[ESMF.StaggerLoc.CENTER]
    center_ub = grid.upper_bounds[ESMF.StaggerLoc.CENTER]
    x_slice = slice(center_lb[0], center_ub[0])
    y_slice = slice(center_lb[1], center_ub[1])
    x_center_par = x_center[x_slice]
    y_center_par = y_center[y_slice]

    # RLO: set Grid center coordinates as a 2D array (this can also be done 1d)
    gridXCenter[...] = x_center_par.reshape((x_center_par.size, 1))
    gridYCenter[...] = y_center_par.reshape((1, y_center_par.size))

    # RLO: access Grid corner coordinates
    gridXCorner = grid.get_coords(0, staggerloc=ESMF.StaggerLoc.CORNER)
    gridYCorner = grid.get_coords(1, staggerloc=ESMF.StaggerLoc.CORNER)

    # # RLO-v2: adjust coordinate array to bounds of the current PET (rank)
    x_corner_par = x_corner[grid.lower_bounds[ESMF.StaggerLoc.CORNER][0]:grid.upper_bounds[ESMF.StaggerLoc.CORNER][0]]
    y_corner_par = y_corner[grid.lower_bounds[ESMF.StaggerLoc.CORNER][1]:grid.upper_bounds[ESMF.StaggerLoc.CORNER][1]]

    # # RLO: set Grid corner coordinats as a 2D array
    gridXCorner[...] = x_corner_par.reshape((x_corner_par.size, 1))
    gridYCorner[...] = y_corner_par.reshape((1, y_corner_par.size))

    # grid._write_(f'{ESMF.local_pet()}-grid')

    df = xr.open_mfdataset(ugrid_nc)

    if variables is None:
        variables = list(df.keys())

    # don't convert these to tiff
    exclude_list = ['Mesh2', 'Mesh2_face_nodes', 'Mesh2_node_x', 'Mesh2_node_y', 'Mesh2_face_x', 'Mesh2_face_y', 'time', 'global_id' ]

    # the sort is important as otherwise this can have a different order on different mpi ranks
    variables = sorted(list(set(variables)-set(exclude_list)))

    srcfield = ESMF.Field(mesh, meshloc=ESMF.MeshLoc.ELEMENT)
    dstfield = ESMF.Field(grid, staggerloc=ESMF.StaggerLoc.CENTER)

    regrid_method = ESMF.RegridMethod.CONSERVE if method == 'conservative' else ESMF.RegridMethod.BILINEAR
    # log(f"""Using {'ESMF.RegridMethod.CONSERVE' if method == 'conservative' else 'ESMF.RegridMethod.BILINEAR'} regridder""")

    # clean up old weight file and
    if save_weights_file is not None and comm.Get_rank() == 0:
        if os.path.isfile(
                os.path.join(os.getcwd(), save_weights_file)):
            os.remove(os.path.join(os.getcwd(), save_weights_file))

    comm.barrier()
    regrid = None
    if save_weights_file is not None:
        regrid = ESMF.Regrid(srcfield, dstfield, regrid_method=regrid_method, filename=save_weights_file,
                         unmapped_action=ESMF.UnmappedAction.IGNORE)
    elif load_weights_file is not None:
        log(f'Loading weights from {load_weights_file}')
        regrid = ESMF.RegridFromFile(srcfield, dstfield, filename=load_weights_file)
    else:
        regrid = ESMF.Regrid(srcfield, dstfield, regrid_method=regrid_method, unmapped_action=ESMF.UnmappedAction.IGNORE)


    # srcfield.read(filename=ugrid_nc, variable='global_id', timeslice=0)
    # offsets = np.array(srcfield.data[:], dtype=np.int64)
    # offset_mask = df.global_id.isin(offsets)


    # get the global_id offsets this rank is using
    srcfield_offsets = ESMF.Field(mesh, meshloc=ESMF.MeshLoc.ELEMENT)
    srcfield_offsets.read(filename=mnc,
                          variable='global_id', timeslice=0)
    offsets = np.array(srcfield_offsets.data[:], dtype=np.int64) #these need to be ints to index with
    offset_mask = df.global_id.isin(offsets).compute()
    srcfield_offsets.destroy()
    srcfield_offsets = None

    #hold a list of the processed times so we don't have to recompute it when dealing with tiff merging
    processed_times = []

    if time_offsets is None:
        time_offsets = range(0, df.time.shape[0])

    time_offsets = list(time_offsets)
    if len(time_offsets) == 0:
        log('No time offsets provided; nothing to process.')
        df.close()
        return

    zarr_time_values = None
    zarr_time_dtype = None
    if zarr_enabled:
        if comm.Get_rank() == 0:
            time_coord_raw = df.time.isel(time=time_offsets).load().data
            zarr_time_values, _ = _prepare_coord_array(time_coord_raw)
            zarr_time_dtype = zarr_time_values.dtype
            log(f'Initializing Zarr store at {zarr_path}')
            _initialize_zarr_store(

                zarr_path, variables, time_coord_raw, y_center, x_center,
                chunk_time=1, chunk_y=zarr_chunk_y, chunk_x=zarr_chunk_x, overwrite=overwrite
            )
        zarr_time_values = comm.bcast(zarr_time_values, root=0)
        zarr_time_dtype = comm.bcast(zarr_time_dtype, root=0)
        comm.barrier()

    if tiff_enabled:
        if comm.Get_rank() == 0:
            log(f"Initializing Tiff output at {tiff_path}")
            _initialize_tiff_path(tiff_path, overwrite=overwrite)

        tiff_path = Path(tiff_path)

    for time_index, ts in enumerate(time_offsets):

        time = str(df.time[ts].dt.strftime('%Y%m%dT%H%M%S').data)
        time_value = zarr_time_values[time_index] if zarr_time_values is not None else None

        for var in variables:
            log(f'{time} - {var}')

            srcfield.data[:] = df.isel(time=ts)[var].where(offset_mask, drop=True).data
            dstfield.data[...] = np.nan

            dstfield = regrid(srcfield, dstfield, zero_region=ESMF.Region.SELECT)
            data_yx = dstfield.data.T

            if zarr_enabled:
                has_data = data_yx.size > 0
                for writer_rank in range(comm.Get_size()):
                    if comm.Get_rank() == writer_rank and has_data:
                        _write_zarr_chunk(
                            zarr_path, var, time_index, time_value, zarr_time_dtype,
                            data_yx.astype(np.float32, copy=True),
                            y_center, x_center, y_slice, x_slice
                        )
                    comm.barrier()

            if tiff_enabled:
                tiff = xr.DataArray(np.flip(data_yx, axis=0), name=var,
                                   coords={'latitude': y_center_par.data,
                                           'longitude': x_center_par.data
                                           },
                                   dims=['latitude', 'longitude'])
                tiff = tiff.rio.write_nodata(-9999.0)
                tiff = tiff.rio.write_crs('+proj=longlat +datum=WGS84 +no_defs +type=crs')
                var_san = var.replace('[', '_').replace(']', '_')

                # r = tiff.rio.resolution(recalc=True)
                # b = tiff.rio.bounds(recalc=True)
                # # print(r)
                # print(b)
                # geotransform = (b[0], r[0], 0.0,
                #                 b[3], 0.0, -r[1] )
                # a = Affine.from_gdal(*geotransform)
                # print(a)
                # tiff = tiffrio.write_transform(transform=a).

                tiff.rio.to_raster(tiff_path / f'{ESMF.local_pet()}-{var_san}-{time}-{dxdy}x{dxdy}-output.tiff')

            # Wait to make sure everyone has written out this timestep + variable.
            # don't want to corrupt the srcfield/dstfields by partially writting into them
            comm.barrier()

        processed_times.append(time)

    comm.barrier()
    df.close()
    comm.barrier()


    if tiff_enabled:
        log('Merging tiffs')
        product = None
        if ESMF.local_pet() == 0:

            var_san = [var.replace('[', '_').replace(']', '_') for var in variables]

            product = [x for x in itertools.product(var_san, processed_times)]
            product = np.array_split(product, ESMF.pet_count())

        product = comm.scatter(product, root=0)
        # log(f'PET{ESMF.local_pet()} has {product}')

        for prod in product:
            var, time = prod
            files = glob.glob( str(tiff_path / f'*-{var}-{time}-{dxdy}x{dxdy}-output.tiff'))
            if len(files) != ESMF.pet_count():
                raise Exception(f"Missing files for {var} {time}")

            parameters = ['', '-o', tiff_path / f"{var}-{dxdy}x{dxdy}_{time}.tiff", '-n', '-9999', '-a_nodata', '-9999'] + files + ['-co', 'COMPRESS=LZW']
            osgeo_utils.gdal_merge.main(parameters)

            ds = gdal.Open(tiff_path / f"{var}-{dxdy}x{dxdy}_{time}.tiff", gdal.GA_Update)
            gt = list(ds.GetGeoTransform())

            ## Y_geo = GT(3) + X_pixel * GT(4) + Y_line * GT(5)


            ulx, xres, xskew, lly, yskew, yres = ds.GetGeoTransform()
            uly = lly + (ds.RasterYSize * yres)

            # print(gt)
            # print(f'lly={lly} ds.RasterYSize={ds.RasterYSize} yres={yres} ==> uly {uly}')
            gt[3] = uly
            gt[5] = -gt[5]
            ds.SetGeoTransform(gt)
            ds.FlushCache()
            ds = None

            for f in files:
                os.remove(f)

        log('Done')
    


def main():
    parser = argparse.ArgumentParser(description="Convert CHM UGRID NetCDF to TIFF")
    parser.add_argument("input_nc", help="Path to the input .nc file")
    parser.add_argument("--dxdy", type=float, default=0.01,
                        help="Grid resolution in degrees (default: 0.01)")
    parser.add_argument("--mesh", type=str, default=None,
                        help="If mesh topology is stored in another file")
    parser.add_argument("--method", type=str, default="bilinear",
                        choices=["bilinear", "conservative"],
                        help="Interpolation method (default: conservative)")
    parser.add_argument(
        "--timeoffset",
        type=int,
        nargs="+",             # allows one or more values
        default=None,
        help="Time offset(s) in hours (e.g., --timeoffset 0 6 12 18)",
    )
    parser.add_argument(
        "--variables",
        type=str,
        nargs="+",             # allows one or more values
        default=None,
        help="Variables (e.g., --variables t swe). Default is to convert all variables",
    )
    parser.add_argument(
        "--zarr-output",
        type=str,
        default=None,
        help="Path to write the structured grid dataset as Zarr",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwriting an existing Zarr and/or Tiff output directory",
    )
    parser.add_argument(
        "--zarr-chunk-y",
        type=int,
        default=512,
        help="Chunk height (number of rows) to use when writing Zarr",
    )
    parser.add_argument(
        "--zarr-chunk-x",
        type=int,
        default=512,
        help="Chunk width (number of columns) to use when writing Zarr",
    )
    parser.add_argument(
        "--tiff-output",
        type=str,
        default=None,
        help="Path to write the structured grid dataset as GeoTiffs",
    )

    args = parser.parse_args()

    if args.tiff_output is None and args.zarr_output is None:
        parser.error("--no-tiff requires --zarr-output to be set")

    def _is_safe_path(path):
        path = os.path.abspath(path)
        cwd = os.path.abspath(os.getcwd())

        # Safety checks
        if path in (cwd, os.path.dirname(cwd), "/"):
            raise ValueError(f"Refusing to operate on unsafe path: {path}")

    if args.tiff_output is not None:
        _is_safe_path(args.tiff_output)

    if args.zarr_output is not None:
        _is_safe_path(args.zarr_output)

    ugrid2grid(args.input_nc,
               dxdy=args.dxdy,
               method=args.method,
               mesh_topology_nc=args.mesh,
               time_offsets=args.timeoffset,
               variables=args.variables,
               zarr_path=args.zarr_output,
               overwrite=args.overwrite,
               zarr_chunk_y=args.zarr_chunk_y,
               zarr_chunk_x=args.zarr_chunk_x,
               tiff_path=args.tiff_output
    )

if __name__ == "__main__":
    main()
