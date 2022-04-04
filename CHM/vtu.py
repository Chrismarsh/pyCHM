
import itertools
import pyvista as pv
import xarray as xr
import os
import xml.etree.ElementTree as ET
import numpy as np
import pandas as pd
import time
import dask
import dask.array as da

import re
import ESMF
import pathlib

# the order of the next two seems to matter for weird proj_data behaviour on some hpc machines
import rioxarray  # for xarray.rio
from rasterio.warp import transform

@xr.register_dataset_accessor("chm")
class GeoAccessor:
    """
    xarray extension. Accessed via `.chm` on a dataframe. E.g., ``df.chm.to_raster(...)``

    """
    def __init__(self, xarray_obj):
        self._obj = xarray_obj

    def get_dxdy(self):
        dxdy = self._obj.coords['dxdy'].values
        return dxdy

    def to_raster(self, var=None, crs_out=None, timefrmtstr='%Y-%m-%dT%H%M%S', _dowork_toraster=None):
        """
        Accessible from the xarray object, e.g., ``df.chm.to_raster(...)``. This allows for converting the entire data array into georeferenced tiffs.
        Will work on every timestep in parallel. Doing so requires dask to be configured to use processes and not threads, as the underlying regridding algorithm is not
        thread safe.

        :param timefrmtstr: Time format string for output
        :param var: List of variables to convert, otherwise all are converted
        :param crs_out: Output CRS to reproject to, otherwise uses source CRS
        :return:
        """

        def _dowork_toraster(crs_in, timefrmtstr, crs_out, d):
            name = d.name

            # this almost always comes as a single length vector but, sometimes, a scalar. No idea
            try:
                t = d.time.values[0]
            except IndexError as e:
                t = d.time.values

            time = pd.to_datetime(str(t))
            time = time.strftime(timefrmtstr)

            d = d.rio.write_nodata(-9999)
            dxdy = d.coords['dxdy'].values
            if crs_out is not None:
                tmp = d.squeeze().drop('lat').drop('lon').drop('time')
                tmp.rio.set_crs(crs_in)
                tmp = tmp.rio.reproject(crs_out)
            else:
                tmp = d

            tmp_tiff = f'{name}_{dxdy}x{dxdy}_{time}.tif'
            print(f'{tmp_tiff}')
            tmp.rio.to_raster(tmp_tiff)

            # return d


        if var is None:
            var = list(self._obj.keys())
        elif var is not isinstance(object, list):
            var = [var]

        work = []
        # fn = xr.apply_ufunc(_dowork_toraster, self._obj, kwargs={'crs_in': self._obj.rio.crs, 'timefrmtstr': timefrmtstr, 'crs_out': crs_out},dask='allowed')
        # print(fn)
        # mapped = xr.map_blocks(_dowork_toraster, self._obj, kwargs={'crs_in': self._obj.rio.crs, 'timefrmtstr': timefrmtstr, 'crs_out': crs_out}, template=self._obj)
        # mapped.compute()

        for t in range(0, len(self._obj.time.values)):
            for v in var:
                df = self._obj.isel(time=t)[v].copy()

                task = dask.delayed(_dowork_toraster)(self._obj.rio.crs, timefrmtstr, crs_out, df)
                work.append(task)

        dask.compute(*work)


def _get_shape(mesh, dxdy):
    # number of cells @ this dxdy
    # numX = int((Emesh.coords[nodes][u].max() - Emesh.coords[nodes][u].min()) / dxdy)
    # numY = int((Emesh.coords[nodes][v].max() - Emesh.coords[nodes][v].min()) / dxdy)

    x = np.abs(mesh.bounds[0] - mesh.bounds[1])
    y = np.abs(mesh.bounds[2] - mesh.bounds[3])

    return int(x/dxdy),int(y/dxdy)


# @dask.delayed
def _load_vtu(fname):
    # print('Loading and combining vtu...',end='')
    vtu = pv.MultiBlock([pv.read(f) for f in fname])
    vtu = vtu.combine()
    return vtu


@dask.delayed
def _regrid_mesh_to_grid(v, dxdy, var):
                         # num_node, nodeId, nodeCoord, nodeOwner,
                         # num_elem, elemId, elemType, elemConn, elemCoord,
                         # numX, numY,
                         # x_center, y_center,
                         # x_corner, y_corner
                         # ):

    print(f'_regrid_mesh_to_grid called for {var}')
    start_time = time.time()
    vtu = _load_vtu(v)
    mesh, grid = _build_regridding_ds(vtu, dxdy)

    # mesh, grid = _regridding_ds_from_partial(num_node, nodeId, nodeCoord, nodeOwner,
    #                             num_elem, elemId, elemType, elemConn, elemCoord,
    #                             numX, numY,
    #                             x_center, y_center,
    #                             x_corner, y_corner)
    print('Took ' + str(time.time() - start_time) + ' to build the regridding ds')

    start_time = time.time()
    srcfield = ESMF.Field(mesh, name=var, meshloc=ESMF.MeshLoc.ELEMENT)
    srcfield.data[:] = vtu[var]
    dstfield = ESMF.Field(grid, var)
    dstfield.data[...] = np.nan
    # Do not use the 2nd order Conserve!
    # Another difference is that the second-order method does not guarantee that after regridding the range of values
    # in the destination field is within the range of values in the source field. For example, if the mininum value
    # in the source field is 0.0, then it's possible that after regridding with the second-order method, the
    # destination field will contain values less than 0.0.
    # https://esmf-org.github.io/dev_docs/ESMF_refdoc/node3.html#SECTION03023000000000000000
    regrid = ESMF.Regrid(srcfield, dstfield, regrid_method=ESMF.RegridMethod.CONSERVE, unmapped_action=ESMF.UnmappedAction.IGNORE)
    out = regrid(srcfield, dstfield, zero_region=ESMF.Region.SELECT)

    df = da.from_array(out.data)

    print('Took ' + str(time.time() - start_time) + ' to regrid')

    return df

def _regridding_ds_from_partial(num_node, nodeId, nodeCoord, nodeOwner,
                                num_elem, elemId, elemType, elemConn, elemCoord,
                                numX, numY,
                                x_center, y_center,
                                x_corner, y_corner):
    nodes, elements = (0, 1)
    u, v = (0, 1)

    Emesh = ESMF.Mesh(parametric_dim=2, spatial_dim=2)
    Emesh.add_nodes(num_node, nodeId, nodeCoord, nodeOwner)
    Emesh.add_elements(num_elem, elemId, elemType, elemConn, element_coords=elemCoord)

    max_index = np.array([len(x_center), len(y_center)])

    grid = ESMF.Grid(max_index, staggerloc=[ESMF.StaggerLoc.CENTER, ESMF.StaggerLoc.CORNER],
                     coord_sys=ESMF.CoordSys.CART)

    # RLO: access Grid center coordinates
    gridXCenter = grid.get_coords(0)
    gridYCenter = grid.get_coords(1)

    # RLO: set Grid center coordinates as a 2D array (this can also be done 1d)
    gridXCenter[...] = x_center.reshape((x_center.size, 1))
    gridYCenter[...] = y_center.reshape((1, y_center.size))

    # RLO: access Grid corner coordinates
    gridXCorner = grid.get_coords(0, staggerloc=ESMF.StaggerLoc.CORNER)
    gridYCorner = grid.get_coords(1, staggerloc=ESMF.StaggerLoc.CORNER)

    # RLO: set Grid corner coordinats as a 2D array
    gridXCorner[...] = x_corner.reshape((x_corner.size, 1))
    gridYCorner[...] = y_corner.reshape((1, y_corner.size))

    return Emesh, grid

def _build_partial_regridding_ds(mesh, dxdy):
    print('_build_partial_regridding_ds called')

    nodes, elements = (0, 1)
    u, v = (0, 1)

    Emesh = ESMF.Mesh(parametric_dim=2, spatial_dim=2)
    num_node = mesh.n_points
    num_elem = mesh.n_cells

    nodeId = np.linspace(0, num_node, num_node)
    nodeCoord = np.empty(num_node * 2)  # bc xy coords
    nodeOwner = np.zeros(num_node)

    for i in range(0, num_node * 2, 2):
        # just x, y
        nodeCoord[i] = mesh.GetPoint(i // 2)[0]
        nodeCoord[i + 1] = mesh.GetPoint(i // 2)[1]

    Emesh.add_nodes(num_node, nodeId, nodeCoord, nodeOwner)

    elemId = np.empty(num_elem)
    elemType = np.empty(num_elem)
    elemConn = np.empty(num_elem * 3)
    elemCoord = np.empty(num_elem * 2)

    for i in range(0, num_elem * 3, 3):
        elemId[i // 3] = i

        elemType[i // 3] = ESMF.MeshElemType.TRI

        v0 = mesh.GetCell(i // 3).GetPointId(0)
        v1 = mesh.GetCell(i // 3).GetPointId(1)
        v2 = mesh.GetCell(i // 3).GetPointId(2)

        elemConn[i] = v0
        elemConn[i + 1] = v2
        elemConn[i + 2] = v1

        centreX = 1 / 3 * (nodeCoord[v0] + nodeCoord[v1] + nodeCoord[v2])
        centreY = 1 / 3 * (nodeCoord[v0 + 1] + nodeCoord[v1 + 1] + nodeCoord[v2 + 1])

        elemCoord[i // 3] = centreX
        elemCoord[(i // 3) + 1] = centreY

    Emesh.add_elements(num_elem, elemId, elemType, elemConn, element_coords=elemCoord)

    numX, numY = _get_shape(mesh, dxdy)

    # cell centres
    x_center = np.linspace(start=Emesh.coords[nodes][u].min() + dxdy / 2.,
                           stop=Emesh.coords[nodes][u].max() - dxdy / 2., num=numX)
    y_center = np.linspace(start=Emesh.coords[nodes][v].min() + dxdy / 2.,
                           stop=Emesh.coords[nodes][v].max() - dxdy / 2., num=numY)

    # node coords
    x_corner = np.linspace(start=Emesh.coords[nodes][u].min(), stop=Emesh.coords[nodes][u].max(), num=numX + 1)
    y_corner = np.linspace(start=Emesh.coords[nodes][v].min(), stop=Emesh.coords[nodes][v].max(), num=numY + 1)

    # max_index = np.array([len(x_center), len(y_center)])

    # grid = ESMF.Grid(max_index, staggerloc=[ESMF.StaggerLoc.CENTER, ESMF.StaggerLoc.CORNER],
    #                  coord_sys=ESMF.CoordSys.CART)
    #
    # # RLO: access Grid center coordinates
    # gridXCenter = grid.get_coords(0)
    # gridYCenter = grid.get_coords(1)
    #
    # # RLO: set Grid center coordinates as a 2D array (this can also be done 1d)
    # gridXCenter[...] = x_center.reshape((x_center.size, 1))
    # gridYCenter[...] = y_center.reshape((1, y_center.size))
    #
    # # RLO: access Grid corner coordinates
    # gridXCorner = grid.get_coords(0, staggerloc=ESMF.StaggerLoc.CORNER)
    # gridYCorner = grid.get_coords(1, staggerloc=ESMF.StaggerLoc.CORNER)
    #
    # # RLO: set Grid corner coordinats as a 2D array
    # gridXCorner[...] = x_corner.reshape((x_corner.size, 1))
    # gridYCorner[...] = y_corner.reshape((1, y_corner.size))

    return num_node, nodeId, nodeCoord, nodeOwner, num_elem, elemId, elemType, elemConn, elemCoord, numX, numY, x_center, y_center, x_corner, y_corner

# @dask.delayed
def _build_regridding_ds(mesh, dxdy):
    # print('_build_regridding_ds called')

    nodes, elements = (0, 1)
    u, v = (0, 1)

    Emesh = ESMF.Mesh(parametric_dim=2, spatial_dim=2)
    num_node = mesh.n_points
    num_elem = mesh.n_cells

    nodeId = np.linspace(0, num_node, num_node)
    nodeCoord = np.empty(num_node * 2)  # bc xy coords
    nodeOwner = np.zeros(num_node)

    for i in range(0, num_node * 2, 2):
        # just x, y
        nodeCoord[i] = mesh.GetPoint(i // 2)[0]
        nodeCoord[i + 1] = mesh.GetPoint(i // 2)[1]

    Emesh.add_nodes(num_node, nodeId, nodeCoord, nodeOwner)

    elemId = np.empty(num_elem)
    elemType = np.empty(num_elem)
    elemConn = np.empty(num_elem * 3)
    elemCoord = np.empty(num_elem * 2)

    for i in range(0, num_elem * 3, 3):
        elemId[i // 3] = i

        elemType[i // 3] = ESMF.MeshElemType.TRI

        v0 = mesh.GetCell(i // 3).GetPointId(0)
        v1 = mesh.GetCell(i // 3).GetPointId(1)
        v2 = mesh.GetCell(i // 3).GetPointId(2)

        elemConn[i] = v0
        elemConn[i + 1] = v2
        elemConn[i + 2] = v1

        centreX = 1 / 3 * (nodeCoord[v0] + nodeCoord[v1] + nodeCoord[v2])
        centreY = 1 / 3 * (nodeCoord[v0 + 1] + nodeCoord[v1 + 1] + nodeCoord[v2 + 1])

        elemCoord[i // 3] = centreX
        elemCoord[(i // 3) + 1] = centreY

    Emesh.add_elements(num_elem, elemId, elemType, elemConn, element_coords=elemCoord)

    numX, numY = _get_shape(mesh, dxdy)
    # numX = int((Emesh.coords[nodes][u].max() - Emesh.coords[nodes][u].min()) / dxdy)
    # numY = int((Emesh.coords[nodes][v].max() - Emesh.coords[nodes][v].min()) / dxdy)


    # cell centres
    x_center = np.linspace(start=Emesh.coords[nodes][u].min() + dxdy / 2.,
                           stop=Emesh.coords[nodes][u].max() - dxdy / 2., num=numX)
    y_center = np.linspace(start=Emesh.coords[nodes][v].min() + dxdy / 2.,
                           stop=Emesh.coords[nodes][v].max() - dxdy / 2., num=numY)

    # node coords
    x_corner = np.linspace(start=Emesh.coords[nodes][u].min(), stop=Emesh.coords[nodes][u].max(), num=numX + 1)
    y_corner = np.linspace(start=Emesh.coords[nodes][v].min(), stop=Emesh.coords[nodes][v].max(), num=numY + 1)

    max_index = np.array([len(x_center), len(y_center)])

    grid = ESMF.Grid(max_index, staggerloc=[ESMF.StaggerLoc.CENTER, ESMF.StaggerLoc.CORNER],
                     coord_sys=ESMF.CoordSys.CART)

    # RLO: access Grid center coordinates
    gridXCenter = grid.get_coords(0)
    gridYCenter = grid.get_coords(1)

    # RLO: set Grid center coordinates as a 2D array (this can also be done 1d)
    gridXCenter[...] = x_center.reshape((x_center.size, 1))
    gridYCenter[...] = y_center.reshape((1, y_center.size))

    # RLO: access Grid corner coordinates
    gridXCorner = grid.get_coords(0, staggerloc=ESMF.StaggerLoc.CORNER)
    gridYCorner = grid.get_coords(1, staggerloc=ESMF.StaggerLoc.CORNER)

    # RLO: set Grid corner coordinats as a 2D array
    gridXCorner[...] = x_corner.reshape((x_corner.size, 1))
    gridYCorner[...] = y_corner.reshape((1, y_corner.size))

    return Emesh, grid


def vtu_to_xarray(fname, dxdy=30, variables=None):
    blocks = pv.MultiBlock([pv.read(fname)])

    shape = _get_shape(blocks, dxdy)

    proj4 = blocks[0]["proj4"][0]
    print(f'proj4 = {proj4}')
    if variables is not None:
        # ensure we are not asking for an output that is a vector
        for v in variables:
            if len(blocks[0][v].shape) > 1:
                raise Exception('Cannot specify a variable that is a vector output shape = (n,3) ')
    else:
        variables = []
        blacklist = ['proj4', 'global_id']
        for v in blocks[0].array_names:
            if len(blocks[0][v].shape) == 1 and v not in blacklist:  # don't add the vectors
                a = v.replace('[param] ', '_param_').replace('/',
                                                             '')  # sanitize the name, this should be fixed in CHM though
                variables.append(a)

    delayed_vtu = {}
    for var in variables:
        delayed_vtu[var] = []

    # cell centres
    x_center = np.linspace(start=blocks.bounds[0] + dxdy / 2.,
                           stop=blocks.bounds[1] - dxdy / 2., num=shape[0])
    y_center = np.linspace(start=blocks.bounds[2] + dxdy / 2.,
                           stop=blocks.bounds[3] - dxdy / 2., num=shape[1])

    vtu = _load_vtu([fname])

    for var in variables:
        df = _regrid_mesh_to_grid(vtu, dxdy, var)
        d = da.from_delayed(df,
                            shape=shape,
                            dtype=np.dtype('float64'))

        tmp = xr.DataArray(d.T, name=var,
                           coords={'y': y_center,
                                   'x': x_center},
                           dims=['y', 'x'])

        delayed_vtu[var].append(tmp)

    epoch = np.datetime64(int(0), 's')
    end_time = np.datetime64(int(0), 's')
    dt = np.timedelta64(1, 's')

    _dt = int(dt.astype("timedelta64[s]") / np.timedelta64(1, 's'))

    times = pd.date_range(start=epoch, end=end_time, freq=f'{_dt} s', name="time")

    for var, arrays in delayed_vtu.items():
        delayed_vtu[var] = xr.concat(arrays, dim=times)
        delayed_vtu[var] = delayed_vtu[var].chunk({'time': 1})

    ds = xr.Dataset(data_vars=delayed_vtu)
    ds = ds.rio.set_crs(proj4)
    ds = ds.rio.write_crs(proj4)

    # Compute the lon/lat coordinates with rasterio.warp.transform
    ny, nx = len(ds['y']), len(ds['x'])
    x, y = np.meshgrid(ds['x'], ds['y'])
    # Rasterio works with 1D arrays
    lon, lat = transform(ds.rio.crs, {'init': 'EPSG:4326'}, x.flatten(), y.flatten())
    lon = np.asarray(lon).reshape((ny, nx))
    lat = np.asarray(lat).reshape((ny, nx))
    ds.coords['lon'] = (('y', 'x'), lon)
    ds.coords['lat'] = (('y', 'x'), lat)

    # Because the geo accessor can so easily lose information, set it like this so it is easily obtained
    ds = ds.assign_coords({'dxdy': dxdy})

    # sets internal rio crs
    ds = ds.rio.set_crs(proj4)

    # writes CRS in a CF compliant manner
    ds = ds.rio.write_crs(proj4)

    return ds

def pvd_to_xarray(fname, dxdy=50, variables=None):
    """
    Opens a pvd file and returns a Dask delayed xarray object. As it is a delayed object,
    once a specific timestep's variable is operated upon, the delayed object is computed
    and regridded.


    :param fname: pvd file path
    :param dxdy: spatial resolution in metres to regrid to.
    :para variables: List of variables to keep from the loaded vtu file
    :return:
    """

    # try:
    #     if dask.config.get('scheduler') != 'processes':
    #         raise("""Dask needs to use processes instead of threads because of the esmf backend.\nSet:\n\tdask.config.set(scheduler='processes')""")
    # except:
    #     raise("""Dask needs to use processes instead of threads because of the esmf backend.\nSet:\n\tdask.config.set(scheduler='processes')""")

    # see if we were given a single vtu file or a pvd xml file
    filename, file_extension = os.path.splitext(fname)

    timesteps = None
    dt = np.timedelta64(1,'s')

    if file_extension != '.pvd':
        raise Exception('Not a pvd file')

    parse = ET.parse(fname)
    pvd = parse.findall(".//*[@file]")

    timesteps = parse.findall(".//*[@timestep]")

    epoch = np.datetime64(int(timesteps[0].get('timestep')), 's')

    # before we can come up with a possible dt, we need to see if we have multiple rank MPI outputs which will have 1 file
    # per MPI rank with a [basename]<timestamp>_<MPIRANK>.vtu format

    ranks = []

    # print(pvd)
    # loop through to detect how many mpi ranks were used
    for f in pvd:

        m = re.findall('.+_([0-9]+).vtu', f.get('file'))
        m = int(m[0])
        if m not in ranks:
            ranks.append(m)
        else:
            break

    print(f'MPI ranks = {len(ranks)} detected in pvd')
    nranks = len(ranks)

    if len(timesteps) > 1 and len(timesteps) > nranks:
        dt = np.datetime64(int(timesteps[nranks].get('timestep')),'s') - np.datetime64(int(timesteps[0].get('timestep')),'s')
        print('dt='+str(dt))
    vtu_paths = []

    base_dir = pathlib.Path(fname).parent.absolute()
    for i in range(0, len(pvd), nranks):
        current_vtu_paths = []  # holds 1 timestep's vtu paths
        for r in range(nranks):
            vtu = pvd[i + r]
            vtu_file = vtu.get('file')
            path = os.path.join(base_dir, vtu_file)
            current_vtu_paths.append(path)

        vtu_paths.append(current_vtu_paths)

    print('Determining mesh extents...',end='')
    blocks = pv.MultiBlock([pv.read(f) for f in vtu_paths[0]])

    shape = _get_shape(blocks, dxdy)

    proj4 = blocks[0]["proj4"][0]
    print(f'proj4 = {proj4}')
    if variables is not None:
        # ensure we are not asking for an output that is a vector
        for v in variables:
            if len(blocks[0][v].shape) > 1:
                raise Exception('Cannot specify a variable that is a vector output shape = (n,3) ')
    else:
        variables = []
        blacklist = ['proj4', 'global_id']
        for v in blocks[0].array_names:
            if len(blocks[0][v].shape) == 1 and v not in blacklist: # don't add the vectors
                a = v.replace('[param] ','_param_').replace('/','') # sanitize the name, this should be fixed in CHM though
                variables.append(a)


    print('Creating regridding topology')
    # start_time = time.time()
    # num_node, nodeId, nodeCoord, nodeOwner, num_elem, elemId, elemType, elemConn, elemCoord, numX, numY, x_center, y_center, x_corner, y_corner = _build_partial_regridding_ds(blocks.combine(), dxdy)
    # print('Took ' + str(time.time() - start_time))

    delayed_vtu = {}
    for var in variables:
        delayed_vtu[var] = []

    # cell centres
    x_center = np.linspace(start=blocks.bounds[0] + dxdy / 2.,
                           stop=blocks.bounds[1] - dxdy / 2., num=shape[0])
    y_center = np.linspace(start=blocks.bounds[2] + dxdy / 2.,
                           stop=blocks.bounds[3] - dxdy / 2., num=shape[1])

    # This can't be done here as it can't be pickled
    # print(f'Building regridding ds' )
    # v = _load_vtu(vtu_paths[0])
    # mesh, grid = _build_regridding_ds(v.compute(), dxdy)


    blocks = None
    for v in vtu_paths:

        # print(v)
        # This prevents pickling
        # vtu = _load_vtu(v)

        for var in variables:
            vtu = v #_load_vtu(v)  # can done here to pickle, but then will end up being a bit more costly
            df = _regrid_mesh_to_grid(vtu, dxdy, var)
                                        # num_node, nodeId, nodeCoord, nodeOwner,
                                        #  num_elem, elemId, elemType, elemConn, elemCoord,
                                        #  numX, numY,
                                        #  x_center, y_center,
                                        #  x_corner, y_corner)
            d = da.from_delayed(df,
                                shape=shape,
                                dtype=np.dtype('float64'))

            tmp = xr.DataArray(d.T, name=var,
                               coords={'y': y_center,
                                       'x': x_center},
                               dims=['y', 'x'])
            delayed_vtu[var].append(tmp)

    end_time = np.datetime64(int(timesteps[-1].get('timestep')), 's')

    _dt = int(dt.astype("timedelta64[s]")/np.timedelta64(1, 's'))

    times = pd.date_range(start=epoch, end=end_time, freq=f'{_dt} s', name="time")

    for var, arrays in delayed_vtu.items():
        delayed_vtu[var] = xr.concat(arrays, dim=times)
        delayed_vtu[var] = delayed_vtu[var].chunk({'time': 1, 'x': -1, 'y': -1})

    ds = xr.Dataset(data_vars=delayed_vtu)


    # Compute the lon/lat coordinates with rasterio.warp.transform
    ny, nx = len(ds['y']), len(ds['x'])
    x, y = np.meshgrid(ds['x'], ds['y'])
    # Rasterio works with 1D arrays
    lon, lat = transform(proj4, 'EPSG:4326', x.flatten(), y.flatten())
    lon = np.asarray(lon).reshape((ny, nx))
    lat = np.asarray(lat).reshape((ny, nx))
    ds.coords['lon'] = (('y', 'x'), lon)
    ds.coords['lat'] = (('y', 'x'), lat)

    # Because the geo accessor can so easily lose information, set it like this so it is easily obtained
    ds = ds.assign_coords({'dxdy': dxdy})

    # we need to be really careful when do this assignment as these attrs can be easily lost, see:
    # https://github.com/corteva/rioxarray/issues/427
    # https://corteva.github.io/rioxarray/stable/getting_started/crs_management.html
    # https://corteva.github.io/rioxarray/stable/getting_started/manage_information_loss.html

    # sets internal rio crs
    ds = ds.rio.set_crs(proj4)

    # writes CRS in a CF compliant manner
    ds = ds.rio.write_crs(proj4)


    return ds



