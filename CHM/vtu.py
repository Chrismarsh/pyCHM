import pyvista as pv
import xarray as xr
import os
import xml.etree.ElementTree as ET
import numpy as np
import pandas as pd
import time
import itertools
import dask
import dask.bag as db
import dask.array as da
# from concurrent import futures
# from tqdm import tqdm
from functools import partial
import re
import ESMF
import pathlib
# import gc

from multiprocessing import Pool

# from memory_profiler import profile

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

    def _dowork_toraster(self, crs_in, timefrmtstr, crs_out, d):

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
            d = d.squeeze().drop('lat').drop('lon').drop('time')
            d.rio.set_crs(crs_in)
            d = d.rio.reproject(crs_out)

        tmp_tiff = f'{name}_{dxdy}x{dxdy}_{time}.tif'
        print(f'{tmp_tiff}')
        d.rio.to_raster(tmp_tiff)

        return [0]

    def to_raster(self, crs_out=None, timefrmtstr='%Y-%m-%dT%H%M%S'):
        for t in range(0, len(self._obj.time.values)):
            for v in list(self._obj.keys()):
                df = self._obj.isel(time=t)[v]
                self._dowork_toraster(self._obj.rio.crs, timefrmtstr, crs_out, df)

    def to_raster_dask(self, var=None, crs_out=None, timefrmtstr='%Y-%m-%dT%H%M%S', client=None):
        """
        Accessible from the xarray object, e.g., ``df.chm.to_raster(...)``. This allows for converting the entire data array into georeferenced tiffs.
        Will work on every timestep in parallel. Doing so requires dask to be configured to use processes and not threads, as the underlying regridding algorithm is not
        thread safe.

        :param timefrmtstr: Time format string for output
        :param var: List of variables to convert, otherwise all are converted
        :param crs_out: Output CRS to reproject to, otherwise uses source CRS
        :return:
        """

        if var is None:
            var = list(self._obj.keys())
        elif var is not isinstance(object, list):
            var = [var]

        work = []
        # fn = xr.apply_ufunc(_dowork_toraster, self._obj, kwargs={'crs_in': self._obj.rio.crs, 'timefrmtstr': timefrmtstr, 'crs_out': crs_out},dask='allowed')
        # print(fn)
        # mapped = xr.map_blocks(_dowork_toraster, self._obj, kwargs={'crs_in': self._obj.rio.crs, 'timefrmtstr': timefrmtstr, 'crs_out': crs_out}, template=self._obj)
        # mapped.compute()

        if client is not None:
            print('using fire and forget code path')

        total = 0
        for t in range(0, len(self._obj.time.values)):
            for v in var:
                df = self._obj.isel(time=t)[v]# .copy()
                total = total + 1

                # if client is None:
                task = dask.delayed(self._dowork_toraster)(self._obj.rio.crs, timefrmtstr, crs_out, df)
                # task = (self._obj.rio.crs, timefrmtstr, crs_out, df)
                work.append(task)

        b = db.from_delayed(work)
        b.compute()



def _get_shape(mesh, dxdy):
    # number of cells @ this dxdy
    x = np.abs(mesh.bounds[0] - mesh.bounds[1])
    y = np.abs(mesh.bounds[2] - mesh.bounds[3])

    return int(x/dxdy), int(y/dxdy)


def _load_vtu(fname):
    # print('Loading and combining vtu...',end='')
    vtu = pv.MultiBlock([pv.read(f) for f in fname])
    vtu = vtu.combine()
    return vtu


def _regrid_mesh_to_grid_future(dxdy, proj4, x_center, y_center, regridding_method, write_weights, weight_file, args):

    vtu_timestep, var  = args
    vtu = vtu_timestep[0]
    timesteps = vtu_timestep[1]

    df = _regrid_mesh_to_grid(vtu, dxdy, var, regridding_method, write_weights, weight_file)

    tmp = xr.DataArray(df, name=var,
                       coords={'y': y_center,
                               'x': x_center},
                       dims=['y', 'x'])
    data_vars={}

    #just a 1 ts range
    t = pd.date_range(start=timesteps, end=timesteps, name="time")

    data_vars[var] = xr.concat([tmp], dim=t)

    ds = xr.Dataset(data_vars=data_vars)

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

    ds.chm.to_raster()



def _regrid_mesh_to_grid(v, dxdy, var, regridding_method, write_weights=False, weight_file=None):

    print(f'_regrid_mesh_to_grid called for {var}')
    start_time = time.time()
    vtu = _load_vtu(v)
    mesh, grid = _build_regridding_ds(vtu, dxdy)

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

    method = ESMF.RegridMethod.BILINEAR
    if regridding_method == 'CONSERVE':
        method = ESMF.RegridMethod.CONSERVE

    wfile = None
    if write_weights:
        wfile = 'weights.nc'

    regrid = None
    if weight_file is None:
        regrid = ESMF.Regrid(srcfield, dstfield, regrid_method=method,
                             unmapped_action=ESMF.UnmappedAction.IGNORE, filename=wfile)
    else:
        regrid = ESMF.RegridFromFile(srcfield, dstfield, weight_file)

    out = regrid(srcfield, dstfield, zero_region=ESMF.Region.SELECT)

    df = da.from_array(out.data)
    df = df.T

    print('destroy call')

    srcfield.destroy()
    dstfield.destroy()
    mesh.destroy()
    regrid.destroy()
    out.destroy()

    print('Took ' + str(time.time() - start_time) + ' to regrid')

    return df


def _build_regridding_ds(mesh, dxdy):

    # print('_build_regridding_ds called')

    nodes, elements = (0, 1)
    u, v = (0, 1)

    Emesh = ESMF.Mesh(parametric_dim=2, spatial_dim=2)
    num_node = mesh.n_points
    num_elem = mesh.n_cells

    nodeId = np.linspace(0, num_node, num_node)

    # store as x,y pairs in sequence
    # [ x0,y0,
    #   x1,y1. [...] ]
    nodeCoord = np.empty(num_node * 2)
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
        elemId[i // 3] = i // 3

        elemType[i // 3] = ESMF.MeshElemType.TRI

        v0 = mesh.GetCell(i // 3).GetPointId(0)
        v1 = mesh.GetCell(i // 3).GetPointId(1)
        v2 = mesh.GetCell(i // 3).GetPointId(2)

        # nodes that make up the triangulation
        elemConn[i] = v0
        elemConn[i + 1] = v2
        elemConn[i + 2] = v1

        # Centroid
        # i = x + width*y;
        centreX = 1 / 3 * (nodeCoord[2*v0 + 0] + nodeCoord[2*v1 + 0] + nodeCoord[2*v2 + 0])
        centreY = 1 / 3 * (nodeCoord[2*v0 + 1] + nodeCoord[2*v1 + 1] + nodeCoord[2*v2 + 1])

        #i = x + width*y;
        elemCoord[2*(i // 3) + 0] = centreX
        elemCoord[2*(i // 3) + 1] = centreY

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

        tmp = xr.DataArray(d, name=var,
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

def pvd_to_tiff(fname, dxdy=50, variables=None, regridding_method='BILINEAR', nworkers=4, save_weights=False):
    timesteps, vtu_paths = read_pvd(fname)

    print('Determining mesh extents...', end='')
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
        excludelist = ['proj4', 'global_id']
        for v in blocks[0].array_names:
            if len(blocks[0][v].shape) == 1 and v not in excludelist: # don't add the vectors
                a = v.replace('[param] ','_param_').replace('/','') # sanitize the name, this should be fixed in CHM though
                variables.append(a)

    # cell centres
    x_center = np.linspace(start=blocks.bounds[0] + dxdy / 2.,
                           stop=blocks.bounds[1] - dxdy / 2., num=shape[0])
    y_center = np.linspace(start=blocks.bounds[2] + dxdy / 2.,
                           stop=blocks.bounds[3] - dxdy / 2., num=shape[1])

    vtu_var = [x for x in itertools.product(zip(vtu_paths, timesteps), variables)]

    # serial testing
    # for vv in vtu_var:
    #    _regrid_mesh_to_grid_future(dxdy, proj4, x_center, y_center, regridding_method, vv)

    pool = Pool(processes=nworkers, maxtasksperchild=1)

    if save_weights:
        #writting weights
        pool.map(partial(_regrid_mesh_to_grid_future, dxdy, proj4, x_center, y_center, regridding_method, True, None), [vtu_var[0]])

        pool.map( partial(_regrid_mesh_to_grid_future, dxdy, proj4, x_center, y_center, regridding_method, False, 'weights.nc'), vtu_var[1:])
    else:
        pool.map(partial(_regrid_mesh_to_grid_future, dxdy, proj4, x_center, y_center, regridding_method, False, None), vtu_var)

    return

def pvd_to_xarray(fname, dxdy=50, variables=None, regridding_method='BILINEAR'):
    """
    Opens a pvd file and returns a Dask delayed xarray object. As it is a delayed object,
    once a specific timestep's variable is operated upon, the delayed object is computed
    and regridded.


    :param fname: pvd file path
    :param dxdy: spatial resolution in metres to regrid to.
    :param variables: List of variables to keep from the loaded vtu file
    :param regridding_method 'BILINEAR' or 'CONSERVE'
    :return:
    """

    # try:
    #     if dask.config.get('scheduler') != 'processes':
    #         raise("""Dask needs to use processes instead of threads because of the esmf backend.\nSet:\n\tdask.config.set(scheduler='processes')""")
    # except:
    #     raise("""Dask needs to use processes instead of threads because of the esmf backend.\nSet:\n\tdask.config.set(scheduler='processes')""")

    timesteps, vtu_paths = read_pvd(fname)

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
        excludelist = ['proj4', 'global_id']
        for v in blocks[0].array_names:
            if len(blocks[0][v].shape) == 1 and v not in excludelist: # don't add the vectors
                a = v.replace('[param] ','_param_').replace('/','') # sanitize the name, this should be fixed in CHM though
                variables.append(a)

    print('Creating regridding topology')

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
    for vtu in vtu_paths:
        # This prevents pickling
        # vtu = _load_vtu(v)

        for var in variables:

            df = dask.delayed(_regrid_mesh_to_grid)(vtu, dxdy, var, regridding_method)

            d = da.from_delayed(df,
                                shape=(shape[1], shape[0]), # allows us to skip the d.T below as rasterio et al want y,x coords
                                dtype=np.dtype('float64'))

            tmp = xr.DataArray(d, name=var,
                               coords={'y': y_center,
                                       'x': x_center},
                               dims=['y', 'x'])
            delayed_vtu[var].append(tmp)



    for var, arrays in delayed_vtu.items():
        delayed_vtu[var] = xr.concat(arrays, dim=timesteps)
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


def read_pvd(fname):
    # see if we were given a single vtu file or a pvd xml file
    filename, file_extension = os.path.splitext(fname)
    timesteps = None
    dt = np.timedelta64(1, 's')
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
        dt = np.datetime64(int(timesteps[nranks].get('timestep')), 's') - np.datetime64(int(timesteps[0].get('timestep')), 's')
        print('dt=' + str(dt))
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
    end_time = np.datetime64(int(timesteps[-1].get('timestep')), 's')
    _dt = int(dt.astype("timedelta64[s]") / np.timedelta64(1, 's'))
    times = pd.date_range(start=epoch, end=end_time, freq=f'{_dt} s', name="time")
    return times, vtu_paths



