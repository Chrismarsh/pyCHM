import pyvista as pv
import xarray as xr
import os
import xml.etree.ElementTree as ET
import numpy as np
import pandas as pd
import dask
import dask.array as da
import time
import re
import ESMF
import pathlib

esmf_mesh_geom = None

def _get_shape(mesh, dxdy):
    # number of cells @ this dxdy
    # numX = int((Emesh.coords[nodes][u].max() - Emesh.coords[nodes][u].min()) / dxdy)
    # numY = int((Emesh.coords[nodes][v].max() - Emesh.coords[nodes][v].min()) / dxdy)

    x = np.abs(mesh.bounds[0] - mesh.bounds[1])
    y = np.abs(mesh.bounds[2] - mesh.bounds[3])

    return int(x/dxdy),int(y/dxdy)


@dask.delayed
def _regrid_mesh_to_grid(mesh, grid, fname, var):
    vtu = pv.MultiBlock([pv.read(f) for f in fname])
    vtu = vtu.combine()

    srcfield = ESMF.Field(mesh, name=var, meshloc=ESMF.MeshLoc.ELEMENT)
    srcfield.data[:] = vtu[var]
    dstfield = ESMF.Field(grid, var)
    regrid = ESMF.Regrid(srcfield, dstfield, regrid_method=ESMF.RegridMethod.CONSERVE_2ND, unmapped_action=ESMF.UnmappedAction.IGNORE)
    out = regrid(srcfield, dstfield)

    df = da.from_array(out.data)
    return df

@dask.delayed
def _build_regridding_ds(fname, dxdy):

    mesh = pv.MultiBlock([pv.read(f) for f in fname])
    mesh = mesh.combine()

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



def pvd_to_xarray(fname, dxdy=30, variables=None):
    """
    Opens a single vtu file or a pvd file and returns a delayed xarray object
    timestep basis
    :param fname: pvd file path
    :return:
    """

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

    print(f'MPI ranks = {len(ranks)} detected in vtu')
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

    blocks = pv.MultiBlock([pv.read(f) for f in vtu_paths[0]])

    shape = _get_shape(blocks, dxdy)

    if variables is not None:
        # ensure we are not asking for an output that is a vector
        for v in variables:
            if len(blocks[0][v].shape) > 1:
                raise Exception('Cannot specify a variable that is a vector output shape = (n,3) ')
    else:
        variables = []
        blacklist = ['proj4']
        for v in blocks[0].array_names:
            if len(blocks[0][v].shape) == 1 and v not in blacklist: # don't add the vectors
                a = v.replace('[param] ','_param_').replace('/','') # sanitize the name, this should be fixed in CHM though
                variables.append(a)


    delayed_vtu = {}
    for var in variables:
        delayed_vtu[var] = []

    # cell centres
    x_center = np.linspace(start=blocks.bounds[0] + dxdy / 2.,
                           stop=blocks.bounds[1] - dxdy / 2., num=shape[0])
    y_center = np.linspace(start=blocks.bounds[2] + dxdy / 2.,
                           stop=blocks.bounds[3] - dxdy / 2., num=shape[1])

    res = _build_regridding_ds(vtu_paths[0], dxdy)
    esmf_mesh, esmf_grid = res[0],res[1]
    # flush
    blocks = None
    for v in vtu_paths:
        for var in variables:
            df = _regrid_mesh_to_grid(esmf_mesh, esmf_grid, v, var)

            d = da.from_delayed(df,
                                shape=shape,
                                dtype=np.dtype('float64'))

            tmp = xr.DataArray(d, name=var, dims=['y','x'], coords=[('x', x_center), ('y', y_center)])

            delayed_vtu[var].append(tmp.T)

    end_time = np.datetime64(int(timesteps[-1].get('timestep')), 's')

    _dt = int(dt.astype("timedelta64[s]")/np.timedelta64(1, 's'))

    times = pd.date_range(start = epoch, end = end_time, freq= f'{_dt} s' , name="time")

    for key, vtus in delayed_vtu.items():
        delayed_vtu[key] = xr.concat(vtus, dim=times)
        delayed_vtu[key].chunk({'time':1})

    return xr.Dataset(data_vars=delayed_vtu)



