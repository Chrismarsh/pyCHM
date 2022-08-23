import sys
import numpy as np
import ESMF
import xarray as xr
import rioxarray  # for xarray.rio
import os
from mpi4py import MPI
import osgeo_utils.gdal_merge
import glob
import itertools


def ugrid2tiff(ugrid_nc, dxdy=0.005, mesh_topology_nc=None, method='conservative'):
    mg = ESMF.Manager(debug=True)
    comm = MPI.COMM_WORLD

    # This creates the UGRID file

    # df = pc.open_pvd('/Users/cmarsh/Documents/science/model_runs/benchmark_problems/kan_pbsm_TC2020/BSCor3_Alb100_NoSubTopo_NoPomLi_K0p3_NoCplz0_Recirc20_WN1000_24/output_FSM_rhod600/SC.pvd')
    # df=df.set_index('datetime')['2017-11-01':'2018-04-03'].reset_index()
    # df = df.iloc[[0,30,60,90,120]]
    # pc.vtu_to_ugrid(df, 'test2.nc')
    # quit()

    # we might be loading a seperate mesh topology
    mnc = ugrid_nc if mesh_topology_nc is None else mesh_topology_nc

    mesh = ESMF.Mesh(filename=mnc,
                            filetype=ESMF.api.constants.FileFormat.UGRID,
                            meshname='Mesh2'
                     )


    # mesh._write_(f'{ESMF.local_pet()}-domain')

    nodes, elements = (0, 1)
    u, v = (0, 1)
    # dxdy = 0.0005 # degrees

    # communicate accross all the ranks to figure out the bounds of our mesh
    xmin_m = np.array([mesh.coords[nodes][u].min()])
    xmax_m = np.array([mesh.coords[nodes][u].max()])
    ymin_m = np.array([mesh.coords[nodes][v].min()])
    ymax_m = np.array([mesh.coords[nodes][v].max()])

    xmin = np.empty(1,dtype=np.float64)
    xmax = np.empty(1,dtype=np.float64)
    ymin = np.empty(1,dtype=np.float64)
    ymax = np.empty(1,dtype=np.float64)

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

    print(f'numX, numY = {numX}, {numY}')

    print(f'PET{ESMF.local_pet()} - umin={mesh.coords[nodes][u].min()} umax={mesh.coords[nodes][u].max()} vmin={mesh.coords[nodes][v].min()} vmax={mesh.coords[nodes][v].max()} ')

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
    print( f'PET{ESMF.local_pet()} max_index={max_index}')


    grid = ESMF.Grid(max_index, staggerloc=[ESMF.StaggerLoc.CENTER, ESMF.StaggerLoc.CORNER])

    # RLO: access Grid center coordinates
    gridXCenter = grid.get_coords(0)
    gridYCenter = grid.get_coords(1)

    # RLO-v2: adjust coordinate array to bounds of the current PET (rank)
    x_center_par = x_center[grid.lower_bounds[ESMF.StaggerLoc.CENTER][0]:grid.upper_bounds[ESMF.StaggerLoc.CENTER][0]]
    y_center_par = y_center[grid.lower_bounds[ESMF.StaggerLoc.CENTER][1]:grid.upper_bounds[ESMF.StaggerLoc.CENTER][1]]

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
    variables = list(df.keys())

    # don't convert these to tiff
    exclude_list = ['Mesh2', 'Mesh2_face_nodes', 'Mesh2_node_x', 'Mesh2_node_y', 'Mesh2_face_x', 'Mesh2_face_y', 'time', 'global_id' ]

    # the sort is important as otherwise this can have a different order on different mpi ranks
    variables = sorted(list(set(variables)-set(exclude_list)))

    srcfield = ESMF.Field(mesh, meshloc=ESMF.MeshLoc.ELEMENT)
    dstfield = ESMF.Field(grid, staggerloc=ESMF.StaggerLoc.CENTER)

    regrid_method = ESMF.RegridMethod.CONSERVE if method == 'conservative' else ESMF.RegridMethod.BILINEAR
    print(f'Using {regrid_method} regridder')
    regrid = ESMF.Regrid(srcfield, dstfield, regrid_method=regrid_method,
                         unmapped_action=ESMF.UnmappedAction.IGNORE)

    # get the global_id offsets this rank is using
    srcfield_offsets = ESMF.Field(mesh, meshloc=ESMF.MeshLoc.ELEMENT)
    srcfield_offsets.read(filename=mnc,
                          variable='global_id', timeslice=0)
    offsets = np.array(srcfield_offsets.data[:], dtype=np.int64) #these need to be ints to index with
    srcfield_offsets.destroy()
    srcfield_offsets = None

    #hold a list of the processed times so we don't have to recompute it when dealing with tiff merging
    processed_times = []

    for ts in range(0, df.time.shape[0]):

        time = str(df.time[ts].dt.strftime('%Y%m%dT%H%M%S').data)

        for var in variables:
            print(f'{time} - {var}')

            srcfield.data[:] = df.isel(time=ts)[var][offsets].data
            dstfield.data[...] = np.nan

            dstfield = regrid(srcfield, dstfield, zero_region=ESMF.Region.SELECT)

            tiff = xr.DataArray(dstfield.data.T, name=var,
                               coords={'y': y_center_par.data,
                                       'x': x_center_par.data
                                       },
                               dims=['y', 'x'])
            tiff = tiff.rio.write_nodata(-9999.0)
            tiff = tiff.rio.set_crs('+proj=longlat +datum=WGS84 +no_defs +type=crs')
            var_san = var.replace('[', '_').replace(']', '_')
            tiff.rio.to_raster(f'{ESMF.local_pet()}-{var_san}-{time}-output.tiff')

            # Wait to make sure everyone has written out this timestep + variable.
            comm.barrier()

        processed_times.append(time)


    product = None
    if ESMF.local_pet() == 0:

        var_san = [var.replace('[', '_').replace(']', '_') for var in variables]

        product = [x for x in itertools.product(var_san, processed_times)]
        product = np.array_split(product, ESMF.pet_count())

    product = comm.scatter(product, root=0)
    print(f'PET{ESMF.local_pet()} has {product}')

    for prod in product:
        var, time = prod
        files = glob.glob(f'*-{var}-{time}-output.tiff')
        if len(files) != ESMF.pet_count():
            raise Exception(f"Missing files for {var} {time}")

        parameters = ['', '-o', f"{var}-{time}.tiff"] + files + ['-co', 'COMPRESS=LZW']
        osgeo_utils.gdal_merge.main(parameters)

        for f in files:
            os.remove(f)

