import sys
import numpy as np
# sys.path.append("/Users/cmarsh/Documents/science/code/pyCHM/")
# import CHM as pc
import ESMF
import xarray as xr
import rioxarray  # for xarray.rio
import os
# from mpi4py import MPI
from osgeo import gdal, ogr, osr

import pyvista as pv

def write_shp_from_vtu(fname, vtu_fname):
    mesh = pv.read(vtu_fname)

    # Create the shape file to hold the triangulation

    driver = ogr.GetDriverByName("ESRI Shapefile")

    try:
        os.remove(fname)  # remove if existing
    except OSError:
        pass

    output_usm = driver.CreateDataSource(fname)

    srs_out = osr.SpatialReference()

    srs_out.ImportFromProj4(mesh["proj4"][0])

    layer = output_usm.CreateLayer('mesh', srs_out, ogr.wkbPolygon)

    layer.CreateField(ogr.FieldDefn("globalID", ogr.OFTReal))

    for i in range(mesh.n_cells):

        # we need this to do the area calculation
        ring = ogr.Geometry(ogr.wkbLinearRing)

        v0 = mesh.GetCell(i).GetPoints().GetData().GetTuple(0)
        v1 = mesh.GetCell(i).GetPoints().GetData().GetTuple(1)
        v2 = mesh.GetCell(i).GetPoints().GetData().GetTuple(2)

        ring.AddPoint(v0[0], v0[1])
        ring.AddPoint(v1[0], v1[1])
        ring.AddPoint(v2[0], v2[1])
        ring.AddPoint(v0[0], v0[1])  # add again to complete the ring.

        # need this for the area calculation
        tpoly = ogr.Geometry(ogr.wkbPolygon)
        tpoly.AddGeometry(ring)

        feature = ogr.Feature(layer.GetLayerDefn())
        feature.SetGeometry(tpoly)

        output = float(mesh['global_id'][i])
        if output == 1529:
            feature.SetField('globalID', output)

        layer.CreateFeature(feature)

    output_usm.FlushCache()
    output_usm = None  # close file


# write_shp('lol.shp',"SC1601510400_0.vtu")
ESMF.Manager(debug=True)
# comm = MPI.COMM_WORLD

# if ESMF.local_pet() == 0:
#     pc.vtu_to_ugrid("SC1601510400_0.vtu")
#     quit()

mesh = ESMF.Mesh(filename='test.nc',
                        filetype=ESMF.api.constants.FileFormat.UGRID,
                        meshname='Mesh2')
                        # convert_to_dual=True)

mesh._write_(f'{ESMF.local_pet()}-domain')

srcfield = ESMF.Field(mesh, name="global_id", meshloc=ESMF.MeshLoc.ELEMENT)
srcfield.read(filename="test.nc",
            variable="globalID", timeslice=1)

x = np.abs(mesh.get_coords(0).max() - mesh.get_coords(0).min())
y = np.abs(mesh.get_coords(1).max() - mesh.get_coords(1).min())

nodes, elements = (0, 1)
u, v = (0, 1)
dxdy = 0.1 # degrees
numX, numY = int(x/dxdy), int(y/dxdy)

print(numX, numY)

print( f'PET{ESMF.local_pet()} - umin={mesh.coords[nodes][u].min()} umax={mesh.coords[nodes][u].max()} vmin={mesh.coords[nodes][v].min()} vmax={mesh.coords[nodes][v].max()} ')

# cell centres
x_center = np.linspace(start=mesh.coords[nodes][u].min() + dxdy / 2.,
                       stop=mesh.coords[nodes][u].max() - dxdy / 2., num=numX)
y_center = np.linspace(start=mesh.coords[nodes][v].min() + dxdy / 2.,
                       stop=mesh.coords[nodes][v].max() - dxdy / 2., num=numY)


# node coords
x_corner = np.linspace(start=mesh.coords[nodes][u].min(), stop=mesh.coords[nodes][u].max(), num=numX + 1)
y_corner = np.linspace(start=mesh.coords[nodes][v].min(), stop=mesh.coords[nodes][v].max(), num=numY + 1)


max_index = np.array([len(x_center), len(y_center)])

# grid = ESMF.Grid(max_index, staggerloc=[ESMF.StaggerLoc.CENTER, ESMF.StaggerLoc.CORNER])
grid = ESMF.Grid(max_index, staggerloc=ESMF.StaggerLoc.CENTER)

# RLO: access Grid center coordinates
gridXCenter = grid.get_coords(0)
gridYCenter = grid.get_coords(1)

# RLO-v2: adjust coordinate array to bounds of the current PET (rank)
x_center_par = x_center[grid.lower_bounds[ESMF.StaggerLoc.CENTER][0]:grid.upper_bounds[ESMF.StaggerLoc.CENTER][0]]
y_center_par = y_center[grid.lower_bounds[ESMF.StaggerLoc.CENTER][1]:grid.upper_bounds[ESMF.StaggerLoc.CENTER][1]]

print(f'{ESMF.local_pet()} = lower={grid.lower_bounds[ESMF.StaggerLoc.CENTER]} upper={grid.upper_bounds[ESMF.StaggerLoc.CENTER]}')

np.savetxt(f'{ESMF.local_pet()}-x_center_par.txt', x_center_par.reshape((x_center_par.size, 1)))
np.savetxt(f'{ESMF.local_pet()}-y_center_par.txt', y_center_par.reshape((y_center_par.size, 1)))

# RLO: set Grid center coordinates as a 2D array (this can also be done 1d)
gridXCenter[...] = x_center_par.reshape((x_center_par.size, 1))
gridYCenter[...] = y_center_par.reshape((1, y_center_par.size))

# RLO: access Grid corner coordinates
# gridXCorner = grid.get_coords(0, staggerloc=ESMF.StaggerLoc.CORNER)
# gridYCorner = grid.get_coords(1, staggerloc=ESMF.StaggerLoc.CORNER)

# # RLO-v2: adjust coordinate array to bounds of the current PET (rank)
x_corner_par = x_corner[grid.lower_bounds[ESMF.StaggerLoc.CORNER][0]:grid.upper_bounds[ESMF.StaggerLoc.CORNER][0]]
y_corner_par = y_corner[grid.lower_bounds[ESMF.StaggerLoc.CORNER][1]:grid.upper_bounds[ESMF.StaggerLoc.CORNER][1]]

# # RLO: set Grid corner coordinats as a 2D array
# gridXCorner[...] = x_corner_par.reshape((x_corner_par.size, 1))
# gridYCorner[...] = y_corner_par.reshape((1, y_corner_par.size))

np.savetxt(f'{ESMF.local_pet()}-x_corner_par.txt', x_corner_par.reshape((x_corner_par.size, 1)))
np.savetxt(f'{ESMF.local_pet()}-y_corner_par.txt', y_corner_par.reshape((y_corner_par.size, 1)))


grid._write_(f'{ESMF.local_pet()}-grid')

dstfield = ESMF.Field(grid, meshloc=ESMF.MeshLoc.ELEMENT)
dstfield.data[...] = np.nan


regrid = ESMF.Regrid(srcfield, dstfield, regrid_method=ESMF.RegridMethod.BILINEAR,
                                        unmapped_action=ESMF.UnmappedAction.IGNORE)

dstfield = regrid(srcfield, dstfield)


tmp = xr.DataArray(dstfield.data.T, name='test',
                   coords={'y': y_center_par.data,
                           'x': x_center_par.data
                           },
                   dims=['y', 'x'])
tmp.rio.to_raster(f'{ESMF.local_pet()}-output.tiff')

print(tmp)

