import xml.etree.ElementTree as ET
import numpy as np
import pandas as pd
import re
import pathlib
import os
from osgeo import gdal, ogr, osr
import pyvista as pv

@pd.api.extensions.register_dataframe_accessor("chm")

class PdGeoAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self.dt = None



def open_pvd(fname):

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

    df = pd.DataFrame({'datetime':times, 'vtu_paths':vtu_paths})
    # df = df.set_index('datetime')

    df.chm.dt = dt

    return df

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