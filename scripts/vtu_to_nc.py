import pyvista as pv
import xml.etree.ElementTree as ET
import pathlib
import numpy as np
import re
import os
import sys
import pandas as pd
import xarray as xr


def pvd_to_nc(fname, variables=None):

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



    time_sel: int = -48
    vtu = pv.MultiBlock([pv.read(f) for f in vtu_paths[time_sel]])
    vtu = vtu.combine()

    # var = variables[-48]

    df = pd.DataFrame({'global_id': vtu['global_id'],
                       'swe': vtu['swe']})

    d = df.to_xarray()
    d.to_netcdf('swe_today.nc')

    #
    # if variables is not None:
    #     # ensure we are not asking for an output that is a vector
    #     for v in variables:
    #         if len(blocks[0][v].shape) > 1:
    #             raise Exception('Cannot specify a variable that is a vector output shape = (n,3) ')
    # else:
    #     variables = []
    #     blacklist = ['proj4', 'global_id']
    #     for v in blocks[0].array_names:
    #         if len(blocks[0][v].shape) == 1 and v not in blacklist: # don't add the vectors
    #             a = v.replace('[param] ','_param_').replace('/','') # sanitize the name, this should be fixed in CHM though
    #             variables.append(a)

    # delayed_vtu = {}
    # for var in variables:
    #     delayed_vtu[var] = []
    #
    # for v in vtu_paths:
    #
    #     # print(v)
    #     # This prevents pickling
    #     # vtu = _load_vtu(v)
    #
    #     for var in variables:
    #         vtu = _load_vtu(v)


if __name__ == '__main__':

    pvd_to_nc(sys.argv[1], variables=['swe'])

