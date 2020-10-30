import vtk
from vtk.util import numpy_support as VN

import xml.etree.ElementTree as ET
import numpy as np
import os


def mesh():

    vtu_files = []
    def __init__(self):
        pass

    def open_mesh(self, fname):
        """
        Opens a single vtu file or a pvd file. Only gets file handles. The actual read needs to be done on a per
        timestep basis
        :param self:
        :param fname: vtu or pvd file path
        :return:
        """

        # see if we were given a single vtu file or a pvd xml file
        filename, file_extension = os.path.splitext(fname)
        is_pvd = False
        pvd = [fname]  # if not given a pvd file, make this iterable for the below code
        timesteps = None
        dt = 1
        epoch = np.datetime64(0,'s')

        if file_extension == '.pvd':
            print('Detected pvd file, processing all linked vtu files')
            is_pvd = True
            parse = ET.parse(fname)
            pvd = parse.findall(".//*[@file]")

            timesteps = parse.findall(".//*[@timestep]")

            epoch = np.datetime64(int(timesteps[0].get('timestep')),'s')
            if len(timesteps) > 1:
                dt = int(timesteps[1].get('timestep')) - int(timesteps[0].get('timestep'))

        for vtu in pvd:
            path = vtu
            vtu_file = ''
            if is_pvd:
                vtu_file = vtu.get('file')
                path = fname[:fname.rfind('/') + 1] + vtu_file
            else:
                base = os.path.basename(path)  # since we have a full path to vtu, we need to get just the vtu filename
                vtu_file = os.path.splitext(base)[0]  # we strip out vtu later so keep here

            vtu_files.append(vtu_file)

    def read_timestep(self, timestep):
        if timestep > len(vtu_files):
            raise Exception("Index out of bounds")

        reader = vtk.vtkXMLUnstructuredGridReader()
        reader.SetFileName(vtu_files[timestep])

        mesh = reader.GetOutput()

        return mesh

    def get_all_triangle(self, idx):

        mesh = self.read_timestep(idx)
        # Get coords
        points = VN.vtk_to_numpy(mesh.GetPoints().GetData())

        # Get location of nodes
        X = [cpt[0] for cpt in points]
        Y = [cpt[1] for cpt in points]
        E = [cpt[2] for cpt in points]

        # Build face adjacency index
        triang = []
        for i in range(0, mesh.GetNumberOfCells()):
            v0 = mesh.GetCell(i).GetPointId(0)
            v1 = mesh.GetCell(i).GetPointId(1)
            v2 = mesh.GetCell(i).GetPointId(2)

            triang.append([v0, v1, v2])

        return {'X': X, 'Y': Y, 'E': E, 'triang': triang}

    def get_mesh_as_numpy(self, timestep, variable):

        mesh = self.read_timestep(timestep)
        cvar = VN.vtk_to_numpy(mesh.GetCellData().GetArray(variable))

        return cvar