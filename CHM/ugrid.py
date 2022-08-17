import netCDF4 as nc
import numpy as np
import pyvista as pv
from pyproj import Transformer, transform, CRS



def vtu_to_ugrid(path=""):
    outnc = 'test.nc'
    ds = nc.Dataset(outnc, 'w', format='NETCDF4')

    mesh = pv.read(path)

    proj4 = mesh["proj4"][0] #blocks[0]["proj4"][0]
    print(f'proj4 = {proj4}')

    nMesh2_node = ds.createDimension('nMesh2_node', mesh.n_points)
    nMesh2_face = ds.createDimension('nMesh2_face', mesh.n_cells)

    # nMesh2_edge = ds.createDimension('nMesh2_edge', 5)

    timedim = ds.createDimension('time', None)

    nMesh2_face = ds.createDimension('Two', 2)
    nMesh2_face = ds.createDimension('Three', 3)


    Mesh2 = ds.createVariable('Mesh2', 'i4')

    Mesh2.setncattr('cf_role', 'mesh_topology')
    Mesh2.setncattr('long_name', "Topology data of 2D unstructured mesh")
    Mesh2.setncattr('topology_dimension', 2)
    Mesh2.setncattr('node_coordinates', "Mesh2_node_x Mesh2_node_y")
    Mesh2.setncattr('face_node_connectivity', "Mesh2_face_nodes")
    Mesh2.setncattr('face_dimension', "nMesh2_face")
    Mesh2.setncattr('edge_dimension', "nMesh2_edge")

    # Mesh2.setncattr('edge_node_connectivity', "Mesh2_edge_nodes") # optional
    # Mesh2.setncattr('edge_coordinates', "Mesh2_edge_x Mesh2_edge_y") # optional
    Mesh2.setncattr('face_coordinates', "Mesh2_face_x Mesh2_face_y") # optional
    # Mesh2.setncattr('face_edge_connectivity', "Mesh2_face_edges") # optional
    # Mesh2.setncattr('face_face_connectivity', "Mesh2_face_links") # optional
    # Mesh2.setncattr('edge_face_connectivity', "Mesh2_edge_face_links") # optional

    Mesh2_face_nodes = ds.createVariable('Mesh2_face_nodes', 'i4', ("nMesh2_face", "Three"))
    Mesh2_face_nodes.setncattr('cf_role', 'face_node_connectivity')
    Mesh2_face_nodes.setncattr('long_name', "Maps every triangular face to its three corner nodes.")


    # Mesh2_edge_nodes = ds.createVariable('Mesh2_edge_nodes', 'i4', ("nMesh2_edge", "Two"))
    # Mesh2_edge_nodes.setncattr('cf_role', 'edge_node_connectivity')
    # Mesh2_edge_nodes.setncattr('long_name', "Maps every edge to the two nodes that it connects.")


    Mesh2_node_x = ds.createVariable('Mesh2_node_x', 'f4', "nMesh2_node")
    Mesh2_node_x.setncattr('standard_name', "longitude")
    Mesh2_node_x.setncattr('long_name', "Longitude of 2D mesh nodes.")
    Mesh2_node_x.setncattr('units', "degrees_east")


    Mesh2_node_y = ds.createVariable('Mesh2_node_y', 'f4', "nMesh2_node")
    Mesh2_node_y.setncattr('standard_name', "latitude")
    Mesh2_node_y.setncattr('long_name', "Latitude of 2D mesh nodes.")
    Mesh2_node_y.setncattr('units', "degrees_north")


    # face coordinadates

    Mesh2_face_x = ds.createVariable('Mesh2_face_x', 'f4', "nMesh2_face")
    Mesh2_face_x.setncattr('standard_name', "longitude")
    Mesh2_face_x.setncattr('long_name', "Characteristics longitude of 2D mesh triangle (e.g. circumcenter coordinate).")
    Mesh2_face_x.setncattr('units', "degrees_east")

    Mesh2_face_y = ds.createVariable('Mesh2_face_y', 'f4', "nMesh2_face")
    Mesh2_face_y.setncattr('standard_name', "latitude")
    Mesh2_face_y.setncattr('long_name', "Characteristics latitude of 2D mesh triangle (e.g. circumcenter coordinate).")
    Mesh2_face_y.setncattr('units', "degrees_north")

    pts = pv.convert_array(mesh.GetPoints().GetData())

    Mesh2_node_x[:] = pts[:, 0]
    Mesh2_node_y[:] = pts[:, 1]

    # cx = np.empty(mesh.n_cells, dtype=np.int)
    # cy = np.empty(mesh.n_cells, dtype=np.int)

    facenodes = mesh.cell_connectivity
    facenodes = facenodes.reshape(mesh.n_cells, 3)

    centers = pv.convert_array(mesh.cell_centers().GetPoints().GetData())
    # for i in range(0, mesh.n_cells):
    #     v0 = mesh.GetCell(i).GetPointId(0)
    #     v1 = mesh.GetCell(i).GetPointId(1)
    #     v2 = mesh.GetCell(i).GetPointId(2)
    #
    #     centreX = 1 / 3 * (Mesh2_node_x[v0] + Mesh2_node_x[v1] + Mesh2_node_x[v2])
    #     centreY = 1 / 3 * (Mesh2_node_y[v0] + Mesh2_node_y[v1] + Mesh2_node_y[v2])
    #     cx[i] = centreX
    #     cy[i] = centreY


    Mesh2_face_nodes[:] = facenodes

    x = np.abs(mesh.bounds[0] - mesh.bounds[1])
    y = np.abs(mesh.bounds[2] - mesh.bounds[3])

    def reproject(x, y, proj4):
        trans = Transformer.from_proj(
            # proj4,
            CRS.from_proj4(
                str(proj4)),
                '+proj=longlat +datum=WGS84 +no_defs +type=crs',
            always_xy=True,
        )
        xx, yy = trans.transform(x, y)

        return xx, yy



    Mesh2_node_x[:], Mesh2_node_y[:] = reproject(Mesh2_node_x[:],
                                                 Mesh2_node_y[:], proj4)


    Mesh2_face_x[:], Mesh2_face_y[:] = reproject(centers[:,0], centers[:,1], proj4)


    time = ds.createVariable('time', 'f4', ('time',))
    time.long_name = 'time'
    time.units = "hours since 2022-08-11 00:00:00"
    time.calendar = "standard"
    time[0] = [0]

    globalID = ds.createVariable('globalID', 'f4', ('time', 'nMesh2_face'))
    globalID.mesh = "Mesh2"
    globalID.location = "face"
    globalID.coordinates = "Mesh2_face_x Mesh2_face_y"
    globalID[0, :] = mesh['global_id']

    ds.close()

