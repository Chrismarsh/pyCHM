import netCDF4 as nc
import pyvista as pv
from pyproj import Transformer, transform, CRS
from cftime import date2num

def vtu_to_ugrid(pvd, outnc, append=False, variables=None, only_topology=False):

    #load the first timestep to get the topology information from
    blocks = pv.MultiBlock([pv.read(f) for f in pvd.iloc[0].vtu_paths])

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
                a = v.replace('[param] ', '_param_').replace('/', '') # sanitize the name, this should be fixed in CHM though
                variables.append(a)

    print('Merging blocks...')
    mesh = blocks.combine(merge_points=True)
    print('Done')

    def write_mesh(outnc, mesh):
        writemode = 'a' if append else 'w'
        ds = nc.Dataset(outnc, writemode, format='NETCDF4')

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

        nc_var = ds.createVariable('global_id', 'f4', ('nMesh2_face'))
        nc_var.mesh = "Mesh2"
        nc_var.location = "face"
        nc_var.coordinates = "Mesh2_face_x Mesh2_face_y"
        nc_var[:] = mesh['global_id']

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

        facenodes = mesh.cell_connectivity
        facenodes = facenodes.reshape(mesh.n_cells, 3)

        centers = pv.convert_array(mesh.cell_centers().GetPoints().GetData())

        Mesh2_face_nodes[:] = facenodes

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


        Mesh2_face_x[:], Mesh2_face_y[:] = reproject(centers[:,0],
                                                     centers[:,1], proj4)

        return ds

    print('Writing mesh topology')
    # write just the topology
    ds = write_mesh('mesh.nc', mesh)

    nc_var = ds.createVariable('global_id', 'f4', ('nMesh2_face'))
    nc_var.mesh = "Mesh2"
    nc_var.location = "face"
    nc_var.coordinates = "Mesh2_face_x Mesh2_face_y"
    nc_var[:] = mesh['global_id']

    ds.close()

    if only_topology:
        return

    print('Writting mesh + variables')
    ds = write_mesh(outnc, mesh)

    time = ds.createVariable('time', 'f4', ('time',))
    time.long_name = 'time'
    time.units = f"seconds since {str(pvd.datetime[0])}"
    time.calendar = "standard"


    dates = [ d for d in pvd.datetime ]
    time[:] = date2num(dates, units=time.units, calendar=time.calendar)

    # Create all the varibles
    for var in variables:
        nc_var = ds.createVariable(var, 'f4', ('time', 'nMesh2_face'))
        nc_var.mesh = "Mesh2"
        nc_var.location = "face"
        nc_var.coordinates = "Mesh2_face_x Mesh2_face_y"

    i = 0
    for idx, row in pvd.iterrows():
        blocks = pv.MultiBlock([pv.read(f) for f in row.vtu_paths])
        mesh = blocks.combine()

        for var in variables:

            ds.variables[var][i, :] = mesh[var]

        i = i + 1

    ds.close()

