# import sys

import CHM as pc

# df = pc.open_pvd('meshes/FSM.pvd')
# df=df.set_index('datetime')['2018-01-01':'2018-01-01'].reset_index()
# pc.vtu_to_ugrid(df, 'FSM.nc', variables=['swe'])
pc.ugrid2tiff('FSM.nc',
                # save_weights_file="weights.nc",
load_weights_file="weights.nc",
                  # dxdy=0.036,
                    dxdy=0.001,
                  method='bilinear',
              time_offsets=[0])

	# mesh_topology_nc='mesh.nc')


