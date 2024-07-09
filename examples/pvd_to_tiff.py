import sys
sys.path.append("/home/cmarsh/Documents/science/code/pyCHM/")
# import dask
import CHM as pc

df = pc.open_pvd('/Users/cmarsh/Documents/science/model_runs/benchmark_problems/kan_pbsm_TC2020/BSCor3_Alb100_NoSubTopo_NoPomLi_K0p3_NoCplz0_Recirc20_WN1000_24/output_FSM_rhod600/SC.pvd')
df=df.set_index('datetime')['2018-01-01':'2018-01-01'].reset_index()
pc.vtu_to_ugrid(df, 'test.nc', variables=['swe'])
pc.ugrid2tiff('test.nc', mesh_topology_nc='mesh.nc')




# import dask.distributed
# from dask.distributed import Client, LocalCluster
# from dask.distributed import performance_report
#
#
# if __name__ == '__main__':
#
#     cluster = LocalCluster(n_workers=1, processes=True, threads_per_worker=1)
#     client = Client(cluster)
#     #dask.config.set(**{'array.slicing.split_large_chunks': True})
#     # client.get_versions(check=True)
#     # print(dask.config.config)
#     # dask.config.set(scheduler='processes')
#     # print(dask.config.config)
#     # with performance_report(filename="dask-report.html"):
#
#     # df=pc.pvd_to_xarray('/Users/chris/Documents/science/model_runs/benchmark_problems/granger_pbsm_synthetic/output/meshes/granger.pvd',
#     #                         dxdy=50,variables=['swe',"t","rh","snowdepthavg"])
#     df=pc.pvd_to_xarray('/home/cmarsh/science/model_runs/benchmark_problems/kan_pbsm_TC2020/BSCor3_Alb100_NoSubTopo_NoPomLi_K0p3_NoCplz0_Recirc20_WN1000_24/output_FSM_rhod600/SC.pvd',
#                         dxdy=50, variables=['swe'], regridding_method='CONSERVE')
#     #
#     #
#     # print('done pvd load')
#     df = df.isel(time=[0,1,2,3,4])
#     df.chm.to_raster(crs_out='EPSG:4326')
#     #
#     pc.pvd_to_tiff(
#         '/home/cmarsh/science/model_runs/benchmark_problems/kan_pbsm_TC2020/BSCor3_Alb100_NoSubTopo_NoPomLi_K0p3_NoCplz0_Recirc20_WN1000_24/output_FSM_rhod600/SC.pvd',
#         dxdy=150, variables=['swe'])
#
#     #print(df)
#     print('done')