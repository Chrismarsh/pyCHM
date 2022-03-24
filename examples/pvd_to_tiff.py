import sys
sys.path.append("/Users/chris/Documents/science/code/pyCHM/")
import dask
import CHM as pc

dask.config.set(scheduler='processes')
if __name__ == '__main__':
    # df=pc.pvd_to_xarray('/Users/chris/Documents/science/model_runs/benchmark_problems/granger_pbsm_synthetic/output/meshes/granger.pvd',dxdy=10)
    df=pc.pvd_to_xarray('/Users/chris/Documents/science/model_runs/benchmark_problems/kan_pbsm_TC2020/BSCor3_Alb100_NoSubTopo_NoPomLi_K0p3_NoCplz0_Recirc20_WN1000_24/reference'
                        '/SC.pvd', dxdy=2500, variables=['swe'])


    print('done pvd load')
    s = df.isel(time=-1)
    s.chm.to_raster(crs_out='EPSG:4326')
    print('done')