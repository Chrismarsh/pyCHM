# import CHM as pc
import CHM as pc

# df=pc.pvd_to_xarray('/Users/chris/Documents/science/model_runs/benchmark_problems/granger_pbsm_synthetic/output/meshes/granger.pvd',dxdy=10)
df=pc.pvd_to_xarray('/Users/chris/Documents/science/model_runs/UBC_Peter_White/output/SC.pvd',dxdy=540)


s = df.isel(time=-1)

all_var = set([x for x in s.data_vars.keys()])
keep_var = set( ['swe'] )

# figure out what to drop
drop = all_var - keep_var
s = s.drop_vars(drop)

s['swe'].rio.to_raster('swe.tif')