import pyCHM as pc

df=pc.pvd_to_xarray('/Users/chris/Documents/science/code/SnowCast/v2/2_chm/output/meshes/SC.pvd',dxdy=300)
s = df.isel(time=-1)

all_var = set([x for x in s.data_vars.keys()])
keep_var = set( ['swe'] )

# figure out what to drop
drop = all_var - keep_var
s = s.drop_vars(drop)

s['swe'].rio.to_raster('swe.tif')