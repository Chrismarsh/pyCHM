xarray accessor
=================

The ``chm`` extension on the xarray object can be used access various commonly used functionality after loading a
ugrid file via Uxarray.


Examples
---------------------

Provides a ``Dataset.chm`` accessor for uxarray-backed VTU/UGRID datasets.
Common helpers:

- ``df.chm.uxgrid_to_netcdf("mesh.nc")``: export mesh scaffolding and global ids.
- ``df.chm.vars_to_netcdf("all_vars.nc")``: dump all variables to NetCDF.
- ``df.chm.clip(lat=[ymin, ymax], lon=[xmin, xmax])``: subset to a bounding box
  or supply a shapefile/geojson via ``shp_file_path``.
- ``df.chm.regrid(dxdy=0.01)``: bilinear regrid face-centered variables to a
  structured lat/lon grid (single-node meshes only).

Run accessor methods from Python after loading data with xarray/uxarray; this
module is not a standalone CLI.

Accessor
----------

.. autosummary::
    :toctree: generated/
    :template: autosummary/accessor_method.rst

    CHM.xarray_accessor.GeoAccessor.uxgrid_to_netcdf
    CHM.xarray_accessor.GeoAccessor.vars_to_netcdf
    CHM.xarray_accessor.GeoAccessor.clip
    CHM.xarray_accessor.GeoAccessor.regrid

