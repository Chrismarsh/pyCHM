vtu
=====

Allows for the conversion of the pvd files into a rasterized xarray DataSet. All of the variable fields in the xarray dataset are Dask delayed
and so are not actually computed until either ``compute`` or ``persist`` are called, or some other function that requires the data is called, e.g., ``plot``.
The conversion to raster is via the conservative regridding method of ESMF.

The ``chm`` extension on the xarray object can be used to save these datasets to georeferenced rasters.
An example workflow is below:

::

    import CHM as pc
    import dask

    if __name__ == '__main__':

        dask.config.set(scheduler='processes')

        df = pc.pvd_to_xarray('output/SC.pvd',dxdy=500)
        df = df.sel(time=slice('2020-12-01', '2021-05-01'))
        df.chm.to_raster(crs_out='EPSG:26911')

Attribute information such as the crs can end up dropped when doing various operations -- see
`here <https://corteva.github.io/rioxarray/stable/getting_started/manage_information_loss.html>`_  for a more detailed
description of the problem.  To preserve these attributes consider using ``set_options``.

::

    with xarray.set_options(keep_attrs=True):
        ds = ds1 + ds2

.. automodule:: CHM.vtu
    :members: