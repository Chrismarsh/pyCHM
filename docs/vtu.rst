vtu
=====

Allows for the conversion of the pvd files to georeferenced rasters. Uses the conservative regridding method of ESMF.

::

    import CHM as pc
    import dask

    if __name__ == '__main__':

        dask.config.set(scheduler='processes')

        df = pc.pvd_to_xarray('output/SC.pvd',dxdy=500)

        df.chm.to_raster(crs_out='EPSG:26911')

Attribute information such as the crs can end up dropped when doing various operations -- see
`here <https://corteva.github.io/rioxarray/stable/getting_started/manage_information_loss.html>`_  for a more detailed
description of the problem.  To preserve these attributes consider using ``set_options``.

::

    with xarray.set_options(keep_attrs=True):
        ds = ds1 + ds2

.. automodule:: CHM.vtu
    :members: