Tools
=====

This project includes a small collection of helper scripts under ``src/CHM/tools``.
They cover data conversion, validation, and preprocessing tasks commonly needed
when working with CHM outputs or inputs.

chm_ugrid2grid
----------------

Convert CHM UGRID NetCDF files to structured GeoTIFF or Zarr outputs using ESMF
regridding.

Example::

   chm_ugrid2grid input.nc --dxdy 0.01 --method conservative \
       --tiff-output ./tiffs --zarr-output ./zarr --overwrite

Key options:

- ``--mesh``: load mesh topology from a separate NetCDF file.
- ``--variables``: limit conversion to the listed variables.
- ``--timeoffset``: pick specific time indices to process.
- ``--zarr-chunk-y`` / ``--zarr-chunk-x``: tune Zarr chunk sizes.
- ``--overwrite``: allow replacing existing outputs.



chm_pvd_from_dir
------------------

Create an ``out.pvd`` ParaView Data file by scanning ``*.vtu`` files in the
current directory. Assumes filenames follow ``<prefix><timestamp>_<rank>.vtu``.

Usage::

   chm_pvd_from_dir

chm_validateJSON
--------------------

Simple syntax check for CHM JSON configuration files. Reports whether the input
parses without validating content semantics.

Usage::

   chm_validateJSON config.json

chm_windmapper2mesher
-------------------------

Generate a ``config_WN.txt`` mapping for CHM's mesher from a directory of
WindNinja/Windmapper GeoTIFFs.

Usage::

   chm_windmapper2mesher /path/to/windmapper/tiles

The script inspects directional tiles (``*_U.tif``, ``*_V.tif``, ``*spd_up*.tif``),
derives available wind directions, and writes Ninja variable entries for each.
