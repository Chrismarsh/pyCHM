.. pyCHM documentation master file, created by
   sphinx-quickstart on Wed Oct  7 16:42:13 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pyCHM's documentation!
=================================

This Python package provides python tooling for working with the `Canadian Hydrological Model (CHM) <https://chm.readthedocs.io/en/dev/>`_.

It is built on top of Uxarray, xarray, and ESMF. It principally covers regridding the unstructured mesh to structured,
such as zarr, netcdf, and GeoTiff.

These are provided via an xarray accessor (``UxDataset.chm.*``) and CLI tools.

.. toctree::
   :maxdepth: 2
   :caption: Contents:
   :hidden:

   installation
   xarray_accessor
   tools
   examples

