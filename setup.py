from setuptools import setup
import subprocess
from setuptools import find_packages

def get_installed_gdal_version():
    try:
        version = subprocess.run(["gdal-config","--version"], stdout=subprocess.PIPE).stdout.decode()

        version = version.replace('\n', '')
        version = "=="+version+".*"
        return version
    except FileNotFoundError as e:
        raise(""" ERROR: Could not find the system install of GDAL. 
                  Please install it via your package manage of choice.
                """
            )

setup(
    name='CHM',
    version='1.0.4',
    description='python tools for CHM',
    license='MIT',
    packages=find_packages(),
    author='Chris Marsh',
    author_email='chris.marsh@usask.ca',
    install_requires=['vtk','numpy','xarray','netCDF4','pandas',
                      'pygdal'+get_installed_gdal_version(), 'dask[complete]',
                      'pyvista', 'pyESMF~=8.1.0b36','rioxarray','pandas','rasterio'],
    scripts=["scripts/vtu2geo.py","scripts/windmapper2mesher.py"],
)