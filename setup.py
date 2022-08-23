from setuptools import setup
import subprocess
from setuptools import find_packages
import glob
import packaging.version

def get_installed_gdal_version():
    try:
        version = subprocess.run(["gdal-config", "--version"], stdout=subprocess.PIPE).stdout.decode()

        version = version.replace('\n', '')
        # pygdal doesn't always have the most up todate version so we will need to swtich to pygdal-chm if it is not available
        chm = ''
        if packaging.version.parse(version) > packaging.version.parse("3.5.1"):
            chm = '-chm'

        version = chm + "==" + version + ".*"
        return version

    except FileNotFoundError as e:
        raise(""" ERROR: Could not find the system install of GDAL. 
                  Please install it via your package manage of choice.
                """
            )

setup(
    name='CHM',
    version='1.2.5',
    description='python tools for CHM',
    license='MIT',
    packages=find_packages(),
    author='Chris Marsh',
    author_email='chris.marsh@usask.ca',
    install_requires=['vtk', 'numpy', 'xarray>=0.18.1', 'netCDF4', 'pandas',
                      'pygdal'+get_installed_gdal_version(), 'dask[complete]',
                      'pyvista>0.29', 'rioxarray', 'rasterio', 'ninja', 'netCDF4', 'cftime', 'pyproj'],
    extra_requires={'complete': ['pyESMF>=8.3.0']},
    scripts=glob.glob("scripts/*.py"),
)