from setuptools import setup
import subprocess
from setuptools import find_packages
import glob

def get_installed_gdal_version():
    try:
        version = subprocess.run(["gdal-config","--version"], stdout=subprocess.PIPE).stdout.decode()

        version = version.replace('\n', '')
        #pygdal don'ts always have the most up todate version so we will need to swtich to pygdal-chm if it is not available
        # 3.3.2 is most recent as of this version
        mmp = [int(x) for x in version.split('.')]
        chm = ''
        if mmp[0] > 3 or mmp[1] > 3 or mmp[2] > 2:
            chm = '-chm'

        version = chm + "=="+version+".*"
        return version
    except FileNotFoundError as e:
        raise(""" ERROR: Could not find the system install of GDAL. 
                  Please install it via your package manage of choice.
                """
            )

setup(
    name='CHM',
    version='1.0.15',
    description='python tools for CHM',
    license='MIT',
    packages=find_packages(),
    author='Chris Marsh',
    author_email='chris.marsh@usask.ca',
    install_requires=['vtk','numpy','xarray>=0.18.1','netCDF4','pandas',
                      'pygdal'+get_installed_gdal_version(), 'dask[complete]',
                      'pyvista>0.29', 'pyESMF~=8.2.1b3','rioxarray','rasterio', 'ninja'],
    scripts=glob.glob("scripts/*.py"),
)