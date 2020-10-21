from setuptools import setup
import subprocess

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
    name='pyCHM',
    version='0.0.1',
    description='python tools for CHM',
    license='MIT',
    packages=['pyCHM'],
    author='Chris Marsh',
    author_email='chris.marsh@usask.ca',
    install_requires=['vtk','numpy','xarray','netCDF4','pandas', 'pygdal'+get_installed_gdal_version()],
    scripts=["scripts/vtu2geo.py","scripts/windmapper2mesher.py"],
)