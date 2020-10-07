from setuptools import setup

setup(
    name='pyCHM',
    version='0.0.1',
    description='python tools for CHM',
    license='MIT',
    packages=['pyCHM'],
    author='Chris Marsh',
    author_email='chris.marsh@usask.ca',
    install_requires=['vtk','numpy','xarray','netCDF4','pandas'],
    scripts=["scripts/vtu2geo.py","scripts/windmapper2mesher.py"],
)