# import dask
#
# try:
#     data = dask.config.get('scheduler')
# except KeyError as e:
#     # print("""Dask needs to use processes instead of threads because of the esmf backend.\nSet:\n\tdask.config.set(scheduler='processes')""")
#     # print('pyCHM as done this for you')
#     dask.config.set(scheduler='processes')

from CHM.vtu import *
import CHM.conversion as conversion


