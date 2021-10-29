import xarray as xr
import glob
import os
import pandas as pd
import datetime


def pts_dir_to_nc(path, output_nc, UTC_offset=0):
    """
    Converts an output points directory to a netcdf file

    :param path:
    :param output_nc:
    :param UTC_offset:
    :return:
    """
    files = glob.glob( os.path.join(path,'*_out.txt') )

    pts_to_nc(files, output_nc, UTC_offset)


def pts_dir_to_pd(path,  UTC_offset=0):
    """
    Converts an output points directory to a Pandas dataframe

    :param path:
    :param UTC_offset:
    :return:
    """
    files = glob.glob( os.path.join(path,'*_out.txt') )

    return pts_to_pd(files, UTC_offset)


def pts_to_pd(files, UTC_offset = 0):
    """
    Loads output point files into one large Pandas data frame

    :param files: List of input files
    :param output_nc:
    :param UTC_offset:
    :return:
    """
    # Time zone
    UTC_to_MST = 0  # UTC

    # Loop each station
    CHM_pts = []
    for cf in files:

        cSta = os.path.basename(cf).split('_')[0]
        print("processing " + cSta)

        # Importto pandas dataframe
        modData = pd.read_csv(cf, sep=",", parse_dates=True)
        modData.set_index('datetime', inplace=True)
        # Make datetime the index
        modData.index = pd.to_datetime(modData.index)
        modData.index = modData.index + datetime.timedelta(hours=UTC_offset)
        # Convert to data set and add station name
        c_ds = xr.Dataset(modData)
        c_ds['station'] = cSta
        c_ds = c_ds.rename({'datetime': 'time'})
        # Save in list
        CHM_pts.append(c_ds)

    # concat all stations
    ds = xr.concat(CHM_pts, dim='station')

    # Set -9999 (CHM missing value) to nan
    ds = ds.where(ds != -9999)

    if 'dm/dt' in ds.data_vars:
        ds = ds.drop(['dm/dt'])

    return ds


def pts_to_nc(files, output_nc, UTC_offset = 0):
    """
    Converts the point file to a netcdf file

    :param files: List of files
    :param output_nc: Output netcdf file
    :param UTC_offset: Conversion of UTC to desired timeframe
    :return:
    """

    ds = pts_to_pd(files,UTC_offset)

    # Save out to netcdf
    ds.to_netcdf(output_nc, engine='netcdf4')