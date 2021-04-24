#!/usr/bin/env python

import numpy as np
import os 
import glob
import re
import sys

if len(sys.argv) == 1:
    print('Requires input path')
    exit(1)

searchpath = sys.argv[1]

files = glob.glob(f'{searchpath}/*.tif')

if len(files) == 0:
    print('No tif files found')
    exit(1)

# Find the vrt files
dirs = set()
suffix = None
basename = None
for f in files:
    m = re.findall(r'[0-9]+', f)

    try:
        # ensure we get the last number incase there are numbers in the filename
        m = int(m[-1])
    except IndexError as e:
        # the ref-DEM.tif file probably, keep going
        continue

    # the spd_up will have a pattern like
    # ref-DEM-utm_0_spd_up_1000.vrt
    # where the last number is actually not what we want.
    # as we enumerate all the files, we will eventually find its pair, so we just skip adding 1000 to the dir set
    if 360 >= m >= 0:
        dirs.add(m)

    s = re.findall(r'spd_up_(.+)\.tif', f)
    
    if suffix is None:
        if len(s) > 0:
            suffix = s[-1]
        
    #figure out our base path
    if basename is None:
        m = re.findall(r'/(.+)_[0-9]+_[UV]\.tif', f)
        if len(m) > 0:
            basename = m[-1]
        
dirs = list(dirs)
dirs = np.sort(dirs)

dtheta = dirs[1]-dirs[0]

ncat = len(dirs) # number of wind maps

print(f'Found {ncat} directions with step size = {dtheta} degrees')
print(f'Using basename = {basename}')

with open('config_WN.txt','w') as file:
    for i, ww in enumerate(dirs):

        if i == 0:
            i = ncat

        for var in ['U', 'V', 'spd_up']:

            v = var
            if 'spd_up' in var:
                v = f'{var}_{suffix}'

            fname = f'{basename}_{ww}_{v}.tif'

            p = os.path.abspath(searchpath + '/' + fname)

            NinjaName = f'Ninja{i}'

            if 'spd_up' not in var:
                NinjaName = f'{NinjaName}_{var}'

            line = "'%s' : {'file':'%s','method':'mean'}, \n" % (NinjaName, p)
            file.write(line)
            print(line)
