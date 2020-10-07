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

files = glob.glob(f'{searchpath}/*.vrt')

if len(files) == 0:
    print('No vrt files found')
    exit(1)

# Find the vrt files
dirs = set()
suffix = None
basename = None
for f in files:
    m = re.findall(r'[0-9]+', f)
    m = m[-1]  #ensure we get the last number incase there are numbers in the filename
    dirs.add(int(m))
    
   
    s = re.findall(r'spd_up_(.+)\.vrt', f)
    
    if suffix is None:
        if len(s) > 0:
            suffix = s[-1]
        
    #figure out our base path
    if basename is None:
        m = re.findall(r'/(.+)_[0-9]+_[UV].vrt', f)
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

        if(i==0):
            i=ncat  

        for var in ['U','V','spd_up']:

            v = var
            if 'spd_up' in var:
                v = f'{var}_{suffix}'

            fname = f'{basename}_{ww}_{v}.vrt'

            p = os.path.abspath(searchpath + '/' + fname)

            NinjaName = f'Ninja{i}'

            if 'spd_up' not in var:
                NinjaName = f'{NinjaName}_{var}'

            line = "'%s' : {'file':'%s','method':'mean'}, \n" % (NinjaName,p)
            file.write(line)
            print(line)