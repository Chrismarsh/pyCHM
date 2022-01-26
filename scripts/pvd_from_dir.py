#!/usr/bin/env python

import glob
import re

header = """<?xml version="1.0" encoding="utf-8"?>
<VTKFile type="Collection" version="0.1">
    <Collection>
"""

footer = """
    </Collection>
</VTKFile>
"""

dataset = []

# We are making the assumption these are modern CHM files in the format <prefix><unix time>_<MPI rank>.vtu

for f in glob.glob('*.vtu'):
    m = re.findall(r'[0-9]+', f)
    ts = int(m[0])
    rank = int(m[1])

    item = """         <DataSet timestep="%s" group="" part="%s" file="%s"/>\n"""

    item = item % (ts, rank, f)

    dataset.append( (ts,rank, item) )

dataset.sort(key=lambda tup: (tup[0], tup[1]) )


with open('out.pvd','w') as file:
    file.write(header)
    for i in dataset:
        file.write( i[2] )
    file.write(footer)

