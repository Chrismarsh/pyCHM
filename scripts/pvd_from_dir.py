import glob
import re

header = """
<?xml version="1.0" encoding="utf-8"?>
<VTKFile type="Collection" version="0.1">
    <Collection>
"""

footer = """
    </Collection>
</VTKFile>
"""

dataset = {}

for f in glob.glob('*.vtu'):
    # print(f)
    item =  """         <DataSet timestep="%s" group="" part="0" file="%s"/>\n"""
    m = re.findall(r'[0-9]+', f)
    item = item % (m[0], f)
    dataset[int(m[1])] = item
    # print(item)

s = sorted(dataset.keys())

with open('out.pvd','w') as file:
    file.write(header)
    for i in s:
        file.write(dataset[i])
    file.write(footer)

