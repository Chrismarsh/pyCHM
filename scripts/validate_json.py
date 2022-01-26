#!/usr/bin/env python

import json
import sys

print('This checks if a json file has correct syntax. It does not check nested json files.')
print('If this too passes, it does not guarantee the items within the json make sense to CHM!')

if(len(sys.argv) == 1):
    print("Requires .json config file")
with open(sys.argv[1]) as f:
    try:
        json.load(f)
    except ValueError as error:
        print("invalid json: %s" % error)

print("Ok!")