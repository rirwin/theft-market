#!/usr/bin/env python                                                                                             
from operator import itemgetter
import sys

state_dict = {}

for line in sys.stdin:

    # remove leading and trailing whitespace
    line = line.strip()
    
    # for Hive queries
    print line 
