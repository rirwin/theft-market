#!/usr/bin/env python                                                                                             

from operator import itemgetter
import sys

# Key is (state_code, num_bed) 

state_dict = {}

for line in sys.stdin:

    # remove leading and trailing whitespace
    line = line.strip()
    
    # for Hive queries
    #print line 

    # for Hbase queries
    words = line.split('\t')
    
    try:
        state_code = words[0]
        num_bed = words[1] # used as str below
        week = words[2] 
        avg_price = int(words[3])
        num_list = int(words[4])
    except:
        print line


    key = state_code + '-' + num_bed
    if key not in state_dict:
        state_dict[key] = {}
    state_dict[key][week] = {'n':num_list, 'a':avg_price}


print state_dict['AZ-3']
