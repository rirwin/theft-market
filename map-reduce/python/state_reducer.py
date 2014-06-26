#!/usr/bin/env python                                                                                             

from operator import itemgetter
import sys

# Key is (state_code, num_bed) 

state_dict = {}

for line in sys.stdin:

    # remove leading and trailing whitespace
    line = line.strip()
    
    # for Hive queries
    print line

    '''
    # for Hbase queries
    words = line.split('\t')
    
    try:
        state_code = words[0]
        num_bed = words[1] # used as str below
        week = words[2] 
        avg_price = int(words[3])
        num_list = int(words[4])

        key = state_code + '-' + num_bed

        # create key if not in state_dict
        if key not in state_dict:
            state_dict[key] = {}

        # assumes column family 'cf'
        state_dict[key]['cf:'+week] = {'n':num_list, 'a':avg_price}
    except:
        pass

    
import happybase
conn = happybase.Connection('172.31.11.76',9090)
#conn = happybase.Connection('localhost')
table = conn.table('state_stats')
for key in state_dict: # state bed key
    for wk_key in state_dict[key]:
        val = str(state_dict[key][wk_key])
        table.put(key,{wk_key:val})

'''
