#!/usr/bin/env python                                                                                             

from operator import itemgetter
import sys

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    print line
    '''kv  = line.split('\t')

    print kv[0] + "\t" + kv[1] 
    '''
