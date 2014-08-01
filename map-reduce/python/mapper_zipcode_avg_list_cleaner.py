#!/usr/bin/env python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    words = line.split('\t')

    try:
        rec = eval(words[2])
        int(rec['num_beds']) # ignore cases of 'All' by thowing exception
        print '%s\t%s\t%s\t%s\t%s' % (rec['zipcode'], rec['num_beds'], rec['week_ending_date'], rec['avg_list'], rec['num_list'])
    except:
        continue

