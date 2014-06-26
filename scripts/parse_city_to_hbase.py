import sys
import os
import fnmatch
from xml.dom import minidom
import happybase
import pprint

sys.path.append('../trulia-fetcher')
import TruliaDataFetcher

data_dir = '/home/ubuntu/data'
loc_type = 'city'


def get_file_list(data_dir, loc_type):
    file_list = []
    for root, dir_names, file_names in os.walk(data_dir + '/' + loc_type):
        for file_name in fnmatch.filter(file_names, '*.xml'):
            file_list.append(os.path.join(root, file_name))
    return file_list


def parse_get_city_stats_resp(text): # TODO pass output function :-)

        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('listingStat')
        state_code = head_dom.getElementsByTagName('state')[0].firstChild.nodeValue
        city = head_dom.getElementsByTagName('city')[0].firstChild.nodeValue

        print city, state_code
        # No key, do not log                                                                                              
        if len(state_code) != 2 and len(city) > 0:
            return

        # Base of HBase key, will append bedroom count
        hbase_base_key = state_code.lower() + '-' + '_'.join(city.lower().split(' '))

        for dom_i in dom_list:

            week_ending_date = dom_i.getElementsByTagName('weekEndingDate')[0].firstChild.nodeValue
            week_list = dom_i.getElementsByTagName('listingPrice')

            for week_dom_i in week_list:
                k_bed_list = week_dom_i.getElementsByTagName('subcategory')
                for k_bed_i in k_bed_list:
                    prop_list = k_bed_i.getElementsByTagName('type')[0].firstChild.nodeValue

                    # checking k_bed to be either a positive int
                    # don't record aggregated 'All Properties' stats
                    k_bed = prop_list.split(' ')[0]


                    if (TruliaDataFetcher.TruliaDataFetcher.is_str_positive_int(k_bed)):
                        print k_bed
                        try:
                            num_list = k_bed_i.getElementsByTagName('numberOfProperties')[0].firstChild.nodeValue
                            avg_list = k_bed_i.getElementsByTagName('averageListingPrice')[0].firstChild.nodeValue
                            key = hbase_base_key + '-' + str(k_bed)
                            col_name = 'cf:' + week_ending_date
                            val = str({'a': avg_list, 'n' : num_list })
                            print val
                            table.put(key, {col_name : val} )
                        except:
                            continue



conn = happybase.Connection('localhost')
table = conn.table('city_stats')


file_list = get_file_list(data_dir, loc_type)

for file_name in file_list:
    print 'Parsing',file_name
    file_handle = open(file_name, 'r')
    text = file_handle.read()
    file_handle.close()
    
    parse_get_city_stats_resp(text)




#city_dict = TruliaDataFetcher.parse_get_city_stats_resp(text)


