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
    for root, dir_names, file_names in os.walk(data_dir + '/' + loc_type ):
        for file_name in fnmatch.filter(file_names, '*.xml'):
            file_list.append(os.path.join(root, file_name))
    return file_list


def parse_get_city_stats_resp(text): 

        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('listingStat')
        state_code = head_dom.getElementsByTagName('state')[0].firstChild.nodeValue
        city = head_dom.getElementsByTagName('city')[0].firstChild.nodeValue

        print city, state_code
        # No key, do not log                                                                                              
        if len(state_code) != 2 and len(city) > 0:
            return

        #res = dm.simple_select_query(dm.conn, "info_city", "latitude, longitude", "state_code = '" + state_code + "' and city = '" + city + "' LIMIT 1")
        #lat = res[0][0]
        #lon = res[0][1]

        # Base of HBase key, will append bedroom count
        hbase_base_key = state_code.lower() + '-' + '_'.join(city.lower().split(' '))

        # for batching to hbase
        dom_accum = {}

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
                        #print k_bed
                        try:
                            ## HBase
                            key = hbase_base_key + '-' + str(k_bed)

                            num_list = k_bed_i.getElementsByTagName('numberOfProperties')[0].firstChild.nodeValue
                            avg_list = k_bed_i.getElementsByTagName('averageListingPrice')[0].firstChild.nodeValue
                            col_name = 'cf:' + week_ending_date
                            val = str({'a': avg_list, 'n' : num_list })
                            
                            if k_bed not in dom_accum:
                                dom_accum[k_bed] = {}
                            dom_accum[k_bed][col_name] = val

                            #table.put(key, {col_name : val} )

                            # city meta-data
                            # table.put(key, {'i:city': city, 'i:sc':state_code, 'i:lat:':str(lat),'i:lon':str(lon)})
                            # End Hbase
                            
                            ## Fluentd
                            city_dict = {}
                            city_dict['state_code'] = str(state_code)
                            city_dict['city'] = str(city)
                            city_dict['week_ending_date'] = str(week_ending_date)
                            city_dict['num_beds'] = int(k_bed)
                            
                            # carefully parsing of sub xml dom
                            listing_stat_dict = TruliaDataFetcher.TruliaDataFetcher.parse_listing_stat(k_bed_i)

                            # merge keys
                            city_dict = dict(city_dict.items() + listing_stat_dict.items())
                            #event.Event('city.all_list_stats', city_dict)
                            ## End Fluentd

                        except:
                            continue
            
        for key in dom_accum:
            table.put(hbase_base_key + '-' + key, dom_accum[key])

def get_dom_dict(dom):
    pass

sys.path.append('../common/')
import DatabaseManager
dm = DatabaseManager.DatabaseManager('../conf/')
import time
conn = happybase.Connection('ip-172-31-11-76.us-west-1.compute.internal')

conn.delete_table('city_stats_26june14', disable=True)

conn.create_table('city_stats_26june14', {'cf': {} ,'i':{}})




table = conn.table('city_stats_26june14')
#table = conn.table('city_stats')

# TODO cleanup
fluent_path = "../extern/fluent-logger-python"
sys.path.append(fluent_path)
from fluent import sender
from fluent import event
sender.setup('hdfs')

file_list = get_file_list(data_dir, loc_type)

for file_name in file_list:
    print 'Parsing',file_name
    file_handle = open(file_name, 'r')
    text = file_handle.read()
    file_handle.close()
    parse_get_city_stats_resp(text)




#city_dict = TruliaDataFetcher.parse_get_city_stats_resp(text)


