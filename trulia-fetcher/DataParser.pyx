from xml.dom import minidom
import time

    # Static parsing methods
    # each are manual because we have a different schema
    # and each handles dirty data in different ways
    # TODO merge some common functionalities

def parse_get_state_stats_resp(text):
       
    head_dom = minidom.parseString(text)
    dom_list = head_dom.getElementsByTagName('listingStat')
    state_code = head_dom.getElementsByTagName('stateCode')[0].firstChild.nodeValue

    # No key, do not log
    if len(state_code) != 2:
        return

    # for batching to hbase
    dom_accum = {}

    # Base of HBase key, will append bedroom count
    hbase_base_key = state_code.lower()
        
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
                
                if (is_str_positive_int(k_bed)):
                    try:
                        state_dict = {}
                        state_dict['state_code'] = str(state_code)
                        state_dict['week_ending_date'] = str(week_ending_date)
                        state_dict['num_beds'] = int(k_bed)
                            
                        # carefully parsing of sub xml dom
                        listing_stat_dict = parse_listing_stat(k_bed_i)
                        
                        # merge keys
                        state_dict = dict(state_dict.items() + listing_stat_dict.items())

                        # hbase aggregation
                        val = str({'a': state_dict['avg_list'], 'n' : state_dict['num_list'] })
                        col_name = 'cf:' + week_ending_date
                        if k_bed not in dom_accum:
                            dom_accum[k_bed] = {}
                        dom_accum[k_bed][col_name] = val
                    except:
                        continue
    
    print "done parsing"


def is_str_positive_int(k_bed):
    try:
        k = int(k_bed)
        if k < 0:
           return False
    except:
        return False

    return True

def parse_listing_stat(list_stat_dom):

        stat_dict = {}

        try:
            num_list = list_stat_dom.getElementsByTagName('numberOfProperties')[0].firstChild.nodeValue
        except:
            pass

        try:
            avg_list = list_stat_dom.getElementsByTagName('averageListingPrice')[0].firstChild.nodeValue
        except:
            pass

        try:
            med_list = list_stat_dom.getElementsByTagName('medianListingPrice')[0].firstChild.nodeValue
        except:
            pass

        stat_dict['ts'] = int(time.time()*1000)                           

        if is_str_positive_int(num_list):
            stat_dict['num_list'] = int(num_list)

        try:
            stat_dict['avg_list'] = int(avg_list)
        except:
            pass

        try:
            stat_dict['med_list'] = int(med_list)
        except:
            pass

        return stat_dict
