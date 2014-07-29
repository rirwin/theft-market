from xml.dom import minidom
import time
import datetime

def parse_get_state_stats_resp(text):
       
    head_dom = minidom.parseString(text)
    dom_list = head_dom.getElementsByTagName('listingStat')
    state_code = head_dom.getElementsByTagName('stateCode')[0].firstChild.nodeValue

    # No key, do not log
    if len(state_code) != 2:
        return
    else:
        stats_doc, most_recent_week = parse_week_listings(dom_list)
        head_doc = {}
        head_doc['doc_type'] = 'state_record'
        head_doc['state_code'] = state_code
        head_doc['stats'] = stats_doc
        head_doc['most_recent_week'] = most_recent_week
        return head_doc


def parse_get_city_stats_resp(text):
       
    head_dom = minidom.parseString(text)
    dom_list = head_dom.getElementsByTagName('listingStat')
    state_code = head_dom.getElementsByTagName('state')[0].firstChild.nodeValue
    city = head_dom.getElementsByTagName('city')[0].firstChild.nodeValue

    # No key, do not log
    if len(state_code) != 2:
        return
    else:
        stats_doc, most_recent_week = parse_week_listings(dom_list)
        head_doc = {}
        head_doc['doc_type'] = 'city_record'
        head_doc['state_code'] = state_code
        head_doc['city'] = city
        head_doc['stats'] = stats_doc
        head_doc['most_recent_week'] = most_recent_week
        return head_doc


def parse_get_county_stats_resp(text):
       
    head_dom = minidom.parseString(text)
    dom_list = head_dom.getElementsByTagName('listingStat')
    state_code = head_dom.getElementsByTagName('state')[0].firstChild.nodeValue
    county = head_dom.getElementsByTagName('county')[0].firstChild.nodeValue

    # No key, do not log
    if len(state_code) != 2 and len(county) > 0:
        return
    else:
        stats_doc, most_recent_week = parse_week_listings(dom_list)
        head_doc = {}
        head_doc['doc_type'] = 'county_record'
        head_doc['state_code'] = state_code
        head_doc['county'] = county
        head_doc['stats'] = stats_doc
        head_doc['most_recent_week'] = most_recent_week
        return head_doc


def parse_get_zipcode_stats_resp(text):
            
    head_dom = minidom.parseString(text)
    dom_list = head_dom.getElementsByTagName('listingStat')

    # No key, do not log
    try:
        zipcode = head_dom.getElementsByTagName('zipCode')[0].firstChild.nodeValue
        int(zipcode)
    except:
        return

    if len(zipcode) != 5:
        return
    else:
        stats_doc, most_recent_week = parse_week_listings(dom_list)
        head_doc = {}
        head_doc['doc_type'] = 'zipcode_record'
        head_doc['zipcode'] = zipcode
        head_doc['stats'] = stats_doc
        head_doc['most_recent_week'] = most_recent_week
        return head_doc


def parse_week_listings(dom_list):

    # semi-structured json document
    json_doc = {}
    most_recent_week = datetime.datetime.strptime('2000-01-01', '%Y-%m-%d')
    for dom_i in dom_list:

        week_ending_date_str = dom_i.getElementsByTagName('weekEndingDate')[0].firstChild.nodeValue
        week_ending_datetime = datetime.datetime.strptime(week_ending_date_str, '%Y-%m-%d') 

        if week_ending_datetime > most_recent_week:
            most_recent_week = week_ending_datetime

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
                        sub_doc = {}
                        sub_doc['week_ending_date'] = str(week_ending_date_str)
                        sub_doc['num_beds'] = int(k_bed)

                        # parsing of sub xml dom
                        listing_stat_dict = parse_listing_stat(k_bed_i)

                        # merge keys
                        sub_doc = dict(listing_stat_dict.items() + listing_stat_dict.items())

                        # record aggregation
                        val = {'a': sub_doc['avg_list'], 'n' : sub_doc['num_list'] }

                        if k_bed not in json_doc:
                            json_doc[k_bed] = {}
                        json_doc[k_bed][week_ending_date_str] = val
                    except:
                        continue

    return json_doc, most_recent_week.strftime('%Y-%m-%d')


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
