import urllib2 
import time
import sys
import os
import Queue
import datetime
import glob
import getopt
from xml.dom import minidom
from fluent import sender
from fluent import event
from pprint import pprint as pprint

common_path = "../common/"
sys.path.append(common_path)
import TruliaConfLoader
import DatabaseManager
import wrappers
import DataParser


def usage():

    print "python TruliaDataFetcher.py -l <geo location to load>"
    print "python TruliaDataFetcher.py -f <geo location to fetch>"
    print "geo locations are c,s,z,o for city, state, zipcode, county respectively"
    print "For example:"
    print "python TruliaDataFetcher.py -l cszo -f z"
    print "will read cities, states, zipcodes, and counties"
    print "and fetch even more recent data for zipcodes"
    print ""
    print "To actually load things parsed into a kv-store, you must pass the -k option"
    print "with the specified type of store (Redis or HBase) "
    print "-k r for Redis"
    print "-k h for Hbase"
    print ""
    print "To send parsed output to fluentd, you must pass the -d option"
    print "with the type of filesystem sent to (this host filesystem or hdfs)"
    print "-d f for this filesystem"
    print "-d h for the hdfs filesystem"
    print ""
    print "-d is equivalent to --fluentd"
    print "-k is equivalent to --kv-store"
    print "-f is equivalent to --fetch"
    print "-l is equivalent to --load"
    sys.exit(1)


def parse_args(argv):

    fetch_geo_types = ""
    load_geo_types = ""
    kv_store = ""
    fluentd_rx = ""
    try:
        opts, args = getopt.getopt(argv,"d:k:f:l:h",["fluentd=","kv-store=","fetch=","load=","help"])
    except getopt.GetoptError:
        usage()

    for opt, arg in opts:

        if opt in ("-h","--help"):
            usage()
        elif opt in ("-d","--fluentd"):
            fluentd_rx = arg.lower()
        elif opt in ("-k","--kv-store"):
            kv_store = arg.lower()
        elif opt in ("-f","--fetch"):
            fetch_geo_types = arg
        elif opt in ("-l","--load"):
            load_geo_types = arg
        else:
            print "bad argument"
            usage()

    return [fetch_geo_types, load_geo_types, kv_store, fluentd_rx]

        
class TruliaDataFetcher:
    def __init__(self, config_path, cla_dict):
        trulia_conf = TruliaConfLoader.TruliaConfLoader(config_path)
        self.load_trulia_params(trulia_conf)
        self.db_mgr = DatabaseManager.DatabaseManager(config_path)

        kv_store = cla_dict['kv_store']
        if kv_store == '':
            self.kv_mgr = None
        elif kv_store == 'h':
            print "loading HBase manager",
            import HBaseManager
            self.kv_mgr = HBaseManager.HBaseManager()
            print "completed"
        elif kv_store == 'r':
            print "loading Redis manager",
            import RedisManager
            self.kv_mgr = RedisManager.RedisManager()
            print "completed"


        fluentd_rx = cla_dict['fluentd_rx']
        if fluentd_rx == '':
            self.fluentd_enabled = False
            print "FluentD not enabled"
        elif fluentd_rx == 'f':
            print "loading fluentd for local fs"
            sender.setup('fs') 
            self.fluentd_enabled = True
            print "FluentD enabled for local filesystem"
        elif fluentd_rx == 'h':
            print "loading fluentd for hdfs"
            sender.setup('hdfs')
            self.fluentd_enabled = True
            print "FluentD enabled for HDFS"


    def load_trulia_params(self, trulia_conf):
        self.stats_library = trulia_conf.stats_library
        self.location_library = trulia_conf.location_library
        self.stats_functions_params = trulia_conf.stats_functions_params
        self.stats_functions = trulia_conf.stats_functions
        self.location_functions_params = trulia_conf.location_functions_params
        self.location_functions = trulia_conf.location_functions
        self.url = trulia_conf.url
        self.apikeys = trulia_conf.apikeys
        self.curr_key_idx = 0
        self.data_dir = trulia_conf.data_dir


    def load_all_states_from_xml_archive(self):
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            print "loading state from archive:",state_code
            self.read_already_fetched_files("state",{"state_code":state_code})


    def load_all_zipcodes_from_xml_archive(self):
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_zipcode", "zipcode")
        for zipcode_tuple in list(res):
            zipcode = zipcode_tuple[0]
            zipcode = str(100000 + zipcode)[1:]
            print "loading zipcode from archive:",zipcode 
            self.read_already_fetched_files("zipcode",{"zipcode":zipcode})


    def load_all_cities_from_xml_archive(self):
        state_tuples = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(state_tuples):
            state_code = state_code_tuple[0]
            city_tuples = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_city", "city", "state_code = '" + state_code + "'")
            for city_tuple in list(city_tuples):
                city = city_tuple[0]
                print "loading city from archive:", city, ",", state_code 
                self.read_already_fetched_files("city",{"state_code":state_code, "city":city})


    def load_all_counties_from_xml_archive(self):
        state_tuples = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(state_tuples):
            state_code = state_code_tuple[0]
            county_tuples = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_county", "county", "state_code = '" + state_code + "'")
            for county_tuple in list(county_tuples):
                county = county_tuple[0]
                print "loading county from archive:", county, ",", state_code 
                self.read_already_fetched_files("county",{"state_code":state_code, "county":county})


    def fetch_all_states_data(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")

        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            geo_type = "state"
            geo_dict = {"state_code":state_code}
            
            if self.fetch(geo_type, geo_dict, DataParser.parse_get_state_stats_resp):
                time.sleep(2.0/len(self.apikeys)) # trulia api restriction     


    def fetch_all_cities_all_states_data(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            
            state_code = state_code_tuple[0]
            self.fetch_all_cities_in_state_data(state_code)


    def fetch_all_cities_in_state_data(self, state_code):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_city", "city", "state_code = '" + state_code + "'")
        for city_tuple in list(res):

            city = city_tuple[0]
            geo_type = "city"
            geo_dict = {"city":city, "state_code":state_code}

            if self.fetch(geo_type, geo_dict, DataParser.parse_get_city_stats_resp):
                time.sleep(2.0/len(self.apikeys)) # trulia api restriction 


    def fetch_all_counties_all_states_data(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            print "Fetching counties in ", state_code
            self.fetch_all_counties_in_state_data(state_code)


    def fetch_all_counties_in_state_data(self, state_code):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_county", "county", "state_code = '" + state_code + "'")
        for county_tuple in list(res):

            county = county_tuple[0]
            geo_type = "county"
            geo_dict = {"county": county, "state_code": state_code}
    
            if self.fetch(geo_type, geo_dict, DataParser.parse_get_county_stats_resp):
                time.sleep(2.0/len(self.apikeys)) # trulia api restriction 


    def fetch_all_zipcodes_data(self): 

        # distinct needed since some zipcodes span multiple states (as reported by Trulia)
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_zipcode", "distinct zipcode")
        for zipcode_tuple in list(res):

            zipcode = str(100000 + zipcode_tuple[0])[1:] 
            geo_type = "zipcode"
            geo_dict = {"zipcode":zipcode}

            if self.fetch(geo_type, geo_dict, DataParser.parse_get_zipcode_stats_resp):
                time.sleep(2.0/len(self.apikeys)) # trulia api restriction 


    def fetch(self, geo_type, geo_dict, parser_func):

        print "fetching", geo_type, geo_dict
        if self.is_most_recent_api_call_today(geo_type, geo_dict):
            return False  # Notify caller that we did not hit Trulia's API 

        latest_rx_date, now_date = self.get_api_call_date_range(geo_type, geo_dict)
        print latest_rx_date, now_date
        url_str = self.form_url(geo_type, geo_dict, latest_rx_date, now_date)
        text = self.fetch_executor(url_str)
        if text is None:
            return False

        self.save_xml_file(text, geo_type, geo_dict)
        json_doc = self.parse_executor(parser_func, text)
        self.write_executor(json_doc, geo_type, geo_dict)
        return True


    def get_latest_list_stat_date(self, obj_type, obj_key_dict):
        table_str = "data_" + obj_type
        where_str = ""
        for k in obj_key_dict:
            where_str += k + " = '" + str(obj_key_dict[k]) + "' and "
        where_str = where_str[:-4]
        print where_str
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, table_str, "most_recent_week", where_str)
        if len(res) == 0:
            latest_ls_date = "2000-01-01"
        else:
            latest_ls_date = str(res[0][0])
        return latest_ls_date


    def get_latest_api_call_date(self, obj_type, obj_key_dict):
        table_str = "data_" + obj_type
        where_str = ""
        for k in obj_key_dict:
            where_str += k + " = '" + str(obj_key_dict[k]) + "' and "
        where_str = where_str[:-4]
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, table_str, "date_fetched", where_str)
        if res is None or len(res) == 0:
            latest_ac_date = "2000-01-01"
        else:
            latest_ac_date = datetime.datetime.strftime(res[0][0], '%Y-%m-%d')

        return latest_ac_date


    def form_url(self, geo_type, geo_dict, latest_rx_date, now_date):

        url_str = self.url + "library=" + self.stats_library + "&function="

        if geo_type == 'state':
            url_str += "getStateStats&state=" + geo_dict['state_code']

        if geo_type == 'zipcode':
            url_str += "getZipCodeStats&zipCode=" + geo_dict['zipcode']

        elif geo_type == 'county':
            county_spaced = '%20'.join(geo_dict['county'].split(' '))
            url_str += "getCountyStats&county=" + county_spaced + "&state=" + geo_dict['state_code'] 
        elif geo_type == 'city':
            city_spaced = '%20'.join(geo_dict['city'].split(' '))
            url_str += "getCityStats&city=" + city_spaced + "&state=" + geo_dict['state_code']

        url_str += "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikeys[self.curr_key_idx]
        self.curr_key_idx = (self.curr_key_idx + 1) % len(self.apikeys)

        return url_str
        

    def get_api_call_date_range(self, geo_type, geo_dict):

        latest_rx_date = self.get_latest_list_stat_date(geo_type, geo_dict)
        now_date = TruliaDataFetcher.get_current_date()
        print "Calling API from", latest_rx_date, "to", now_date
        return latest_rx_date, now_date


    def is_most_recent_api_call_today(self, geo_type, geo_dict):

        last_api_call_date = self.get_latest_api_call_date(geo_type, geo_dict)
        now_date = TruliaDataFetcher.get_current_date()
        if now_date == last_api_call_date:
            print " already fetched data today"
            return True
        else:
            return False


    def write_executor(self, json_doc, geo_type, geo_dict):

        if json_doc is None:
            return

        metadata_table = "data_" + geo_type
        if geo_type == "city":
            metadata_key_list = ["state_code = '" + geo_dict['state_code'] + "'", "city = '" + geo_dict['city'] + "'"]
        elif geo_type == "county":
            metadata_key_list = ["state_code = '" + geo_dict['state_code'] + "'", "county = '" + geo_dict['county'] + "'"] 
        elif geo_type == "state":
            metadata_key_list = ["state_code = '" + geo_dict['state_code'] + "'"]
        elif geo_type == "zipcode":
            metadata_key_list = ["zipcode = '" + geo_dict['zipcode'] + "'"]

        # send to FluentD
        if self.fluentd_enabled:
            self.send_json_doc_records_to_fluentd(geo_type + '.all_listing_stats', json_doc)        

        # send to kv store
        if self.kv_mgr is not None:
            self.kv_mgr.insert_json_doc_records(json_doc)
            
        # update meta-store
        most_recent_week = json_doc['most_recent_week']
        date_fetched = json_doc['date_fetched']
        
        self.db_mgr.establish_timestamp_and_most_recent_week(self.db_mgr.conn, metadata_table, most_recent_week, date_fetched, metadata_key_list)


    # short function, may expand to threads or generator design pattern
    def parse_executor(self, parse_func, text):
        json_doc = parse_func(text)
        if json_doc is not None:
            json_doc['date_fetched'] = datetime.datetime.now().strftime('%Y-%m-%d')
            return json_doc
        else:
            return None
        

    def fetch_executor(self, url_str):
        
        try:
            resp = urllib2.urlopen(url_str) 
            if resp.code == 200:
                return resp.read()
            else: # simple retry
                time.sleep(5)
                resp = urllib2.urlopen(url_str)
                if resp.code == 200:
                    return resp.read()
        except:
            # TODO log error
            print "Error fetching", url_str
        
        return None


    def read_already_fetched_files(self, geo_type, geo_dict):

        if geo_type == "state":
            state_code = geo_dict['state_code']
            file_list = glob.glob(self.data_dir + '/state/'+ state_code + '/*/' + state_code + '.xml')
            parser_func = DataParser.parse_get_state_stats_resp

        elif geo_type == "zipcode":
            zipcode = geo_dict['zipcode']
            file_list = glob.glob(self.data_dir + '/zipcode/'+ zipcode + '/*/' + zipcode + '.xml')    
            parser_func = DataParser.parse_get_zipcode_stats_resp

        elif geo_type == "county":
            state_code = geo_dict['state_code']
            county = geo_dict['county']
            county_ = '_'.join(county.split(' '))
            file_list = glob.glob(self.data_dir + '/county/' + state_code + '/' + county_ + '/*/' + county_ + '.xml')
            
            parser_func = DataParser.parse_get_county_stats_resp

        elif geo_type == "city":
            state_code = geo_dict['state_code']
            city = geo_dict['city']
            city_ = '_'.join(city.split(' '))
            file_list = glob.glob(self.data_dir + '/city/' + state_code + '/' + city_ + '/*/' + city_ + '.xml')

            parser_func = DataParser.parse_get_city_stats_resp

        for file_ in file_list:
            with open(file_, 'r') as stream:

                text = stream.read()
                json_doc = self.parse_executor(parser_func, text)
                if json_doc is None: 
                    return

                # overwrite what the parser wrote
                json_doc['date_fetched'] = file_.split('/')[-2] 
                
                print 'read data up to', json_doc['most_recent_week'], 
                print ', fetched on', json_doc['date_fetched']
                
                self.write_executor(json_doc, geo_type, geo_dict)


    def save_xml_file(self, text, geo_type, geo_dict):

        now_date = TruliaDataFetcher.get_current_date()
        dest_dir = self.data_dir + '/' + geo_type + '/'

        if geo_type != 'zipcode':
            dest_dir += geo_dict['state_code'] + '/'

        # unfortunate 1-offs
        if geo_type == 'state': 
            filename = geo_dict['state_code'] + '.xml'
        else:
            geo_spaced = '_'.join(geo_dict[geo_type].split(' '))
            dest_dir += geo_spaced + '/'
            filename = geo_spaced + '.xml'

        dest_dir += now_date

        try:
            os.makedirs(dest_dir)
        except OSError:
            pass # already exists, ignore
        
        with open(dest_dir + "/" + filename, 'w') as stream:
            stream.write(str(text))


    def send_json_doc_records_to_fluentd(self, match_rule, json_doc):

        metadata_keys = json_doc.keys()
        metadata_keys.remove('stats')

        metadata_dict = {}
        for key in metadata_keys:
            metadata_dict[key] = json_doc[key]

        for k_bed in json_doc['stats']:
            for week in json_doc['stats'][k_bed]:
                rec = dict(metadata_dict)
                rec['num_beds'] = k_bed
                rec['week_ending_date'] = week
                rec['avg_list'] = json_doc['stats'][k_bed][week]['a']
                rec['num_list'] = json_doc['stats'][k_bed][week]['n']
                event.Event(match_rule, rec)


    @staticmethod
    def get_current_date():
        return time.strftime("%Y-%m-%d")


    @staticmethod
    def is_str_positive_int(k_bed):
        try:
            k = int(k_bed)
            if k < 0:
                return False
        except:
                return False

        return True


def fetch(tdf, fetch_geo_items):

    print "fetching items", fetch_geo_items
    if 's' in fetch_geo_items:
        tdf.fetch_all_states_data()
    if 'o' in fetch_geo_items:
        tdf.fetch_all_counties_all_states_data()
    if 'c' in fetch_geo_items:
        tdf.fetch_all_cities_all_states_data()
    if 'z' in fetch_geo_items:
        tdf.fetch_all_zipcodes_data()

def load(tdf, load_geo_items):

    print "loading items", load_geo_items
    if 's' in load_geo_items:
        tdf.load_all_states_from_xml_archive()
    if 'o' in load_geo_items:
        tdf.load_all_counties_from_xml_archive()
    if 'c' in load_geo_items:
        tdf.load_all_cities_from_xml_archive()
    if 'z' in load_geo_items:
        tdf.load_all_zipcodes_from_xml_archive()


def main(argv):

    [fetch_geo_items, load_geo_items, kv_store, fluentd_rx] = parse_args(argv)
     
    if fetch_geo_items == "" and load_geo_items == "":
        usage()
    
    params_dict = {"kv_store":kv_store, "fluentd_rx":fluentd_rx}

    tdf = TruliaDataFetcher('../conf/', params_dict)

    if len(load_geo_items) > 0:
        load(tdf, load_geo_items)

    if len(fetch_geo_items) > 0:
        fetch(tdf, fetch_geo_items)


# program
if __name__ == "__main__":
    main(sys.argv[1:])
