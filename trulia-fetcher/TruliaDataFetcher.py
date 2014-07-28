import urllib2 
import time
import sys
import os
import Queue
import datetime
import glob
from xml.dom import minidom
from fluent import sender
from fluent import event
from pprint import pprint as pprint

common_path = "../common/"
sys.path.append(common_path)
import TruliaConfLoader
import DatabaseManager
import HBaseManager
import wrappers
import RedisManager
import DataParser


class TruliaDataFetcher:
    def __init__(self, config_path):
        trulia_conf = TruliaConfLoader.TruliaConfLoader(config_path)
        self.load_trulia_params(trulia_conf)
        self.db_mgr = DatabaseManager.DatabaseManager(config_path)

        # TODO make this a configuration decision
        self.kv_mgr = RedisManager.RedisManager()
        #self.kv_mgr = HBaseManager.HBaseManager()
        
        self.init_fluent()


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


    def init_fluent(self):
        sender.setup('hdfs')


    def load_all_states_from_xml_archive(self):
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            print "loading state from archive:",state_code
            self.read_already_fetched_files("ST",{"state_code":state_code})


    def load_all_zipcodes_from_xml_archive(self):
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_zipcode", "zipcode")
        for zipcode_tuple in list(res):
            zipcode = zipcode_tuple[0]
            zipcode = str(100000 + zipcode)[1:]
            print "loading zipcode from archive:",zipcode 
            self.read_already_fetched_files("ZP",{"zipcode":zipcode})


    def load_all_cities_from_xml_archive(self):
        state_tuples = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(state_tuples):
            state_code = state_code_tuple[0]
            city_tuples = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_city", "city", "state_code = '" + state_code + "'")
            for city_tuple in list(city_tuples):
                city = city_tuple[0]
                print "loading city from archive:", city, ",", state_code 
                self.read_already_fetched_files("CT",{"state_code":state_code, "city":city})


    def load_all_counties_from_xml_archive(self):
        state_tuples = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(state_tuples):
            state_code = state_code_tuple[0]
            county_tuples = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_county", "county", "state_code = '" + state_code + "'")
            for county_tuple in list(county_tuples):
                county = county_tuple[0]
                print "loading county from archive:", county, ",", state_code 
                self.read_already_fetched_files("CO",{"state_code":state_code, "county":county})


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
        if geo_type == "county" or geo_type == "city":
            metadata_key_list = [ k + " = '" + geo_dict[k] + "'" for k in geo_dict]
        elif geo_type == "state":
            metadata_key_list = ["state_code = '" + geo_dict['state_code'] + "'"]
        elif geo_type == "zipcode":
            metadata_key_list = ["zipcode = '" + geo_dict['zipcode'] + "'"]

        # send to HDFS
        # self.send_accum_fluentd_records('state.all_listing_stats', fluentd_accum)        
        # send to kv store
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


    # TODO refactor this function
    def read_already_fetched_files(self, geo_type, geo_dict):

        # TODO geo_types here are ST for state, CT for city... fix
        if geo_type == "ST":
            state_code = geo_dict['state_code']
            file_list = glob.glob(self.data_dir + '/state/'+ state_code + '/*/' + state_code + '.xml')
            for file_ in file_list:
                with open(file_, 'r') as stream:
                    # check if already read in
                    date_fetched = file_.split('/')[-2]
                    date_fetched_in_DB = self.get_latest_api_call_date("state",{"state_code":state_code})
                    if date_fetched_in_DB != date_fetched:
                        text = stream.read()
                        json_doc = self.parse_executor(DataParser.parse_get_state_stats_resp, text)
                        if json_doc is None: 
                            return

                        json_doc['date_fetched'] = file_.split('/')[-2] # overwrite what the parser wrote
                        print 'read local data with most recent week', json_doc['most_recent_week'], ", fetched on", json_doc['date_fetched']
                        self.write_executor(json_doc, "state", geo_dict)

        elif geo_type == "ZP":
            zipcode = geo_dict['zipcode']
            file_list = glob.glob(self.data_dir + '/zipcode/'+ zipcode + '/*/' + zipcode + '.xml')
            for file_ in file_list:
                with open(file_, 'r') as stream:
                    # check if already read in
                    date_fetched = file_.split('/')[-2]
                    date_fetched_in_DB = self.get_latest_api_call_date("zipcode",{"zipcode":zipcode})
                    if date_fetched_in_DB != date_fetched:
                        text = stream.read()
                        json_doc = self.parse_executor(DataParser.parse_get_zipcode_stats_resp, text)
                        if json_doc is None: 
                            return

                        json_doc['date_fetched'] = file_.split('/')[-2] # overwrite what the parser wrote
                        print 'read local data with most recent week', json_doc['most_recent_week'], ", fetched on", json_doc['date_fetched']
                        self.write_executor(json_doc, "zipcode", geo_dict)

        elif geo_type == "CO":
            state_code = geo_dict['state_code']
            county = geo_dict['county']
            county_ = '_'.join(county.split(' '))
            file_list = glob.glob(self.data_dir + '/county/' + state_code + '/' + county_ + '/*/' + county_ + '.xml')
            for file_ in file_list:
                with open(file_, 'r') as stream:
                    # check if already read in
                    date_fetched = file_.split('/')[-2]
                    date_fetched_in_DB = self.get_latest_api_call_date("county",{"state_code":state_code,"county":county})
                    if date_fetched_in_DB != date_fetched:
                        text = stream.read()
                        json_doc = self.parse_executor(DataParser.parse_get_county_stats_resp, text)
                        if json_doc is None: 
                            return

                        json_doc['date_fetched'] = file_.split('/')[-2] # overwrite what the parser wrote
                        print 'read local data with most recent week', json_doc['most_recent_week'], ", fetched on", json_doc['date_fetched']

                        self.write_executor(json_doc, "county", geo_dict)
                                        
        elif geo_type == "CT":
            state_code = geo_dict['state_code']
            city = geo_dict['city']
            city_ = '_'.join(city.split(' '))
            file_list = glob.glob(self.data_dir + '/city/' + state_code + '/' + city_ + '/*/' + city_ + '.xml')
            for file_ in file_list:
                with open(file_, 'r') as stream:
                    # check if already read in
                    date_fetched = file_.split('/')[-2]
                    date_fetched_in_DB = self.get_latest_api_call_date("city",{"state_code":state_code,"city":city})
                    if date_fetched_in_DB != date_fetched:
                        text = stream.read()
                        json_doc = self.parse_executor(DataParser.parse_get_city_stats_resp, text)
                        if json_doc is None: 
                            return

                        json_doc['date_fetched'] = file_.split('/')[-2] # overwrite what the parser wrote
                        print 'read local data with most recent week', json_doc['most_recent_week'], ", fetched on", json_doc['date_fetched']

                        self.write_executor(json_doc, "city", geo_dict)
                    
                    

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


    def send_accum_fluentd_records(self, match_rule, fluentd_accum):
        for record in fluentd_accum:
            event.Event(match_rule, record)


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

def main():

    #dm = DatabaseManager.DatabaseManager('../conf/')
    #dm.reset_data_metadata_tables()

    tdf = TruliaDataFetcher('../conf/')

    #tdf.load_all_states_from_xml_archive()
    #tdf.load_all_counties_from_xml_archive()
    #tdf.load_all_cities_from_xml_archive()
    tdf.load_all_zipcodes_from_xml_archive()

    #tdf.fetch_all_states_data()
    #tdf.fetch_all_counties_all_states_data()
    #tdf.fetch_all_cities_all_states_data()
    #tdf.fetch_all_zipcodes_data()


# program
if __name__ == "__main__":
    main()
