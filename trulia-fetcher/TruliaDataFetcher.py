import urllib2 
import time
import threading
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
#import FetcherWorker
#import ParserWorker
#import DatastoreWriterWorker


class TruliaDataFetcher:
    def __init__(self, config_path):
        trulia_conf = TruliaConfLoader.TruliaConfLoader(config_path)
        self.load_trulia_params(trulia_conf)
        self.db_mgr = DatabaseManager.DatabaseManager(config_path)

        # TODO make this a configuration decision
        self.kv_mgr = RedisManager.RedisManager()
        #self.kv_mgr = HBaseManager.HBaseManager()
        
        self.init_fluent()

        # lock for threads to use to add to fetch_metadata
        self.lock = threading.Lock()
        self.fetch_metadata = list()


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


    def load_all_states_from_xml_archive(self):
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            print "loading state from archive:",state_code
            self.read_already_fetched_files("ST",{"state_code":state_code})


    def load_all_zipcodes_from_xml_archive(self):
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_zipcode", "zipcode")
        for zipcode_tuple in list(res):
            zipcode = state_code_tuple[0]
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
            print county_tuples
            for county_tuple in list(county_tuples):
                county = county_tuple[0]
                print "loading county from archive:", county, ",", state_code 
                self.read_already_fetched_files("CT",{"state_code":state_code, "county":county})


    def fetch_all_states_data(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")

        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            fetched = self.fetch_state(state_code)
            if fetched: 
                time.sleep(2.0/len(self.apikeys)) # trulia api restriction


    def fetch_all_cities_all_states_data(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            print "Fetching cities in ", state_code
            self.fetch_all_cities_in_state_data(state_code)


    def fetch_all_cities_in_state_data(self, state_code):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_city", "city", "state_code = '" + state_code + "'")
        for city_tuple in list(res):
            city = city_tuple[0]
            fetched = self.fetch_city(city, state_code)
            if fetched:
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
            fetched = self.fetch_county(county, state_code)
            if fetched:
                time.sleep(2.0/len(self.apikeys)) # trulia api restriction 


    def fetch_all_zipcodes_data(self): 

        # distinct needed since some zipcodes span multiple states (as reported by Trulia)
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_zipcode", "distinct zipcode")
        for zipcode_tuple in list(res):
            zipcode = zipcode_tuple[0]
            zipcode = str(100000 + zipcode)[1:] 
            self.fetch_zipcode(zipcode)
            time.sleep(2.0/len(self.apikeys)) # trulia api restriction 


    def fetch_state(self, state_code):

        # check if xml files exist about this topic and read that in
        self.read_already_fetched_files("ST",{"state_code":state_code})
        latest_api_call_date = self.get_latest_api_call_date("state",{"state_code":state_code})
        now_date = TruliaDataFetcher.get_current_date()

        print "loading data about state:", state_code
        if now_date == latest_api_call_date:
            print "  already have logs fetched today"
            return False # Notify caller that we did not hit Trulia's API

        latest_rx_date = self.get_latest_list_stat_date("state",{"state_code":state_code})
        print "Calling API from", latest_rx_date, "to", now_date
        url_str = self.url + "library=" + self.stats_library + "&function=getStateStats&state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikeys[self.curr_key_idx]

        self.curr_key_idx = (self.curr_key_idx + 1) % len(self.apikeys)
        metadata_key_list = ["state_code = '" + state_code + "'"]
        metadata_table = "data_state"

        text = self.fetch_executor(url_str)
        if text is None:
            return False
        dest_dir = self.data_dir + '/state/'+ state_code + '/' + now_date + '/'
        file_name = state_code + '.xml'
        self.save_xml_file(text, dest_dir, file_name)

        json_doc = self.parse_executor(DataParser.parse_get_state_stats_resp, text)
        self.write_executor(json_doc, metadata_table, metadata_key_list)
        return True


    def fetch_city(self, city, state_code):

        # check if xml files exist about this topic and read that in
        self.read_already_fetched_files("CT",{"state_code":state_code,"city":city})

        latest_api_call_date = self.get_latest_api_call_date("city",{"state_code":state_code,"city":city})
        now_date = TruliaDataFetcher.get_current_date()

        print "loading data about city:", city, ",", state_code
        if now_date == latest_api_call_date:
            print "already have logs fetched today"
            return False # Notify caller that we did not hit Trulia's API 

        latest_rx_date = self.get_latest_list_stat_date("city",{"state_code":state_code,"city":city})
        print "Calling API from", latest_rx_date, "to", now_date, "about", city, state_code, 
        city_spaced = '%20'.join(city.split(' '))

        url_str = self.url + "library=" + self.stats_library + "&function=getCityStats&city=" + city_spaced + "&state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikeys[self.curr_key_idx]

        self.curr_key_idx = (self.curr_key_idx + 1) % len(self.apikeys)
        metadata_table = "data_city"
        metadata_key_list = ["state_code = '" + state_code + "'", "city ='" + city + "'"]

        text = self.fetch_executor(url_str)
        if text is None:
            return False
        city_ = '_'.join(city.split(' '))
        dest_dir = self.data_dir + '/city/'+ state_code + '/' + city_ + '/' + now_date + '/'
        file_name = city_ + '.xml'
        self.save_xml_file(text, dest_dir, file_name)

        json_doc = self.parse_executor(DataParser.parse_get_city_stats_resp, text)
        self.write_executor(json_doc, metadata_table, metadata_key_list)
        return True
            

    def fetch_county(self, county, state_code):

        # check if xml files exist about this topic and read that in
        self.read_already_fetched_files("CO",{"state_code":state_code,"county":county})

        last_api_call_date = self.get_latest_api_call_date("county",{"state_code":state_code,"county":county})
        now_date = TruliaDataFetcher.get_current_date()

        print "fetching data about county:", county, ",", state_code,
        if now_date == last_api_call_date:
            print "already have logs fetched today"
            return False # Notify caller that we did not hit Trulia's API 

        latest_rx_date = self.get_latest_list_stat_date("city",{"state_code":state_code,"county":county})
        print "Calling API from", latest_rx_date, "to", now_date
        county_spaced = '%20'.join(county.split(' '))

        url_str = self.url + "library=" + self.stats_library + "&function=getCountyStats&county=" + county_spaced + "&state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikeys[self.curr_key_idx]

        self.curr_key_idx = (self.curr_key_idx + 1) % len(self.apikeys)
        metadata_table = "data_county"
        metadata_key_list = ["state_code = '" + state_code + "'", "county ='" + county + "'"]

        text = self.fetch_executor(url_str)
        if text is None:
            return False
        county_ = '_'.join(county.split(' '))
        dest_dir = self.data_dir + '/county/'+ state_code + '/' + county_ + '/' + now_date + '/'
        file_name = county_ + '.xml'
        self.save_xml_file(text, dest_dir, file_name)

        json_doc = self.parse_executor(DataParser.parse_get_county_stats_resp, text)
        self.write_executor(json_doc, metadata_table, metadata_key_list)
        return True


    def fetch_zipcode(self, zipcode):

        # check if xml files exist about this topic and read that in
        self.read_already_fetched_files("ZP",{"zipcode":state_code})

        last_api_call_date = self.get_latest_api_call_date("zipcode",{"zipcode":zipcode})
        now_date = TruliaDataFetcher.get_current_date()

        print "fetching data about zipcode:", zipcode
        if now_date == last_api_call_date:
            print "already have logs fetched today"
            return False # Notify caller that we did not hit Trulia's API 

        latest_rx_date = self.get_latest_list_stat_date("zipcode",{"zipcode":zipcode})
        print "Calling API from", latest_rx_date, "to", now_date

        url_str = self.url + "library=" + self.stats_library + "&function=getZipCodeStats&zipCode=" + zipcode + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikeys[self.curr_key_idx]

        self.curr_key_idx = (self.curr_key_idx + 1) % len(self.apikeys)
        metadata_table = "data_zipcode"
        metadata_key_list = ["zipcode = " + zipcode ]

        text = self.fetch_executor(url_str)
        if text is None:
            return False
        dest_dir = self.data_dir + '/zipcode/'+ zipcode + '/' + now_date + '/'
        file_name = zipcode + '.xml'
        self.save_xml_file(text, dest_dir, file_name)

        json_doc = self.parse_executor(DataParser.parse_get_zipcode_stats_resp, text)
        self.write_executor(json_doc, metadata_table, metadata_key_list)
        return True


    def write_executor(self, json_doc, metadata_table, metadata_key_list):

        # send to HDFS
        #self.send_accum_fluentd_records('state.all_listing_stats', fluentd_accum)
        
        # send to kv store
        self.kv_mgr.insert_json_doc_records(json_doc)
            
        # update meta-store
        most_recent_week = json_doc['most_recent_week']
        date_fetched = json_doc['date_fetched']
        
        self.db_mgr.establish_timestamp_and_most_recent_week(self.db_mgr.conn, metadata_table, most_recent_week, date_fetched, metadata_key_list)


    # short function, may expand to threads or generator design pattern
    def parse_executor(self, parse_func, text):
        json_doc = parse_func(text)
        json_doc['date_fetched'] = datetime.datetime.now().strftime('%Y-%m-%d')
        return json_doc
        

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

        if geo_type == "ST":
            state_code = geo_dict['state_code']
            file_list = glob.glob(self.data_dir + '/state/'+ state_code + '/*/' + state_code + '.xml')
            for file_ in file_list:
                with open(file_, 'r') as stream:
                    text = stream.read()
                    json_doc = self.parse_executor(DataParser.parse_get_state_stats_resp, text)
                    json_doc['date_fetched'] = file_.split('/')[-2] # overwrite what the parser wrote
                    print 'read local data with most recent week', json_doc['most_recent_week'], ", fetched on", json_doc['date_fetched']
                    self.write_executor(json_doc, "data_state", ["state_code = '" + state_code + "'"])

        elif geo_type == "ZP":
            zipcode = geo_dict['zipcode']
            file_list = glob.glob(self.data_dir + '/zipcode/'+ zipcode + '/*/' + zipcode + '.xml')
            for file_ in file_list:
                with open(file_, 'r') as stream:
                    text = stream.read()
                    json_doc = self.parse_executor(DataParser.parse_get_zipcode_stats_resp, text)
                    json_doc['date_fetched'] = file_.split('/')[-2] # overwrite what the parser wrote
                    print 'read local data with most recent week', json_doc['most_recent_week'], ", fetched on", json_doc['date_fetched']
                    self.write_executor(json_doc, "data_zipcode", ["zipcode = '" + zipcode + "'"])

        elif geo_type == "CO":
            state_code = geo_dict['state_code']
            county = geo_dict['county']
            county_ = '_'.join(county.split(' '))
            file_list = glob.glob(self.data_dir + '/county/' + state_code + '/' + county_ + '/*/' + county_ + '.xml')
            for file_ in file_list:
                with open(file_, 'r') as stream:
                    text = stream.read()
                    json_doc = self.parse_executor(DataParser.parse_get_county_stats_resp, text)
                    json_doc['date_fetched'] = file_.split('/')[-2] # overwrite what the parser wrote
                    print 'read local data with most recent week', json_doc['most_recent_week'], ", fetched on", json_doc['date_fetched']

                    self.write_executor(json_doc, "data_county", ["state_code = '" + state_code + "'","county = '" + county + "'"])
                                        
        elif geo_type == "CT":
            state_code = geo_dict['state_code']
            city = geo_dict['city']
            city_ = '_'.join(city.split(' '))
            file_list = glob.glob(self.data_dir + '/city/' + state_code + '/' + city_ + '/*/' + city_ + '.xml')
            for file_ in file_list:
                with open(file_, 'r') as stream:
                    text = stream.read()
                    json_doc = self.parse_executor(DataParser.parse_get_city_stats_resp, text)
                    json_doc['date_fetched'] = file_.split('/')[-2] # overwrite what the parser wrote
                    print 'read local data with most recent week', json_doc['most_recent_week'], ", fetched on", json_doc['date_fetched']

                    self.write_executor(json_doc, "data_city", ["state_code = '" + state_code + "'","city = '" + city + "'"])
                    
                    

    def save_xml_file(self, text, dest_dir, file_name):

        try:
            os.makedirs(dest_dir)
        except OSError:
            pass # already exists, ignore
        
        with open(dest_dir + "/" + file_name, 'w') as stream:
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



# program
if __name__ == "__main__":

    #dm = DatabaseManager.DatabaseManager('../conf/')
    #dm.reset_data_metadata_tables()

    tdf = TruliaDataFetcher('../conf/')

    tdf.load_all_states_from_xml_archive()
    tdf.load_all_counties_from_xml_archive()
    tdf.load_all_cities_from_xml_archive()
    tdf.load_all_zipcodes_from_xml_archive()

    # Stable functions, but single threaded
    #tdf.fetch_all_states_data()
    #tdf.fetch_all_counties_all_states_data()
    #tdf.fetch_all_cities_all_states_data()
    #tdf.fetch_all_zipcodes_data()
    
    # not much faster
    #tdf.fetch_all_states_data_threaded()
    #tdf.fetch_all_states_data_queue_threaded()

    #Debugging section
    '''
    tdf.fetch_city('Alpaugh','CA')
    
    fh = open("/home/ubuntu/test/Apple_Valley.xml")
    text = fh.read()
    fh.close()
    TruliaDataFetcher.parse_get_city_stats_resp(text)
    '''

'''
    def fetch_all_states_data_queue_threaded(self):
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        state_codes = [sc_tup[0] for sc_tup in res]
        match_rule = 'state.all_list_stats'
        metadata_table = 'data_state'

        num_fetchers = len(self.apikeys)
        num_parsers = num_fetchers * 4
        num_datastore_writers = 3  # only 3 datastore writer types (hbase, fluentd, mysql)
        
        url_queue = Queue.Queue(num_fetchers)
        text_queue = Queue.Queue(num_parsers)
        records_queue = Queue.Queue(num_datastore_writers)

        locks = {'hbase':threading.Lock(), 'mysql':threading.Lock(), 'fluentd':threading.Lock()}
        writers = {'hbase':self.hbase_mgr.state_stats_table, 'mysql':self.db_mgr, 'fluentd':event}

        for i in xrange(num_fetchers):
            FetcherWorker.FetcherWorker(url_queue, self.apikeys[i], match_rule, writers, locks, metadata_table).start()
            
        #for i in xrange(num_parsers):
        #    ParserWorker.ParserWorker(text_queue,records_queue,DataParser.parse_get_state_stats_resp).start()

        #for i in xrange(num_datastore_writers):
        #    DatastoreWriterWorker.DatastoreWriterWorker(records_queue, writers, locks, match_rule, metadata_table).start()

        for state_code in state_codes:
            latest_api_call_date = self.get_latest_api_call_date("state",{"state_code":state_code})
            now_date = TruliaDataFetcher.get_current_date()
            url_str = self.url + "library=" + self.stats_library + "&function=getStateStats&\
state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=list\
ings" + "&apikey="
            # put url and primary key(s) value(s) of metadata table in mysql
            url_queue.put((url_str,[state_code]))

        for i in xrange(num_fetchers):
            url_queue.put(None) # add end-of-queue markers

        #url_queue.join()


    def fetch_all_states_data_threaded(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")

        state_codes = list(res)
        state_code_idx = 0;
        t1 = time.time()
        
        while state_code_idx < len(state_codes):
           
            threads = list()
            self.fetch_metadata = list()
            self.hbase_threads_accum = list()
            self.fluentd_threads_accum = list()

            num_threads = min(len(self.apikeys), len(state_codes)-state_code_idx)
            
            for thread_idx in xrange(num_threads):
                state_code = state_codes[state_code_idx][0]

                latest_rx_date = self.get_latest_api_call_date("state",{"state_code":state_code})

                now_date = TruliaDataFetcher.get_current_date()
                print 'fetching state', state_code, latest_rx_date, now_date

                url_str = self.url + "library=" + self.stats_library + "&function=getStateStats&state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikeys[thread_idx]

                t = threading.Thread(target=self.fetch_state_threaded, args=[url_str, state_code, send_to_hdfs, now_date])
                
                threads.append(t)
                state_code_idx+=1

            # start down here because mysql calls above
            for t in threads:
                t.start()

            for t in threads:
                t.join()

            for fluentd_accum in self.fluentd_threads_accum:
                self.send_accum_fluentd_records('state.all_listing_stats', fluentd_accum)

            for hbase_accum in self.hbase_threads_accum:
                self.insert_accum_hbase_records(hbase_accum)

            for val_str in self.fetch_metadata:
                self.db_mgr.simple_insert_query(self.db_mgr.conn, "data_state", val_str)  

            # make sure we don't use the same API key within 2 seconds
            t2 = time.time()
            if t2 - t1 < 2.0:
                time.sleep(2.0 - (t2 - t1))
            t1 = t2


    def fetch_state_threaded(self, url_str, state_code, send_to_hdfs, now_date):

        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            text = resp.read()

            fluentd_accum, hbase_accum = DataParser.parse_get_state_stats_resp(text)

            self.lock.acquire()  
            self.hbase_threads_accum.append(hbase_accum)
            self.fluentd_threads_accum.append(fluentd_accum)
            self.fetch_metadata.append("('" + state_code + "',  '" + now_date + "', NOW())")
            self.lock.release()
'''
