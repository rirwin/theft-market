import urllib2 
import time
import threading
import sys
import os
import Queue
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
#import FetcherWorker
#import ParserWorker
#import DatastoreWriterWorker


class TruliaDataFetcher:
    def __init__(self, config_path):
        trulia_conf = TruliaConfLoader.TruliaConfLoader(config_path)
        self.load_trulia_params(trulia_conf)
        self.db_mgr = DatabaseManager.DatabaseManager(config_path)
        self.redis_mgr = RedisManager.RedisManager()

        #self.hbase_mgr = HBaseManager.HBaseManager()
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


    def get_latest_rx_date(self, obj_type, obj_key_dict):
        table_str = "data_" + obj_type
        where_str = ""
        for k in obj_key_dict:
            where_str += k + " = '" + str(obj_key_dict[k]) + "' and "
        where_str = where_str[:-4]
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, table_str, "most_recent_week", where_str)
        if len(res) == 0:
            latest_rx_date = "2000-01-01"
        else:
            latest_rx_date = str(res[0][0])
        return latest_rx_date


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
            latest_rx_date = self.get_latest_rx_date("state",{"state_code":state_code})
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

                latest_rx_date = self.get_latest_rx_date("state",{"state_code":state_code})

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


    def fetch_all_states_data(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")

        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            fetched = self.fetch_state(state_code)
            if fetched: 
                time.sleep(2.0/len(self.apikeys)) # trulia api restriction


    def fetch_state(self, state_code):
        latest_rx_date = self.get_latest_rx_date("state",{"state_code":state_code})
        now_date = TruliaDataFetcher.get_current_date()

        print "fetching state data about", state_code, "data from", latest_rx_date, "to", now_date,
        if now_date == latest_rx_date:
            print "already have fetched today"
            # Notify caller that we did not hit Trulia's API
            return False
        print ""
        
        url_str = self.url + "library=" + self.stats_library + "&function=getStateStats&state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikeys[self.curr_key_idx]

        self.curr_key_idx = (self.curr_key_idx + 1) % len(self.apikeys)
        metadata_key_list = ["state_code = '" + state_code + "'"]
        metadata_table = "data_state"

        text = self.fetch_executor(url_str)
        if text is None:
            return False
        json_doc = self.parse_executor(TruliaDataFetcher.parse_get_state_stats_resp, text)
        self.write_executor(json_doc, metadata_table, now_date, metadata_key_list)
        return True


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
            

    def fetch_city(self, city, state_code):

        latest_rx_date = self.get_latest_rx_date("city",{"state_code":state_code})
        now_date = TruliaDataFetcher.get_current_date()

        print "fetching city data about", city, state_code, "from", latest_rx_date, "to", now_date
        city_spaced = '%20'.join(city.split(' '))

        if now_date == latest_rx_date:
            print "already have fetched today"
            # Notify caller that we did not hit Trulia's API
            return False
        print ""

        url_str = self.url + "library=" + self.stats_library + "&function=getCityStats&city=" + city_spaced + "&state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikeys[self.curr_key_idx]

        self.curr_key_idx = (self.curr_key_idx + 1) % len(self.apikeys)
        metadata_table = "data_city"
        metadata_key_list = ["state_code = '" + state_code + "'", "city ='" + city + "'"]

        text = self.fetch_executor(url_str)
        json_doc = self.parse_executor(TruliaDataFetcher.parse_get_city_stats_resp, text)
        self.write_executor(json_doc, metadata_table, now_date, metadata_key_list)
        sys.exit(0)


    def fetch_all_counties_all_states_data(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            print "Fetching cities in ", state_code
            self.fetch_all_counties_in_state_data(state_code)


    def fetch_all_counties_in_state_data(self, state_code):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_county", "county", "state_code = '" + state_code + "'")
        for county_tuple in list(res):
            county = county_tuple[0]
            fetched = self.fetch_county(county, state_code)
            if fetched:
                time.sleep(2.0/len(self.apikeys)) # trulia api restriction 
            

    def fetch_county(self, county, state_code):

        latest_rx_date = self.get_latest_rx_date("county",{"state_code":state_code})
        now_date = TruliaDataFetcher.get_current_date()

        print "fetching county data about", county, state_code, "from", latest_rx_date, "to", now_date
        county_spaced = '%20'.join(county.split(' '))

        if now_date == latest_rx_date:
            print "already have fetched today"
            # Notify caller that we did not hit Trulia's API
            return False
        print ""

        url_str = self.url + "library=" + self.stats_library + "&function=getCountyStats&county=" + county_spaced + "&state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikeys[self.curr_key_idx]

        self.curr_key_idx = (self.curr_key_idx + 1) % len(self.apikeys)
        metadata_table = "data_county"
        metadata_key_list = ["state_code = '" + state_code + "'", "county ='" + county + "'"]

        text = self.fetch_executor(url_str)
        json_doc = self.parse_executor(TruliaDataFetcher.parse_get_county_stats_resp, text)
        self.write_executor(json_doc, metadata_table, now_date, metadata_key_list)


    def fetch_all_zipcodes_data(self): 

        # distinct needed since some zipcodes span multiple states (as reported by Trulia)
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_zipcode", "distinct zipcode")
        for zipcode_tuple in list(res):
            zipcode = zipcode_tuple[0]
            zipcode = str(100000 + zipcode)[1:] 
            self.fetch_zipcode_data(zipcode)
            time.sleep(2.0/len(self.apikeys)) # trulia api restriction 


    def fetch_zipcode_data(self, zipcode):

        latest_rx_date = self.get_latest_rx_date("zipcode",{"zipcode":zipcode})
        now_date = TruliaDataFetcher.get_current_date()
        print "fetching zipcode data about", zipcode, "from", latest_rx_date, "to", now_date
        url_str = self.url + "library=" + self.stats_library + "&function=getZipCodeStats&zipCode=" + zipcode + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikeys[self.curr_key_idx]
        self.curr_key_idx = (self.curr_key_idx + 1) % len(self.apikeys)
        metadata_table = "data_zipcode"
        metadata_key_list = ["zipcode = " + zipcode ]

        text = self.fetch_executor(url_str)
        json_doc = self.parse_executor(TruliaDataFetcher.parse_get_zipcode_stats_resp, text)
        self.write_executor(json_doc, metadata_table, now_date, metadata_key_list)


    def write_executor(self, json_doc, metadata_table, most_recent_week, metadata_key_list):
        # send to archive store
        # TODO send to S3
        # send to HDFS
        #self.send_accum_fluentd_records('state.all_listing_stats', fluentd_accum)
        
        # send to kv store
        #self.insert_accum_hbase_records(hbase_accum)
        self.insert_accum_redis_records(json_doc)
            
        # update meta-store
        most_recent_week = TruliaDataFetcher.get_current_date()

        self.db_mgr.establish_timestamp_and_most_recent_week(self.db_mgr.conn, metadata_table, most_recent_week, metadata_key_list)


    # short function, may expand to threads or generator design pattern
    def parse_executor(self, parse_func, text):
        json_doc = parse_func(text)
        return json_doc
        

    def fetch_executor(self, url_str):

        resp = urllib2.urlopen(url_str)
 
        if resp.code == 200:
            return resp.read()
            
        else: # simple retry
            time.sleep(5)
            resp = urllib2.urlopen(url_str)
            if resp.code == 200:
                return resp.read()
        
        return None


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


    def insert_accum_hbase_records(self, hbase_accum):
        for key in hbase_accum:
            try:
                self.hbase_mgr.state_stats_table.put(key, hbase_accum[key])
            except:
                print "Exception while inserting", key, "into HBase new function"


    def insert_accum_redis_records(self, json_doc):
        if json_doc['doc_type'] == 'state_record':
            geo_label = json_doc['state_code']
            namespace = 'ST'
        elif json_doc['doc_type'] == 'county_record':
            geo_label = json_doc['state_code'] + '-' + '_'.join(json_doc['county'].lower().split(' '))
            namespace = 'CO'
        elif json_doc['doc_type'] == 'zipcode_record':
            geo_label = json_doc['zipcode']
            namespace = 'ZP'
        elif json_doc['doc_type'] == 'city_record':
            geo_label = json_doc['state_code'] + '-' + '_'.join(json_doc['city'].lower().split(' '))
            namespace = 'CI'

        for bed_i in json_doc['stats']:
            for week_i in json_doc['stats'][bed_i]:
                rec = json_doc['stats'][bed_i][week_i]
                avg = rec['a']
                num = rec['n']
                self.redis_mgr.set_listing(namespace, geo_label, bed_i, week_i, avg, num)


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


    # Static parsing methods
    # each are manual because we have a different schema
    # and each handles dirty data in different ways
    # TODO merge some common functionalities

    # TODO add redis and clean up code that has hbase and fluentd baked in too much

    @staticmethod
    def parse_get_state_stats_resp(text):
       
        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('listingStat')
        state_code = head_dom.getElementsByTagName('stateCode')[0].firstChild.nodeValue

        # No key, do not log
        if len(state_code) != 2:
            return
        else:
            stats_doc = TruliaDataFetcher.parse_week_listings(dom_list)
            head_doc = {}
            head_doc['doc_type'] = 'state_record'
            head_doc['state_code'] = state_code
            head_doc['stats'] = stats_doc
            return head_doc


    @staticmethod
    def parse_get_city_stats_resp(text):
       
        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('listingStat')
        state_code = head_dom.getElementsByTagName('state')[0].firstChild.nodeValue
        city = head_dom.getElementsByTagName('city')[0].firstChild.nodeValue

        # No key, do not log
        if len(state_code) != 2:
            return
        else:
            stats_doc = TruliaDataFetcher.parse_week_listings(dom_list)
            head_doc = {}
            head_doc['doc_type'] = 'city_record'
            head_doc['state_code'] = state_code
            head_doc['city'] = city
            head_doc['stats'] = stats_doc
            return head_doc


    @staticmethod
    def parse_get_county_stats_resp(text):
       
        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('listingStat')
        state_code = head_dom.getElementsByTagName('state')[0].firstChild.nodeValue
        county = head_dom.getElementsByTagName('county')[0].firstChild.nodeValue

        # No key, do not log
        if len(state_code) != 2 and len(county) > 0:
            return
        else:
            stats_doc = TruliaDataFetcher.parse_week_listings(dom_list)
            head_doc = {}
            head_doc['doc_type'] = 'county_record'
            head_doc['state_code'] = state_code
            head_doc['county'] = county
            head_doc['stats'] = stats_doc
            return head_doc


    @staticmethod
    def parse_get_zipcode_stats_resp(text):
            
        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('listingStat')
        zipcode = head_dom.getElementsByTagName('zipCode')[0].firstChild.nodeValue

        # No key, do not log
        try:
            int(zipcode)
        except:
            return

        if len(zipcode) != 5:
            return
        else:
            stats_doc = TruliaDataFetcher.parse_week_listings(dom_list)
            head_doc = {}
            head_doc['doc_type'] = 'zipcode_record'
            head_doc['zipcode'] = zipcode
            head_doc['stats'] = stats_doc
            return head_doc


    @staticmethod
    def parse_week_listings(dom_list):

        # semi-structured json document
        json_doc = {}

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

                    if (TruliaDataFetcher.is_str_positive_int(k_bed)):
                        try:
                            sub_doc = {}
                            sub_doc['week_ending_date'] = str(week_ending_date)
                            sub_doc['num_beds'] = int(k_bed)

                            # carefully parsing of sub xml dom
                            listing_stat_dict = TruliaDataFetcher.parse_listing_stat(k_bed_i)

                            # merge keys
                            sub_doc = dict(listing_stat_dict.items() + listing_stat_dict.items())

                            # record aggregation
                            val = {'a': sub_doc['avg_list'], 'n' : sub_doc['num_list'] }

                            if k_bed not in json_doc:
                                json_doc[k_bed] = {}
                            json_doc[k_bed][week_ending_date] = val
                        except:
                            continue

        return json_doc


    @staticmethod
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

        if TruliaDataFetcher.is_str_positive_int(num_list):
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


# unit-test
if __name__ == "__main__":


    tdf = TruliaDataFetcher('../conf/')


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
