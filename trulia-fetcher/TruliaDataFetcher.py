import urllib2 
import time
import threading
import sys
import os
from xml.dom import minidom
import time

common_path = "../common/"
sys.path.append(common_path)
import TruliaConfLoader
import DatabaseManager
import HBaseManager
import wrappers


# TODO cleanup
fluent_path = "../extern/fluent-logger-python"
sys.path.append(fluent_path)
from fluent import sender
from fluent import event

class TruliaDataFetcher:
    def __init__(self, config_path):
        trulia_conf = TruliaConfLoader.TruliaConfLoader(config_path)
        self.load_trulia_params(trulia_conf)
        self.db_mgr = DatabaseManager.DatabaseManager(config_path)
        self.init_fluent()
        self.hbase_mgr = HBaseManager.HBaseManager()

        # lock for threads to use to add to val_strs
        self.lock = threading.Lock()
        self.val_strs = list()


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
        self.kafka_dir = trulia_conf.kafka_dir
        self.kafka_host = trulia_conf.kafka_host
        self.kafka_port = trulia_conf.kafka_port
        self.fluent_dir = trulia_conf.fluent_dir


    def init_fluent(self):
        #sys.path.append(self.fluent_dir)
        #import fluent
        #from fluent import sender
        #from fluent import event
        sender.setup('hdfs')


    def init_kafka(self):
        sys.path.append(self.kafka_dir)
        from kafka.client import KafkaClient
        from kafka.consumer import SimpleConsumer
        from kafka.producer import SimpleProducer, KeyedProducer
        self.kafka = KafkaClient(self.kafka_host + ":" + self.kafka_port)

 
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



    def fetch_all_states_data_threaded(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")

        state_codes = list(res)
        state_code_idx = 0;
        send_to_hdfs = False
        t1 = time.time()
        
        while state_code_idx < len(state_codes):
           
            threads = list()
            self.val_strs = list()
            num_threads = min(len(self.apikeys), len(state_codes)-state_code_idx)
            
            for thread_idx in xrange(num_threads):
                state_code = state_codes[state_code_idx][0]
                latest_rx_date = self.get_latest_rx_date("state",{"state_code":state_code})
                now_date = TruliaDataFetcher.get_current_date()
                url_str = self.url + "library=" + self.stats_library + "&function=getStateStats&state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikeys[thread_idx]

                t = threading.Thread(target=self.fetch_state_threaded, args=[url_str, state_code, send_to_hdfs, now_date])
       
                threads.append(t)
                state_code_idx+=1

            for t in threads:
                t.start()

            for t in threads:
                t.join()

            print self.val_strs
            sys.exit(0)
            for val_str in self.val_strs:
                self.db_mgr.simple_insert_query(self.db_mgr.conn, "data_state", val_str, send_to_hdfs)  

            # make sure we don't use the same API within 2 seconds
            t2 = time.time()
            if t2 - t1 < 2.0:
                time.sleep(2.0 - (t2 - t1))
            t1 = t2


    def fetch_state_threaded(self, url_str, state_code, send_to_hdfs, now_date):

        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            text = resp.read()
            dest_dir = '/Users/rirwin/data/state/' + state_code + '/fethed_on_' + '_'.join(now_date.split('-'))
            dest_file = state_code + ".xml"

            #self.save_xml_file(text, dest_dir, dest_file)
            TruliaDataFetcher.parse_get_state_stats_resp(text, self.db_mgr, self.hbase_mgr, send_to_hdfs)

            self.lock.acquire()
            self.val_strs.append("('" + state_code + "',  '" + latest_rx_date + "', NOW())")
            self.lock.release()
        


    def fetch_all_states_data(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            self.fetch_state(state_code)
            time.sleep(2.0/len(self.apikeys)) # trulia api restriction


    def fetch_state(self, state_code):
        latest_rx_date = self.get_latest_rx_date("state",{"state_code":state_code})
        now_date = TruliaDataFetcher.get_current_date()
        print "fetching state data about", state_code,"data from", latest_rx_date, "to", now_date
        
        url_str = self.url + "library=" + self.stats_library + "&function=getStateStats&state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikeys[self.curr_key_idx]

        self.curr_key_idx = (self.curr_key_idx + 1) % len(self.apikeys)

        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            text = resp.read()
            dest_dir = '/home/ubuntu/data/state/' + state_code + '/fethed_on_' + '_'.join(now_date.split('-'))
            dest_file = state_code + ".xml"

            self.save_xml_file(text, dest_dir, dest_file)
            TruliaDataFetcher.parse_get_state_stats_resp(text, self.db_mgr, self.hbase_mgr)
            self.db_mgr.simple_insert_query(self.db_mgr.conn, "data_state", "('" + state_code + "',  '" + latest_rx_date + "', NOW())")
        



    def fetch_all_cities_all_states_data(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            print state_code
            self.fetch_all_cities_in_state_data(state_code)


    def fetch_all_cities_in_state_data(self, state_code):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_city", "city", "state_code = '" + state_code + "'")
        for city_tuple in list(res):
            city = city_tuple[0]
            self.fetch_city(city, state_code)
            time.sleep(2.0/len(self.apikeys)) # trulia api restriction 


    def fetch_city(self, city, state_code):

        latest_rx_date = self.get_latest_rx_date("city",{"state_code":state_code})
        now_date = TruliaDataFetcher.get_current_date()

        print "fetching city data about", city, state_code, "from", latest_rx_date, "to", now_date
        city_spaced = '%20'.join(city.split(' '))
        url_str = self.url + "library=" + self.stats_library + "&function=getCityStats&city=" + city_spaced + "&state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikeys[self.curr_key_idx]
        self.curr_key_idx = (self.curr_key_idx + 1) % len(self.apikeys)

        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            text = resp.read()
            dest_dir = '/home/ubuntu/data/city/' + state_code + '/fethed_on_' + '_'.join(now_date.split('-'))
            dest_file = '_'.join(city.split(' ')) + ".xml"
            self.save_xml_file(text, dest_dir, dest_file)
            TruliaDataFetcher.parse_get_city_stats_resp(text, self.db_mgr, self.hbase_mgr)
            self.db_mgr.simple_insert_query(self.db_mgr.conn, "data_city", "('" + state_code + "', '" + city + "', '" + latest_rx_date + "', NOW())")
        else: # try twice
            time.sleep(5)
            resp = urllib2.urlopen(url_str)
            if resp.code == 200:
                text = resp.read()
                dest_dir = '/home/ubuntu/data/city/' + state_code + '/fethed_on_' + '_'.join(now_date.split('-'))
                dest_file = '_'.join(city.split(' ')) + ".xml"
                self.save_xml_file(text, dest_dir, dest_file)
                TruliaDataFetcher.parse_get_city_stats_resp(text, self.db_mgr, self.hbase_mgr)
                self.db_mgr.simple_insert_query(self.db_mgr.conn, "data_city", "('" + state_code + "', '" + city + "', '" + latest_rx_date + "', NOW())")



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

        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            text = resp.read()
            dest_dir = '/home/ubuntu/data/zipcode/' + zipcode + '/fethed_on_' + '_'.join(now_date.split('-'))
            dest_file = zipcode + ".xml"
            self.save_xml_file(text, dest_dir, dest_file)
            TruliaDataFetcher.parse_get_zipcode_stats_resp(text, self.db_mgr, self.hbase_mgr)
            self.db_mgr.simple_insert_query(self.db_mgr.conn, "data_zipcode", "(" + zipcode + ", '" + latest_rx_date + "', NOW())")
        else: # try twice
            time.sleep(5)
            resp = urllib2.urlopen(url_str)
            if resp.code == 200:
                text = resp.read()
                dest_dir = '/home/ubuntu/data/zipcode/' + zipcode + '/fethed_on_' + '_'.join(now_date.split('-'))
                dest_file = zipcode + ".xml"
                self.save_xml_file(text, dest_dir, dest_file)
                TruliaDataFetcher.parse_get_zipcode_stats_resp(text, self.db_mgr, self.hbase_mgr)
                self.db_mgr.simple_insert_query(self.db_mgr.conn, "data_zipcode", "(" + zipcode + ", '" + latest_rx_date + "', NOW())")


    def save_xml_file(self, text, dest_dir, file_name):

        try:
            os.makedirs(dest_dir)
        except OSError:
            pass # already exists, ignore
            
        with open(dest_dir + "/" + file_name, 'w') as stream:
            stream.write(str(text))


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

    @staticmethod
    def parse_get_state_stats_resp(text, db_mgr, hbase_mgr, send_to_hdfs = True):
       
        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('listingStat')
        state_code = head_dom.getElementsByTagName('stateCode')[0].firstChild.nodeValue

        # No key, do not log
        if len(state_code) != 2:
            return

        res = db_mgr.simple_select_query(db_mgr.conn, "info_state", "latitude, longitude", "state_code = '" + state_code + "' LIMIT 1")
        lat = res[0][0]
        lon = res[0][1]

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

                    if (TruliaDataFetcher.is_str_positive_int(k_bed)):
                        try:
                            state_dict = {}
                            state_dict['state_code'] = str(state_code)
                            state_dict['week_ending_date'] = str(week_ending_date)
                            state_dict['num_beds'] = int(k_bed)
                            
                            # carefully parsing of sub xml dom
                            listing_stat_dict = TruliaDataFetcher.parse_listing_stat(k_bed_i)
                        
                            # merge keys
                            state_dict = dict(state_dict.items() + listing_stat_dict.items())
                            if send_to_hdfs is True:
                                event.Event('state.all_list_stats', state_dict)

                            # hbase aggregation
                            val = str({'a': state_dict['avg_list'], 'n' : state_dict['num_list'] })
                            col_name = 'cf:' + week_ending_date
                            if k_bed not in dom_accum:
                                dom_accum[k_bed] = {}
                            dom_accum[k_bed][col_name] = val
                        except:
                            continue
    

        # HBase bulk insert
        for key in dom_accum:
            hbase_mgr.city_stats_table.put(hbase_base_key + '-' + key, dom_accum[key])
            hbase_mgr.city_stats_table.put(hbase_base_key + '-' + key, {'i:sc':str(state_code), 'i:lat:':str(lat),'i:lon':str(lon)})
                        

    @staticmethod
    def parse_get_city_stats_resp(text, db_mgr, hbase_mgr, send_to_hdfs = True):
       
        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('listingStat')
        state_code = head_dom.getElementsByTagName('state')[0].firstChild.nodeValue
        city = head_dom.getElementsByTagName('city')[0].firstChild.nodeValue

        print city, state_code
        # No key, do not log
        if len(state_code) != 2 and len(city) > 0:
            return

        res = db_mgr.simple_select_query(db_mgr.conn, "info_city", "latitude, longitude", "state_code = '" + state_code + "' and city = '" + city + "' LIMIT 1")
        lat = res[0][0]
        lon = res[0][1]

        # for batching to hbase
        dom_accum = {}
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

                    if (TruliaDataFetcher.is_str_positive_int(k_bed)):
                        try:
                            city_dict = {}
                            city_dict['state_code'] = str(state_code)
                            city_dict['city'] = str(city)
                            city_dict['week_ending_date'] = str(week_ending_date)
                            city_dict['num_beds'] = int(k_bed)
                        
                            # carefully parsing of sub xml dom
                            listing_stat_dict = TruliaDataFetcher.parse_listing_stat(k_bed_i)
                        
                            # merge keys
                            city_dict = dict(city_dict.items() + listing_stat_dict.items())
                            if send_to_hdfs is True:
                                event.Event('city.all_list_stats', city_dict)
                        
                            # hbase aggregation
                            val = str({'a': city_dict['avg_list'], 'n' : city_dict['num_list'] })
                            col_name = 'cf:' + week_ending_date
                            if k_bed not in dom_accum:
                                dom_accum[k_bed] = {}
                            dom_accum[k_bed][col_name] = val
                        except:
                            continue

        for key in dom_accum:
            hbase_mgr.city_stats_table.put(hbase_base_key + '-' + key, dom_accum[key])
            hbase_mgr.city_stats_table.put(hbase_base_key + '-' + key, {'i:city': str(city), 'i:sc':str(state_code), 'i:lat:':str(lat),'i:lon':str(lon)})


    @staticmethod
    def parse_get_zipcode_stats_resp(text, db_mgr, hbase_mgr, send_to_hdfs = True):
       
        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('listingStat')

        try:
            state_code = head_dom.getElementsByTagName('state')[0].firstChild.nodeValue
            zipcode = head_dom.getElementsByTagName('zipCode')[0].firstChild.nodeValue
        except:
            return

        print zipcode, state_code

        # No key, do not log
        if len(state_code) != 2 and len(zipcode) > 0:
            return
        
        res = db_mgr.simple_select_query(db_mgr.conn, "info_zipcode", "latitude, longitude", "state_code = '" + state_code + "' and zipcode = '" + zipcode + "' LIMIT 1")
        lat = res[0][0]
        lon = res[0][1]

        # HBase bulk insert
        dom_accum = {}
        hbase_base_key = str(zipcode)

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
                            zipcode_dict = {}
                            zipcode_dict['state_code'] = str(state_code)
                            zipcode_dict['zipcode'] = str(zipcode)
                            zipcode_dict['week_ending_date'] = str(week_ending_date)
                            zipcode_dict['num_beds'] = int(k_bed)
                            
                            # carefully parsing of sub xml dom
                            listing_stat_dict = TruliaDataFetcher.parse_listing_stat(k_bed_i)
                        
                            # merge keys
                            zipcode_dict = dict(zipcode_dict.items() + listing_stat_dict.items())
                            if send_to_hdfs is True:
                                event.Event('zipcode.all_list_stats', zipcode_dict)

                            # hbase aggregation
                            val = str({'a': zipcode_dict['avg_list'], 'n' : zipcode_dict['num_list'] })
                            col_name = 'cf:' + week_ending_date
                            if k_bed not in dom_accum:
                                dom_accum[k_bed] = {}
                            dom_accum[k_bed][col_name] = val
                        except:
                            continue

        for key in dom_accum:
            hbase_mgr.zipcode_stats_table.put(hbase_base_key + '-' + key, dom_accum[key])
            hbase_mgr.zipcode_stats_table.put(hbase_base_key + '-' + key, {'i:zc': str(zipcode), 'i:sc':str(state_code), 'i:lat:':str(lat),'i:lon':str(lon)})



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

    '''
    tdf.fetch_all_states_data()
    tdf.fetch_all_cities_all_states_data()
    tdf.fetch_all_zipcodes_data()
    '''

    tdf.fetch_all_states_data_threaded()

    #Debugging section
    '''
    tdf.fetch_city('Alpaugh','CA')
    
    fh = open("/home/ubuntu/test/Apple_Valley.xml")
    text = fh.read()
    fh.close()
    TruliaDataFetcher.parse_get_city_stats_resp(text)
    '''
