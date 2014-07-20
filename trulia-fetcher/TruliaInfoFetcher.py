import urllib2
import time
import threading
import sys
import os
from xml.dom import minidom


common_path = "../common/"
sys.path.append(common_path)
import TruliaConfLoader
import DatabaseManager
import wrappers

class TruliaInfoFetcher:
    def __init__(self, config_path):
        trulia_conf = TruliaConfLoader.TruliaConfLoader(config_path)
        self.load_trulia_params(trulia_conf)
        self.db_mgr = DatabaseManager.DatabaseManager(config_path)
        self.curr_key_idx = 0
        
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
        self.data_dir = trulia_conf.data_dir

 
    # Bootstrapping of all api calls
    def fetch_all_states(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "count(*)")
        if res[0][0] == 51:
            return # already called

        url_str = self.url + "library=" + self.location_library + "&function=getStates&apikey=" + self.apikeys[self.curr_key_idx] 
        self.curr_key_idx = (self.curr_key_idx + 1) % len(self.apikeys)
 
        resp = urllib2.urlopen(url_str)

        if resp.code == 200:
            dest_dir = self.data_dir + "/states_location_library"
            file_name = "getStates.xml"
            text = resp.read()
 
            info_state_val_str = TruliaInfoFetcher.parse_get_states_resp(text)
            self.db_mgr.simple_insert_query(self.db_mgr.conn, "info_state", info_state_val_str)
            

    def fetch_all_zipcodes_threaded(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")

        state_codes = list(res)
        state_code_idx = 0;
        t1 = time.time()
        
        while state_code_idx < len(state_codes):
           
            threads = list()
            self.val_strs = list()
            num_threads = min(len(self.apikeys), len(state_codes)-state_code_idx)
            
            for thread_idx in xrange(num_threads):
                state_code = state_codes[state_code_idx][0]
                url_str = self.url + "library=" + self.location_library + "&function=getZipCodesInState&state=" + state_code + "&apikey=" + self.apikeys[thread_idx]
                t = threading.Thread(target=self.fetch_zipcodes_in_state_threaded, args=[url_str, state_code])
                t.start()
                threads.append(t)
                state_code_idx+=1

            for t in threads:
                t.join()

            for val_str in self.val_strs:
                self.db_mgr.simple_insert_query(self.db_mgr.conn, "info_zipcode", val_str)  

            # make sure we don't use the same API within 2 seconds
            t2 = time.time()
            if t2 - t1 < 2.0:
                time.sleep(2.0 - (t2 - t1))
            t1 = t2

    def fetch_zipcodes_in_state_threaded(self, url_str, state_code):
        
        print 'fetch_zipcodes threaded', state_code

        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            dest_dir = self.data_dir + "/zipcodes_location_library"
            file_name = "getZipCodesInState_state_EQ_" + state_code + "_.xml"
            text = resp.read()

            info_zipcodes_val_str = TruliaInfoFetcher.parse_get_zipcodes_in_state_resp(text, state_code)
            self.lock.acquire()
            self.val_strs.append(info_zipcodes_val_str)
            self.lock.release()
            print "zipcode info retrieved", state_code


    def fetch_all_counties_threaded(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")

        state_codes = list(res)
        state_code_idx = 0;
        t1 = time.time()
        
        while state_code_idx < len(state_codes):
           
            threads = list()
            self.val_strs = list()
            num_threads = min(len(self.apikeys), len(state_codes)-state_code_idx)
            
            for thread_idx in xrange(num_threads):
                state_code = state_codes[state_code_idx][0]
                url_str = self.url + "library=" + self.location_library + "&function=getCountiesInState&state=" + state_code + "&apikey=" + self.apikeys[thread_idx]
                t = threading.Thread(target=self.fetch_counties_in_state_threaded, args=[url_str, state_code])
                t.start()
                threads.append(t)
                state_code_idx+=1

            for t in threads:
                t.join()

            for val_str in self.val_strs:
                self.db_mgr.simple_insert_query(self.db_mgr.conn, "info_county", val_str)  

            # make sure we don't use the same API within 2 seconds
            t2 = time.time()
            if t2 - t1 < 2.0:
                time.sleep(2.0 - (t2 - t1))
            t1 = t2

    def fetch_counties_in_state_threaded(self, url_str, state_code):
        
        print 'fetch_counties threaded', state_code

        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            dest_dir = self.data_dir + "/counties_location_library"
            file_name = "getCountiesInState_state_EQ_" + state_code + "_.xml"
            text = resp.read()

            info_counties_val_str = TruliaInfoFetcher.parse_get_counties_in_state_resp(text, state_code)
            self.lock.acquire()
            self.val_strs.append(info_counties_val_str)
            self.lock.release()
            print "county info retrieved", state_code


    def fetch_all_cities_threaded(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")

        state_codes = list(res)
        state_code_idx = 0;
        t1 = time.time()
        
        while state_code_idx < len(state_codes):
           
            threads = list()
            self.val_strs = list()
            num_threads = min(len(self.apikeys), len(state_codes)-state_code_idx)
            
            for thread_idx in xrange(num_threads):
                state_code = state_codes[state_code_idx][0]
                url_str = self.url + "library=" + self.location_library + "&function=getCitiesInState&state=" + state_code + "&apikey=" + self.apikeys[thread_idx]
                t = threading.Thread(target=self.fetch_cities_in_state_threaded, args=[url_str, state_code])
                t.start()
                threads.append(t)
                state_code_idx+=1

            for t in threads:
                t.join()

            for val_str in self.val_strs:
                self.db_mgr.simple_insert_query(self.db_mgr.conn, "info_city", val_str)  

            # make sure we don't use the same API within 2 seconds
            t2 = time.time()
            if t2 - t1 < 2.0:
                time.sleep(2.0 - (t2 - t1))
            t1 = t2

    def fetch_cities_in_state_threaded(self, url_str, state_code):
        
        print 'fetch_cities threaded', state_code

        resp = urllib2.urlopen(url_str)

        if resp.code == 200:
            dest_dir = self.data_dir + "/cities_location_library"
            file_name = "getCitiesInState_state_EQ_" + state_code + "_.xml"
            text = resp.read()

            info_cities_val_str = TruliaInfoFetcher.parse_get_cities_in_state_resp(text, state_code)
            self.lock.acquire()
            self.val_strs.append(info_cities_val_str)
            self.lock.release()
            print "city info retrieved", state_code


    def save_xml_file(self, text, dest_dir, file_name):

        try:
            os.makedirs(dest_dir)
        except OSError:
            pass # already exists, ignore
            
        with open(dest_dir + "/" + file_name, 'w') as stream:
            stream.write(str(text))


    def parse_one_tag(self, text, tag):
        dom = minidom.parseString(text)
        tagged_items = dom.getElementsByTagName(tag)

        tagged_list = []
        for tagged_item in tagged_items:
            print tagged_item.firstChild.nodeValue


    # Static parsing methods
    # each are manual because we have a different schema
    # and each handles dirty data in different ways

    @staticmethod
    def parse_get_states_resp(text):
       
        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('state')
        val_str = ""
        for dom_i in dom_list:
            state_code = dom_i.getElementsByTagName('stateCode')[0].firstChild.nodeValue
            state_name = dom_i.getElementsByTagName('name')[0].firstChild.nodeValue
            latitude = dom_i.getElementsByTagName('latitude')[0].firstChild.nodeValue
            longitude = dom_i.getElementsByTagName('longitude')[0].firstChild.nodeValue
            val_str+="('" + state_name + "','" + state_code + "'," + latitude + "," + longitude + "),"

        return val_str[:-1]

    @staticmethod
    def parse_get_counties_in_state_resp(text, state_code):
       
        head_dom = minidom.parseString(text)

        dom_list = head_dom.getElementsByTagName('county')
        val_str = ""
        for dom_i in dom_list:
            county_id = dom_i.getElementsByTagName('countyId')[0].firstChild.nodeValue
            county_name = dom_i.getElementsByTagName('name')[0].firstChild.nodeValue
            latitude = dom_i.getElementsByTagName('latitude')[0].firstChild.nodeValue
            longitude = dom_i.getElementsByTagName('longitude')[0].firstChild.nodeValue
            val_str+="(" + county_id + ",\"" + county_name + "\",'" + state_code + "'," + latitude + "," + longitude + "),"

        return val_str[:-1]
            
    @staticmethod
    def parse_get_cities_in_state_resp(text, state_code):
       
        head_dom = minidom.parseString(text)

        dom_list = head_dom.getElementsByTagName('city')
        val_str = ""
        for dom_i in dom_list:
            city_name = dom_i.getElementsByTagName('name')[0].firstChild.nodeValue
            try:
                city_id = dom_i.getElementsByTagName('cityId')[0].firstChild.nodeValue
                int(city_id) # will throw exception if not integer
            except:
                city_id = str(-1) # don't really care anyway

            latitude = dom_i.getElementsByTagName('latitude')[0].firstChild.nodeValue
            longitude = dom_i.getElementsByTagName('longitude')[0].firstChild.nodeValue
            val_str+="(" + city_id + ",\"" + city_name + "\",'" + state_code + "'," + latitude + "," + longitude + "),"

        return val_str[:-1]

            
    @staticmethod
    def parse_get_zipcodes_in_state_resp(text, state_code):
       
        head_dom = minidom.parseString(text)

        dom_list = head_dom.getElementsByTagName('zipCode')
        val_str = ""
        for dom_i in dom_list:

                zipcode = dom_i.getElementsByTagName('name')[0].firstChild.nodeValue
                try: # sometimes there is no lat/lon
                    latitude = dom_i.getElementsByTagName('latitude')[0].firstChild.nodeValue
                    longitude = dom_i.getElementsByTagName('longitude')[0].firstChild.nodeValue
                except:
                    latitude = str(-1000)
                    latitude = str(-1000)

                val_str+="(" + zipcode + ",\"" + state_code + "\"," + latitude + "," + longitude + "),"


        return val_str[:-1]
            

if __name__ == "__main__":
    import pprint
    tf = TruliaInfoFetcher('../conf/')

    # This clears the theft market meta store
    DatabaseManager.main()
    
    tf.fetch_all_states()
    tf.fetch_all_counties_threaded()
    tf.fetch_all_zipcodes_threaded()
    tf.fetch_all_cities_threaded()
        
