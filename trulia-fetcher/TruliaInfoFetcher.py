import urllib2
import time
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
        

    def load_trulia_params(self, trulia_conf):
        self.stats_library = trulia_conf.stats_library
        self.location_library = trulia_conf.location_library
        self.stats_functions_params = trulia_conf.stats_functions_params
        self.stats_functions = trulia_conf.stats_functions
        self.location_functions_params = trulia_conf.location_functions_params
        self.location_functions = trulia_conf.location_functions
        self.url = trulia_conf.url
        self.apikey = trulia_conf.apikeys[0] # just use the first key
        self.data_dir = trulia_conf.data_dir

 
    # Bootstrapping of all api calls
    def fetch_all_states(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "count(*)")
        if res[0][0] == 51:
            return # already called

        url_str = self.url + "library=" + self.location_library + "&function=getStates&apikey=" + self.apikey 
 
        resp = urllib2.urlopen(url_str)

        if resp.code == 200:
            dest_dir = self.data_dir + "/states_location_library"
            file_name = "getStates.xml"
            text = resp.read()
 
            info_state_val_str = TruliaInfoFetcher.parse_get_states_resp(text)
            self.db_mgr.simple_insert_query(self.db_mgr.conn, "info_state", info_state_val_str)
            
    def fetch_all_counties(self):
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            self.fetch_counties_in_state(state_code_tuple[0])


    def fetch_counties_in_state(self, state_code):
        print "Fetch counties in " + state_code
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_county", "count(*)", "state_code='" + state_code + "'")
        if res[0][0] > 0:
            print " already retrieved"
            return # already called

        url_str = self.url + "library=" + self.location_library + "&function=getCountiesInState&state=" + state_code + "&apikey=" + self.apikey

        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            dest_dir = self.data_dir + "/counties_location_library"
            file_name = "getCountiesInState_state_EQ_" + state_code + "_.xml"
            text = resp.read()

            info_counties_val_str = TruliaInfoFetcher.parse_get_counties_in_state_resp(text, state_code)
            self.db_mgr.simple_insert_query(self.db_mgr.conn, "info_county", info_counties_val_str)
            print " county info retrieved"
            time.sleep(2) # trulia api restriction


    def fetch_all_cities(self):
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            self.fetch_cities_in_state(state_code_tuple[0])


    def fetch_cities_in_state(self, state_code):
        print "Fetching cities in " + state_code
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_city", "count(*)", "state_code='" + state_code + "'")
        if res[0][0] > 0:
            print " already retrieved"
            return # already called

        url_str = self.url + "library=" + self.location_library + "&function=getCitiesInState&state=" + state_code + "&apikey=" + self.apikey

        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            dest_dir = self.data_dir + "/cities_location_library"
            file_name = "getCitiesInState_state_EQ_" + state_code + "_.xml"
            text = resp.read()

            info_cities_val_str = TruliaInfoFetcher.parse_get_cities_in_state_resp(text, state_code)
            self.db_mgr.simple_insert_query(self.db_mgr.conn, "info_city", info_cities_val_str)
            print " city info retrieved"
            time.sleep(2) # trulia api restriction


    def fetch_all_zipcodes(self):
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            self.fetch_zipcodes_in_state(state_code_tuple[0])


    def fetch_zipcodes_in_state(self, state_code):
        print "Fetching zipcodes in " + state_code
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_zipcode", "count(*)", "state_code='" + state_code + "'")
        if res[0][0] > 0:
            print " already retrieved"
            return # already called

        url_str = self.url + "library=" + self.location_library + "&function=getZipCodesInState&state=" + state_code + "&apikey=" + self.apikey

        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            dest_dir = self.data_dir + "/zipcodes_location_library"
            file_name = "getZipCodesInState_state_EQ_" + state_code + "_.xml"
            text = resp.read()

            info_zipcodes_val_str = TruliaInfoFetcher.parse_get_zipcodes_in_state_resp(text, state_code)
            self.db_mgr.simple_insert_query(self.db_mgr.conn, "info_zipcode", info_zipcodes_val_str)
            print " zipcode info retrieved"
            time.sleep(2) # trulia api restriction


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

    # must run database_manager.DatabaseManager (main) to reset DB tables
    
    tf.fetch_all_states()
    tf.fetch_all_counties()
    tf.fetch_all_cities()
    tf.fetch_all_zipcodes()
    
    ''' Debugging dirty data
    fh = open("/home/vagrant/data/theft-market/zipcodes_location_library/getZipCodesInState_state_EQ_AL_.xml")
    text = fh.read()
    fh.close()
    state_code = 'AL'
    info_zipcodes_val_str = TruliaInfoFetcher.parse_get_zipcodes_in_state_resp(text, state_code)
    '''
