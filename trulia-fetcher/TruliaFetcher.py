import urllib2 # consider requests
import time
import sys
import os
from xml.dom import minidom


common_path = "../common/"
sys.path.append(common_path)
import TruliaConfLoader
import DatabaseManager
import wrappers

class TruliaFetcher:
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
        self.apikey = trulia_conf.apikey
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
            self.save_xml_file(text, dest_dir, file_name)
 
            info_state_val_str = TruliaFetcher.parse_get_states_resp(text)
            self.db_mgr.simple_insert_query(self.db_mgr.conn, "info_state", info_state_val_str)
            
    def fetch_all_counties(self):
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            self.fetch_counties_in_state(state_code_tuple[0])

    def fetch_counties_in_state(self, state_code):
        print "Fetch Counties in " + state_code
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_county", "count(*)", "state_code='" + state_code + "'")
        if res[0][0] > 0:
            return # already called

        url_str = self.url + "library=" + self.location_library + "&function=getCountiesInState&state=" + state_code + "&apikey=" + self.apikey

        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            dest_dir = self.data_dir + "/counties_location_library"
            file_name = "getCountiesInState_state_EQ_" + state_code + "_.xml"
            text = resp.read()
            self.save_xml_file(text, dest_dir, file_name)
            info_counties_val_str = TruliaFetcher.parse_get_counties_in_state_resp(text, state_code)
            self.db_mgr.simple_insert_query(self.db_mgr.conn, "info_county", info_counties_val_str)
            time.sleep(2)

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
            

# unit-test
if __name__ == "__main__":
    import pprint
    tf = TruliaFetcher('../conf/')
    #pprint.pprint(vars(tf)) # prints all contents
    #pprint.pprint(vars(tf.trulia_conf)) # prints all contents of tcl

    tf.fetch_all_states()
    tf.fetch_all_counties()
