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

class TruliaDataFetcher:
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
    def fetch_all_states_data(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            print state_code

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


# unit-test
if __name__ == "__main__":

    tdf = TruliaDataFetcher('../conf/')
    #pprint.pprint(vars(tf)) # prints all contents
    #pprint.pprint(vars(tf.trulia_conf)) # prints all contents of tcl
    

    # must run database_manager.DatabaseManager (main) to reset tables
    
    tdf.fetch_all_states_data()

    
    ''' Debugging dirty data
    fh = open("/home/vagrant/data/theft-market/zipcodes_location_library/getZipCodesInState_state_EQ_AL_.xml")
    text = fh.read()
    fh.close()
    state_code = 'AL'
    info_zipcodes_val_str = TruliaFetcher.parse_get_zipcodes_in_state_resp(text, state_code)
    '''
