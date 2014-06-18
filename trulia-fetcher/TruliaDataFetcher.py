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
import time

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
        self.kafka_dir = trulia_conf.kafka_dir
        self.kafka_host = trulia_conf.kafka_host
        self.kafka_port = trulia_conf.kafka_port


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
            where_str += k + " = '" + obj_key_dict[k] + "' and "
        where_str = where_str[:-4]
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, table_str, "most_recent_week", where_str)
        if len(res) == 0:
            latest_rx_date = "2000-01-01"
        else:
            latest_rx_date = str(res[0][0])
        return latest_rx_date


    def fetch_all_states_data(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            latest_rx_date = self.get_latest_rx_date("state",{"state_code":state_code})
            now_date = TruliaDataFetcher.get_current_date()
            url_str = self.url + "library=" + self.stats_library + "&function=getStateStats&state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikey
            resp = urllib2.urlopen(url_str)
            if resp.code == 200:
                text = resp.read()
                info_state_val_str = TruliaDataFetcher.parse_get_state_stats_resp(text)

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

    # Static parsing methods
    # each are manual because we have a different schema
    # and each handles dirty data in different ways

    @staticmethod
    def parse_get_state_stats_resp(text):
       
        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('listingStat')
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
    
    
    tdf.init_kafka()
    tdf.fetch_all_states_data()

    
    ''' Debugging dirty data
    fh = open("/home/vagrant/data/theft-market/zipcodes_location_library/getZipCodesInState_state_EQ_AL_.xml")
    text = fh.read()
    fh.close()
    state_code = 'AL'
    info_zipcodes_val_str = TruliaFetcher.parse_get_zipcodes_in_state_resp(text, state_code)
    '''
