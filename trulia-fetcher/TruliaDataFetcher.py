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


    def fetch_all_states_data(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code")
        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            self.fetch_state(state_code)
            time.sleep(2) # trulia api restriction

    def fetch_state(self, state_code):
        latest_rx_date = self.get_latest_rx_date("state",{"state_code":state_code})
        now_date = TruliaDataFetcher.get_current_date()
        print "fetching state data about", state_code,"data from", latest_rx_date, "to", now_date

        url_str = self.url + "library=" + self.stats_library + "&function=getStateStats&state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikey
        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            text = resp.read()
            TruliaDataFetcher.parse_get_state_stats_resp(text)
            self.db_mgr.simple_insert_query(self.db_mgr.conn, "data_state", "('" + state_code + "',  '" + latest_rx_week + "', NOW())")


    def fetch_all_cities_all_states_data(self):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_state", "state_code", "state_code > 'IN'")
        for state_code_tuple in list(res):
            state_code = state_code_tuple[0]
            print state_code
            self.fetch_all_cities_in_state_data(state_code)


    def fetch_all_cities_in_state_data(self, state_code):

        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_city", "city", "state_code = '" + state_code + "'")
        for city_tuple in list(res):
            city = city_tuple[0]
            self.fetch_city(city, state_code)
            time.sleep(2) # trulia api restriction 


    def fetch_city(self, city, state_code):

        latest_rx_date = self.get_latest_rx_date("city",{"state_code":state_code})
        now_date = TruliaDataFetcher.get_current_date()

        print "fetching city data about", city, state_code, "from", latest_rx_date, "to", now_date
        city_spaced = '%20'.join(city.split(' '))
        url_str = self.url + "library=" + self.stats_library + "&function=getCityStats&city=" + city_spaced + "&state=" + state_code + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikey
        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            text = resp.read()
            dest_dir = '/home/ubuntu/data/city/' + state_code + '/fethed_on_' + '_'.join(now_date.split('-'))
            dest_file = '_'.join(city.split(' ')) + ".xml"
            self.save_xml_file(text, dest_dir, dest_file)
            TruliaDataFetcher.parse_get_city_stats_resp(text)
            self.db_mgr.simple_insert_query(self.db_mgr.conn, "data_city", "('" + state_code + "', '" + city + "', '" + latest_rx_date + "', NOW())")


    def fetch_all_zipcodes_data(self): 

        # distinct needed since some zipcodes span multiple states (as reported by Trulia)
        res = self.db_mgr.simple_select_query(self.db_mgr.conn, "info_zipcode", "distinct zipcode")
        for zipcode_tuple in list(res):
            zipcode = zipcode_tuple[0]
            zipcode = str(100000 + zipcode)[1:] 
            self.fetch_zipcode_data(zipcode)
            time.sleep(2) # trulia api restriction 


    def fetch_zipcode_data(self, zipcode):

        latest_rx_date = self.get_latest_rx_date("zipcode",{"zipcode":zipcode})
        now_date = TruliaDataFetcher.get_current_date()
        print "fetching zipcode data about", zipcode, "from", latest_rx_date, "to", now_date
        url_str = self.url + "library=" + self.stats_library + "&function=getZipCodeStats&zipCode=" + zipcode + "&startDate=" + latest_rx_date + "&endDate=" + now_date + "&statType=listings" + "&apikey=" + self.apikey
        print url_str
        resp = urllib2.urlopen(url_str)
        if resp.code == 200:
            text = resp.read()
            print text
            dest_dir = '/home/ubuntu/data/zipcode/' + zipcode + '/fethed_on_' + '_'.join(now_date.split('-'))
            dest_file = zipcode + ".xml"
            self.save_xml_file(text, dest_dir, dest_file)
            TruliaDataFetcher.parse_get_zipcode_stats_resp(text)
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


    # Static parsing methods
    # each are manual because we have a different schema
    # and each handles dirty data in different ways

    @staticmethod
    def is_str_positive_int(k_bed):
        try:
            k = int(k_bed)
            if k < 0:
                return False
        except:
                return False

        return True


    @staticmethod
    def parse_get_state_stats_resp(text):
       
        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('listingStat')
        state_code = head_dom.getElementsByTagName('stateCode')[0].firstChild.nodeValue

        # No key, do not log
        if len(state_code) != 2:
            return

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
                         
                        state_dict = {}
                        state_dict['state_code'] = str(state_code)
                        state_dict['week_ending_date'] = str(week_ending_date)
                        state_dict['num_beds'] = int(k_bed)
                        
                        # carefully parsing of sub xml dom
                        listing_stat_dict = TruliaDataFetcher.parse_listing_stat(k_bed_i)
                        
                        # merge keys
                        state_dict = dict(state_dict.items() + listing_stat_dict.items())
                        event.Event('state.all_list_stats', state_dict)

    @staticmethod
    def parse_get_city_stats_resp(text):
       
        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('listingStat')
        state_code = head_dom.getElementsByTagName('state')[0].firstChild.nodeValue
        city = head_dom.getElementsByTagName('city')[0].firstChild.nodeValue

        print city, state_code
        # No key, do not log
        if len(state_code) != 2 and len(city) > 0:
            return

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
                         
                        city_dict = {}
                        city_dict['state_code'] = str(state_code)
                        city_dict['city'] = str(city)
                        city_dict['week_ending_date'] = str(week_ending_date)
                        city_dict['num_beds'] = int(k_bed)
                        
                        # carefully parsing of sub xml dom
                        listing_stat_dict = TruliaDataFetcher.parse_listing_stat(k_bed_i)
                        
                        # merge keys
                        city_dict = dict(city_dict.items() + listing_stat_dict.items())
                        event.Event('city.all_list_stats', city_dict)


    @staticmethod
    def parse_get_zipcode_stats_resp(text):
       
        head_dom = minidom.parseString(text)
        dom_list = head_dom.getElementsByTagName('listingStat')
        state_code = head_dom.getElementsByTagName('state')[0].firstChild.nodeValue
        zipcode = head_dom.getElementsByTagName('zipCode')[0].firstChild.nodeValue

        print zipcode, state_code
        # No key, do not log
        if len(state_code) != 2 and len(zipcode) > 0:
            return

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
                         
                        zipcode_dict = {}
                        zipcode_dict['state_code'] = str(state_code)
                        zipcode_dict['zipcode'] = str(zipcode)
                        zipcode_dict['week_ending_date'] = str(week_ending_date)
                        zipcode_dict['num_beds'] = int(k_bed)
                        
                        # carefully parsing of sub xml dom
                        listing_stat_dict = TruliaDataFetcher.parse_listing_stat(k_bed_i)
                        
                        # merge keys
                        zipcode_dict = dict(zipcode_dict.items() + listing_stat_dict.items())
                        event.Event('zipcode.all_list_stats', zipcode_dict)


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
    #pprint.pprint(vars(tf)) # prints all contents
    #pprint.pprint(vars(tf.trulia_conf)) # prints all contents of tcl
    
    
    #tdf.init_kafka()
    #tdf.fetch_all_states_data()
    #tdf.fetch_all_cities_in_state_data('IN')
    #tdf.fetch_all_cities_all_states_data()
    tdf.fetch_all_zipcodes_data()
    
    #Debugging section
    '''
    tdf.fetch_city('Alpaugh','CA')
    
    fh = open("/home/ubuntu/test/Apple_Valley.xml")
    text = fh.read()
    fh.close()
    TruliaDataFetcher.parse_get_city_stats_resp(text)
    '''
