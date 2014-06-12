import urllib2 # consider requests
import time
import sys
import os
from xml.dom import minidom


common_path = "../common/"
sys.path.append(common_path)
import TruliaConfLoader

class TruliaFetcher:
    def __init__(self, config_path):
        self.trulia_conf = TruliaConfLoader.TruliaConfLoader(config_path)
        

    def fetch_states(self):
        tc = self.trulia_conf
        url_str = tc.url + "library=" + tc.location_library + "&function=getStates&apikey=" + tc.apikey 
 
        '''       
        resp = urllib2.urlopen(url_str)

        if resp.code == 200:
            dest_dir = tc.data_dir + "/states_location_library"
            file_name = "getStates.xml"
            text = resp.read()
            self.save_xml_file(text, dest_dir, file_name)
        '''
        fh = open(tc.data_dir + "/states_location_library/getStates.xml", 'r')
        self.parse_two_tags(fh.read(), "state", "name")


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


    def parse_two_tags(self, text, tag, subtag):
        head_dom = minidom.parseString(text)

        dom_list = head_dom.getElementsByTagName(tag)

        for dom_i in dom_list:
            node_list = dom_i.getElementsByTagName(subtag)
            for node in node_list:
                print node.firstChild.nodeValue

            

# unit-test
if __name__ == "__main__":
    import pprint
    tf = TruliaFetcher('../conf/')
    #pprint.pprint(vars(tf)) # prints all contents
    #pprint.pprint(vars(tf.trulia_conf)) # prints all contents of tcl

    tf.fetch_states()
