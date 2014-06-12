import urllib2 # consider requests
import time
import sys
import os

common_path = "../common/"
sys.path.append(common_path)
import TruliaConfLoader

class TruliaFetcher:
    def __init__(self, config_path):
        self.trulia_conf = TruliaConfLoader.TruliaConfLoader(config_path)
        
    def fetch_states(self):
        tc = self.trulia_conf
        url_str = tc.url + "library=" + tc.location_library + "&function=getStates&apikey=" + tc.apikey 
        
        resp = urllib2.urlopen(url_str)

        if resp.code == 200:
            dest_dir = tc.data_dir + "/states_location_library"
            file_name = "getStates.xml"
            self.save_xml_file(resp.read(), dest_dir, file_name)


    def save_xml_file(self, text, dest_dir, file_name):

        try:
            os.makedirs(dest_dir)
        except OSError:
            pass # already exists, ignore
            
        with open(dest_dir + "/" + file_name, 'wb') as stream:
            stream.write(str(text))


# unit-test
if __name__ == "__main__":
    import pprint
    tf = TruliaFetcher('../conf/')
    #pprint.pprint(vars(tf)) # prints all contents
    #pprint.pprint(vars(tf.trulia_conf)) # prints all contents of tcl

    tf.fetch_states()
