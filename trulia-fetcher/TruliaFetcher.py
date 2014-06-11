import ConfigParser
import urllib2
from xml.dom import minidom

class TruliaFetcher:
    def __init__(self, config_path):
        self.tcl = TruliaConfLoader(config_path)
        

    def fetch(self):
        tcl = self.tcl
        



