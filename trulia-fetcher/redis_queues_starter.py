# script for starting fetchers, parser, and writer

import os
import sys

config_path = "../conf/"
common_path = "../common/"
sys.path.append(common_path)
import TruliaConfLoader

trulia_conf = TruliaConfLoader.TruliaConfLoader(config_path)
apikeys = trulia_conf.apikeys

url_queue_namespace = "URL"
xml_queue_namespace = "XML"
json_queue_namespace = "JSN"

for fetcher_i in xrange(len(apikeys)):
    os.system("python FetcherWorker.py " + url_queue_namespace + " " + xml_queue_namespace + " " + apikeys[fetcher_i])
    break

#os.system("screen python ParserWorker.py " + xml_queue_namespace + " " + json_queue_namespace)

#os.system("python DatastoreWriterWorker.py " + json_queue_namespace)

