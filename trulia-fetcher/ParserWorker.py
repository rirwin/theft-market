import threading
import DataParser # use this for now, then try using passed function
import DatastoreWriterWorker

class ParserWorker(threading.Thread):

    def __init__(self, text_tup):
        self.text_tup = text_tup
        threading.Thread.__init__(self)

    def run(self):
        text = self.text_tup[0]
        location = self.text_tup[1]
        writers_params = self.text_tup[2]
        writers = writers_params["writers"]
        locks = writers_params["locks"]
        match_rule = writers_params["match_rule"]
        metadata_table = writers_params["metadata_table"]

        print "parsing", location
        fluentd_accum = " "
        hbase_accum = " "

        fluentd_accum, hbase_accum = DataParser.parse_get_state_stats_resp(text) 
        DatastoreWriterWorker.DatastoreWriterWorker( ( {"fluentd":fluentd_accum,"hbase":hbase_accum}, location)  , writers, locks, match_rule, metadata_table).start()
        print "done parsing", location
