import threading
import DataParser # use this for now, then try using passed function

class ParserWorker(threading.Thread):

    def __init__(self, text_queue, records_queue, parsing_func):
        self.text_queue = text_queue
        self.records_queue = records_queue
        self.parsing_func = parsing_func
        threading.Thread.__init__(self)

    def run(self):
        while 1:
            text_tup = self.text_queue.get()
            if text_tup is None:
                # put back since more parsers than fetchers
                self.text_queue.put(None)
                # add end-of-queue markers for next set of threads 
                self.records_queue.put(None) 
                # ends thread
                break 

            text = text_tup[0]
            location = text_tup[1]

            fluentd_accum, hbase_accum = DataParser.parse_get_state_stats_resp(text)
            record_bundle = {"fluentd":fluentd_accum, "hbase_accum":hbase_accum}
            self.records_queue.put((record_bundle, location))
