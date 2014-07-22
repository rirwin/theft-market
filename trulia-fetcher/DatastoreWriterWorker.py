import threading
import time

class DatastoreWriterWorker(threading.Thread):

    def __init__(self, records_queue, writers, locks, match_rule, metadata_table):
        self.records_queue = records_queue
        self.writers = writers
        self.locks = locks
        self.match_rule = match_rule
        self.metadata_table = metadata_table
        threading.Thread.__init__(self)

    def run(self):
        while 1:
            record_bundle_tup = self.records_queue.get()
            if record_bundle_tup is None:
                break # reached end of queue

            record_bundle = record_bundle_tup[0]
            location = record_bundle_tup[1]

            # fluentd HDFS writing
            self.locks['fluentd'].acquire()
            for record in record_bundle["fluentd"]:
                self.writers["fluend"].Event(match_rule, record)
            self.locks['fluentd'].release()

            # HBase puts
            hbase_record_bundle = record_bundle["hbase"]
            self.locks['hbase'].acquire()
            for key in hbase_record_bundle:
                try:
                    self.writers["hbase"].put(key, hbase_record_bundle[key])
                except:
                    print "Exception while inserting", key, "into HBase function"
            self.locks['hbase'].release()

            # MySQL metadata record keeping
            val_items = []
            for location_item in location:
                val_items.append("'" + location_item + "'")
            val_items.append("'" + time.strftime("%Y-%m-%d") + "'")
            val_items.append("NOW()")
            val_str = '(' + ','.join(val_items) + ')'

            self.locks['mysql'].acquire()
            self.writers['mysql'].simple_insert_query(writers['mysql'].conn, self.metadata_table, val_str)
            self.locks['mysql'].release()
