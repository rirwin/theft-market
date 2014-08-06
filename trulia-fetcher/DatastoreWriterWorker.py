from RedisQueue import RedisQueue
import sys

class DatastoreWriterWorker():

    def __init__(self, in_queue_namespace):
        
        self.in_queue_namespace = in_queue_namespace

        self.in_queue = RedisQueue(in_queue_namespace)


    def run(self):

        while 1:
            json_doc = self.in_queue.get()
            
            if json_doc == "None":
                break

            print "DatastoreWriterWorker got", json_doc
            print
            print "Write to KV store, Fluentd, and MySQL"
            print
            print
       
def main(argv):
    
    # in_queue_namespace
    in_q_ns = argv[0]

    dw = DatastoreWriterWorker(in_q_ns)
    dw.run()


# program
if __name__ == "__main__":
    main(sys.argv[1:])

