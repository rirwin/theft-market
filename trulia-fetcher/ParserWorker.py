import sys
import DataParser 
from RedisQueue import RedisQueue

class ParserWorker():

    def __init__(self, in_queue_namespace, out_queue_namespace):
        
        self.in_queue_namespace = in_queue_namespace
        self.out_queue_namespace = out_queue_namespace

        self.in_queue = RedisQueue(in_queue_namespace)
        self.out_queue = RedisQueue(out_queue_namespace)

        print "Parser worker loaded"

    def run(self):

        while 1:
            xml_text = self.in_queue.get()
            print "Received XML"
            if xml_text == "None":
                self.out_queue.put("None")
                break

            json_doc = DataParser.parse_get_state_stats_resp(xml_text)
            print "Made JSON"
            self.out_queue.put(json_doc)
     
       
def main(argv):
    
    # in_queue_namespace, out_queue_namespace
    in_q_ns = argv[0]
    out_q_ns = argv[1]
    
    pw = ParserWorker(in_q_ns, out_q_ns)
    pw.run()

# program
if __name__ == "__main__":
    main(sys.argv[1:])
