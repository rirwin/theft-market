import urllib2
import time
import sys

from RedisQueue import RedisQueue

class FetcherWorker:

    def __init__(self, in_queue_namespace, out_queue_namespace, apikey):

        self.in_queue_namespace = in_queue_namespace
        self.out_queue_namespace = out_queue_namespace
        self.apikey = apikey

        self.in_queue = RedisQueue(in_queue_namespace)
        self.out_queue = RedisQueue(out_queue_namespace)

        print "Fetcher loaded with apikey", self.apikey


    def run(self):

        while 1:

            base_url = self.in_queue.get()

            if base_url == "None":
                # add end-of-queue markers for parsers
                self.out_queue.put("None") 

                # ends program
                break

            url = base_url + self.apikey 
            
            t1 = time.time()
            
            print "fetching try 1", url

            resp = urllib2.urlopen(url)
            if resp.code == 200: 
                text = resp.read()
                self.out_queue.put(text)
            else:
                print 'failed once', url
                time.sleep(10)
                print "fetching try 2", url
                resp = urllib2.urlopen(url)
                if resp.code == 200:
                    text = resp.read()
                    self.out_queue.put(text)

            print "done fetching"

            # make sure we don't use the same API key within 2 seconds
            t2 = time.time()
            if t2 - t1 < 2.0:
                time.sleep(2.0 - (t2 - t1))


def main(argv):
    
    # in_queue_namespace, out_queue_namespace, apikey
    in_q_ns = argv[0]
    out_q_ns = argv[1]
    apikey = argv[2]
    
    fw = FetcherWorker(in_q_ns, out_q_ns, apikey)
    fw.run()

# program
if __name__ == "__main__":
    main(sys.argv[1:])
