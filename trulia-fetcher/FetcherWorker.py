import threading
import urllib2
import time

class FetcherWorker(threading.Thread):

    def __init__(self, url_queue, text_queue, apikey):
        self.url_queue = url_queue
        self.text_queue = text_queue
        self.apikey = apikey
        threading.Thread.__init__(self)

    def run(self):
        while 1:
            url_tup = self.url_queue.get()
            if url_tup is None:
                # add end-of-queue markers for parsers
                self.text_queue.put(None) 
                # ends thread
                break
            url = url_tup[0] + self.apikey 
            location = url_tup[1]

            # try twice for now, put in while loop next
            resp = urllib2.urlopen(url_str)
            if resp.code == 200: 
                text = resp.read()
                self.text_queue.put((text, location))
            else:
                time.sleep(10)
                resp = urllib2.urlopen(url_str)
                if resp.code == 200:
                    text = resp.read()
                    self.text_queue.put((text, location))
