from flask import Flask, request
import sys

class WebServer:
    def __init__(self, trulia_home_path):
        self.common_path = trulia_home_path + "/common/"
        self.debug = False

        sys.path.append(self.common_path)

        import RestCallHandler

        # HBase connection
        #import HBaseManager
        self.hbase_manager = None
        
        self.app = Flask (__name__)
        
        @self.app.route('/data/city/volume', methods = ['GET'])
        def data_city_volume():
            # get data city parameters to the right of ?q= as string from flask.request
            params = request.args.get('q', None)
            return RestCallHandler.handle_data_city_volume_query(self.hbase_manager, params)

        
if __name__ == '__main__':
    
    server = WebServer('..')
    server.app.run(debug = True, port = 5000, host = '0.0.0.0')
