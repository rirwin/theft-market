from flask import Flask, request
import sys

class WebServer:
    def __init__(self, trulia_home_path, kv_store = 'r'):
        self.common_path = trulia_home_path + "/common/"
        self.config_path = trulia_home_path + "/conf/"
        self.debug = False

        sys.path.append(self.common_path)

        import RestCallHandler

        if kv_store == 'h':
            import HBaseManager
            self.kv_store_mgr = HBaseManager.HBaseManager()
        elif kv_store == 'r':         
            import RedisManager
            self.kv_store_mgr = RedisManager.RedisManager()

        # RDBMS Manager
        import DatabaseManager
        self.database_manager = DatabaseManager.DatabaseManager(self.config_path)

        self.app = Flask (__name__)

        @self.app.route('/data/state/volume', methods = ['GET'])
        def data_state_volume():
            # get data state parameters to the right of ?q= as string from flask.request
            params = request.args.get('q', None)
            return RestCallHandler.handle_data_state_volume_query(self.kv_store_mgr, self.database_manager, params)

        @self.app.route('/data/state/average', methods = ['GET'])
        def data_state_average():
            # get data state parameters to the right of ?q= as string from flask.request
            params = request.args.get('q', None)
            return RestCallHandler.handle_data_state_average_query(self.kv_store_mgr, self.database_manager, params)

        @self.app.route('/data/county/volume', methods = ['GET'])
        def data_county_volume():
            # get data county parameters to the right of ?q= as string from flask.request
            params = request.args.get('q', None)
            return RestCallHandler.handle_data_county_volume_query(self.kv_store_mgr, self.database_manager, params)

        @self.app.route('/data/county/average', methods = ['GET'])
        def data_county_average():
            # get data county parameters to the right of ?q= as string from flask.request
            params = request.args.get('q', None)
            return RestCallHandler.handle_data_county_average_query(self.kv_store_mgr, self.database_manager, params)
        
        @self.app.route('/data/city/volume', methods = ['GET'])
        def data_city_volume():
            # get data city parameters to the right of ?q= as string from flask.request
            params = request.args.get('q', None)
            return RestCallHandler.handle_data_city_volume_query(self.kv_store_mgr, self.database_manager, params)

        @self.app.route('/data/city/average', methods = ['GET'])
        def data_city_average():
            # get data city parameters to the right of ?q= as string from flask.request
            params = request.args.get('q', None)
            return RestCallHandler.handle_data_city_average_query(self.kv_store_mgr, self.database_manager, params)

        @self.app.route('/data/zipcode/volume', methods = ['GET'])
        def data_zipcode_volume():
            # get data zipcode parameters to the right of ?q= as string from flask.request
            params = request.args.get('q', None)
            return RestCallHandler.handle_data_zipcode_volume_query(self.kv_store_mgr, self.database_manager, params)

        @self.app.route('/data/zipcode/average', methods = ['GET'])
        def data_zipcode_average():
            # get data zipcode parameters to the right of ?q= as string from flask.request
            params = request.args.get('q', None)
            return RestCallHandler.handle_data_zipcode_average_query(self.kv_store_mgr, self.database_manager, params)


if __name__ == '__main__':
    
    kv_store = 'r' # Redis
    #kv_store = 'h' # HBase

    server = WebServer('..',kv_store)
    server.app.run(debug = True, port = 5000, host = '0.0.0.0')
