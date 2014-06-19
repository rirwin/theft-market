import ConfigParser

class TruliaConfLoader:
    def __init__(self, config_path): 
        config = ConfigParser.ConfigParser()
        # effectively turns on case-sensitivity for sections
        config.optionxform = str 
        config.read(config_path + "/theft-market.conf")
        self.load_base_config(config)
        self.load_functions_config(config)
        self.load_stats_functions_params(config)
        self.load_location_functions_params(config)
        self.load_utils(config)

        dbconf = ConfigParser.ConfigParser()
        dbconf.read(config_path + "/theft-metastore.conf")
        self.load_database_conf(dbconf)


    def load_utils(self, config):
        self.kafka_dir = config.get("kafka","srcDir")
        self.kafka_host = config.get("kafka","host")
        self.kafka_port = config.get("kafka","port")
        self.zookeeper_host = config.get("zookeeper","host")
        self.zookeeper_port = config.get("zookeeper","port")
        self.fluent_dir = config.get("fluentd","fluentDir")


    def load_database_conf(self, config):
        dbprog = config.get("main", "database-prog")
        if dbprog == 'mysql':
            self.database = config.get("mysql", "database")
            self.username = config.get("mysql", "username")
            self.password = config.get("mysql", "password")
            self.host = config.get("mysql", "host")
            self.port = config.get("mysql", "port")
        

    def load_base_config(self, config):
        self.url = config.get("trulia", "url")
        self.apikey = config.get("trulia", "apikey")
        self.location_library = config.get("trulia","locationLibrary")
        self.stats_library = config.get("trulia","statsLibrary")
        self.data_dir = config.get("trulia","dataDir")


    def load_functions_config(self, config):
        loc_func_raw = config.get("trulia","locationFunctions")
        self.location_functions=list(filter(None, (x.strip() for x in loc_func_raw.splitlines())))

        stat_func_raw = config.get("trulia","statsFunctions")
        self.stats_functions=list(filter(None, (x.strip() for x in stat_func_raw.splitlines())))


    def load_stats_functions_params(self, config):
        self.stats_functions_params = {}
        sfp = self.stats_functions_params
        for stat_func in self.stats_functions:
            sfp_key = stat_func + 'Params'
            params_raw = config.get("trulia",sfp_key)
            sfp[sfp_key] = list(filter(None, (x.strip() for x in params_raw.splitlines())))


    def load_location_functions_params(self, config):
        self.location_functions_params = {}
        lfp = self.location_functions_params
        for loc_func in self.location_functions:
            lfp_key = loc_func + 'Params'
            params_raw = config.get("trulia",lfp_key)
            lfp[lfp_key] = list(filter(None, (x.strip() for x in params_raw.splitlines())))


# unit-test
if __name__ == "__main__":
    import pprint
    tcl = TruliaConfLoader('../conf/')
    pprint.pprint(vars(tcl)) # prints all contents

    
