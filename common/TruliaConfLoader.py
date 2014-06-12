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

    
