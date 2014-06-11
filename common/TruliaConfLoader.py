import ConfigParser

class TruliaConfLoader:
    def __init__(self, config_path): 
        config = ConfigParser.ConfigParser()
        config.read(config_path + "/theft-market.conf")
        self.url = config.get("trulia", "url")
        self.api_key = config.get("trulia", "api-key")
        self.location_library = config.get("trulia","location-library")
        self.stats_library = config.get("trulia","stats-library")

        loc_func_raw = config.get("trulia","location-functions")
        self.location_functions=list(filter(None, (x.strip() for x in loc_func_raw.splitlines())))

        stat_func_raw = config.get("trulia","stats-functions")
        self.stats_functions=list(filter(None, (x.strip() for x in stat_func_raw.splitlines())))

# unit-test
if __name__ == "__main__":
    import pprint
    tcl = TruliaConfLoader('../conf/')
    pprint.pprint(vars(tcl))

