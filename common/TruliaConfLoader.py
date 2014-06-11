import ConfigParser

class TruliaConfLoader:
    def __init__(self, config_path): 
        config = ConfigParser.ConfigParser()
        config.read(config_path + "/theft-market.conf")
        self.url = config.get("trulia", "url")
        self.api_key = config.get("trulia", "api-key")
        self.location_library = config.get("trulia","location-library")
        self.stats_library = config.get("trulia","stats-library")


# unit-test
if __name__ == "__main__":
    import pprint
    trulia_conf_loader = TruliaConfLoader('../conf/')
    pprint.pprint(vars(trulia_conf_loader))
