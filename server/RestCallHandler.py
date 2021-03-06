import json
import datetime
import time
import logging
import ConfigParser

config_path  = "..//conf/"
config = ConfigParser.ConfigParser()
config.read(config_path + "theft-metastore.conf")
logging.basicConfig(filename = config.get("main", "datastore-access-time-log-path"), level = config.get("main", "loglevel"))

### Functions for handling queries

## Wrappers for handling queries

class city_query_handler(object):
    def __init__(self, func):
        self.func = func
    def __get__(self, obj, type=None):
        return self.__class__(self.func.__get__(obj, type))
    def __call__(self, *args, **kw):

        try:
            params_dict = eval(args[2])
        except:
            return "400 - malformed parameters dictionary", 400
    
        valid, msg = check_params_dict_keys(params_dict, ['state_code','city','num_bedrooms','start_date','end_date'])

        if valid is False:
            return msg, 400

        # checks for alpha characters (and allows space or %20 for city names) 
        # TODO make function  and allow hyphens, periods, and other valid characters
        # not sure if this is a problem
        if params_dict['state_code'].isalpha() is False or is_valid_city(''.join(params_dict['city'].split(' '))) is False:
            return "non alphabet characters for city and/or state " + params_dict['city'] + ", " + params_dict['state_code'], 400

        try:
            int(params_dict['num_bedrooms'])
        except:
            return "400 - Malformed number of bedrooms, integer please"
    
        if len(params_dict['state_code']) != 2:
            return "please use state_code as 'CA'", 400

        params_dict['state_code'] = params_dict['state_code'].lower()

        if is_valid_date(params_dict['start_date']) is False:
            return "400 - malformed start_date parameter (YYYY-MM-DD needed)"

        if is_valid_date(params_dict['end_date']) is False:
            return "400 - malformed end_date parameter (YYYY-MM-DD needed)"
    
        if datetime.datetime.strptime(params_dict['start_date'], '%Y-%m-%d') > datetime.datetime.strptime(params_dict['end_date'], '%Y-%m-%d'):
            return "400 - start_date after than end_date"

        
        retval = self.func(args[0], args[1], params_dict, **kw)

        return retval

class county_query_handler(object):
    def __init__(self, func):
        self.func = func
    def __get__(self, obj, type=None):
        return self.__class__(self.func.__get__(obj, type))
    def __call__(self, *args, **kw):

        try:
            params_dict = eval(args[2])
        except:
            return "400 - malformed parameters dictionary", 400
    
        valid, msg = check_params_dict_keys(params_dict, ['state_code','county','num_bedrooms','start_date','end_date'])

        if valid is False:
            return msg, 400

        # checks for alpha characters (and allows space or %20 for county names) 
        # TODO make function  and allow hyphens, periods, and other valid characters
        # not sure if this is a problem
        if params_dict['state_code'].isalpha() is False or is_valid_city(''.join(params_dict['county'].split(' '))) is False:
            return "non alphabet characters for county and/or state " + params_dict['county'] + ", " + params_dict['state_code'], 400

        try:
            int(params_dict['num_bedrooms'])
        except:
            return "400 - Malformed number of bedrooms, integer please"
    
        if len(params_dict['state_code']) != 2:
            return "please use state_code as 'CA'", 400

        params_dict['state_code'] = params_dict['state_code'].lower()

        if is_valid_date(params_dict['start_date']) is False:
            return "400 - malformed start_date parameter (YYYY-MM-DD needed)"

        if is_valid_date(params_dict['end_date']) is False:
            return "400 - malformed end_date parameter (YYYY-MM-DD needed)"
    
        if datetime.datetime.strptime(params_dict['start_date'], '%Y-%m-%d') > datetime.datetime.strptime(params_dict['end_date'], '%Y-%m-%d'):
            return "400 - start_date after than end_date"

        
        retval = self.func(args[0], args[1], params_dict, **kw)

        return retval


class state_query_handler(object):
    def __init__(self, func):
        self.func = func
    def __get__(self, obj, type=None):
        return self.__class__(self.func.__get__(obj, type))
    def __call__(self, *args, **kw):

        try:
            params_dict = eval(args[2])
        except:
            return "400 - malformed parameters dictionary", 400
    
        valid, msg = check_params_dict_keys(params_dict, ['state_code','num_bedrooms','start_date','end_date'])

        if valid is False:
            return msg, 400

        # checks for alpha characters (and allows space or %20 for city names) 
        # TODO make function  and allow hyphens, periods, and other valid characters
        # not sure if this is a problem
        if params_dict['state_code'].isalpha() is False:
            return "non alphabet characters for state " + params_dict['state_code'], 400

        try:
            int(params_dict['num_bedrooms'])
        except:
            return "400 - Malformed number of bedrooms, integer please"
    
        if len(params_dict['state_code']) != 2:
            return "please use state_code as 'CA'", 400

        params_dict['state_code'] = params_dict['state_code'].lower()

        if is_valid_date(params_dict['start_date']) is False:
            return "400 - malformed start_date parameter (YYYY-MM-DD needed)"

        if is_valid_date(params_dict['end_date']) is False:
            return "400 - malformed end_date parameter (YYYY-MM-DD needed)"
    
        if datetime.datetime.strptime(params_dict['start_date'], '%Y-%m-%d') > datetime.datetime.strptime(params_dict['end_date'], '%Y-%m-%d'):
            return "400 - start_date after than end_date"
        
        retval = self.func(args[0], args[1], params_dict, **kw)

        return retval


class zipcode_query_handler(object):
    def __init__(self, func):
        self.func = func
    def __get__(self, obj, type=None):
        return self.__class__(self.func.__get__(obj, type))
    def __call__(self, *args, **kw):

        try:
            params_dict = eval(args[2])
        except:
            return "400 - malformed parameters dictionary", 400
    
        valid, msg = check_params_dict_keys(params_dict, ['zipcode','num_bedrooms','start_date','end_date'])

        if valid is False:
            return msg, 400

        if len(params_dict['zipcode']) != 5:
            return "400 - Zipcode should be 5 characters long", 400

        try:
            int(params_dict['zipcode'])
        except:
            return "400 - Zipcode should be an integer (in quotes)", 400

        try:
            int(params_dict['num_bedrooms'])
        except:
            return "400 - Malformed number of bedrooms, integer please"

        if is_valid_date(params_dict['start_date']) is False:
            return "400 - malformed start_date parameter (YYYY-MM-DD needed)"

        if is_valid_date(params_dict['end_date']) is False:
            return "400 - malformed end_date parameter (YYYY-MM-DD needed)"
    
        if datetime.datetime.strptime(params_dict['start_date'], '%Y-%m-%d') > datetime.datetime.strptime(params_dict['end_date'], '%Y-%m-%d'):
            return "400 - start_date after than end_date"

        retval = self.func(args[0], args[1], params_dict, **kw)

        return retval


@city_query_handler
def handle_data_city_volume_query(kv_store_mgr, db_mgr, params_dict):

    # add a few params to params_dict
    params_dict["_subject_type"] = "city"
    params_dict["_query_type"] = "volume"

    json_data = get_data(kv_store_mgr, db_mgr, params_dict)

    return json.dumps(json_data)

        
@city_query_handler
def handle_data_city_average_query(kv_store_mgr, db_mgr, params_dict):

    # add a few params to params_dict
    params_dict["_subject_type"] = "city"
    params_dict["_query_type"] = "average"

    json_data = get_data(kv_store_mgr, db_mgr, params_dict)

    return json.dumps(json_data)


@county_query_handler
def handle_data_county_volume_query(kv_store_mgr, db_mgr, params_dict):

    # add a few params to params_dict
    params_dict["_subject_type"] = "county"
    params_dict["_query_type"] = "volume"

    json_data = get_data(kv_store_mgr, db_mgr, params_dict)

    return json.dumps(json_data)


@county_query_handler
def handle_data_county_average_query(kv_store_mgr, db_mgr, params_dict):

    # add a few params to params_dict
    params_dict["_subject_type"] = "county"
    params_dict["_query_type"] = "average"

    json_data = get_data(kv_store_mgr, db_mgr, params_dict)

    return json.dumps(json_data)


@zipcode_query_handler
def handle_data_zipcode_volume_query(kv_store_mgr, db_mgr, params_dict):

    # add a few params to params_dict
    params_dict["_subject_type"] = "zipcode"
    params_dict["_query_type"] = "volume"

    json_data = get_data(kv_store_mgr, db_mgr, params_dict)

    return json.dumps(json_data)


@zipcode_query_handler
def handle_data_zipcode_average_query(kv_store_mgr, db_mgr, params_dict):

    # add a few params to params_dict
    params_dict["_subject_type"] = "zipcode"
    params_dict["_query_type"] = "average"

    json_data = get_data(kv_store_mgr, db_mgr, params_dict)

    return json.dumps(json_data)


@state_query_handler
def handle_data_state_volume_query(kv_store_mgr, db_mgr, params_dict):

    # add a few params to params_dict
    params_dict["_subject_type"] = "state"
    params_dict["_query_type"] = "volume"

    json_data = get_data(kv_store_mgr, db_mgr, params_dict)

    return json.dumps(json_data)


@state_query_handler
def handle_data_state_average_query(kv_store_mgr, db_mgr, params_dict):

    # add a few params to params_dict
    params_dict["_subject_type"] = "state"
    params_dict["_query_type"] = "average"

    json_data = get_data(kv_store_mgr, db_mgr, params_dict)

    return json.dumps(json_data)


def is_valid_city(city):
    for ch in city:
        if not ch.isalpha() and ch not in ['.','-','\'']:
            return False
    return True
        

# TODO refactor using geo_type and geo_label conventions from input side
def get_data(kv_store_mgr, db_mgr, params_dict):

    start_date = params_dict['start_date']
    end_date = params_dict['end_date']
    num_bedrooms = params_dict['num_bedrooms']
    
    get_data_start = time.time()

    if params_dict["_subject_type"] == "city":

        pri_key_list_ref = ["state_code ='" + params_dict['state_code'] + "'", "city = '" + params_dict['city'] + "'"]
        col_list = ["state_code", "city"]
        rad_mi = '25'
        target_table = "info_city"
        ref_table = "info_city"
        nearby_cities = db_mgr.get_nearby_places(db_mgr.conn, ref_table, pri_key_list_ref, target_table, col_list, rad_mi)
        get_city_and_nearby_cities_complete = time.time()

        if nearby_cities is None:
            return {"query result":"No result found","city": params_dict['city'], "state_code":params_dict['state_code']}
        pri_key_list_ref = ["state_code ='" + params_dict['state_code'] + "'", "city = '" + params_dict['city'] + "'"]
        col_list = ["zipcode"]
        rad_mi = '25'
        target_table = "info_zipcode"
        ref_table = "info_city"
        nearby_zipcodes = db_mgr.get_nearby_places(db_mgr.conn, ref_table, pri_key_list_ref, target_table, col_list, rad_mi) 
        get_nearby_zipcodes_complete = time.time()

        # pad zipcodes with 0 (MySQL schema has zipcodes as ints)       
        nearby_zipcodes = [str(100000 + int(x[0]))[1:] for x in nearby_zipcodes]

        if params_dict["_query_type"] == "volume":

            json_resp = []
            for nearby_city in nearby_cities:
                json_resp.append(get_city_list_volume_dict(kv_store_mgr, nearby_city[0], nearby_city[1], num_bedrooms, start_date, end_date))

            for nearby_zipcode in nearby_zipcodes:
                json_resp.append(get_zipcode_list_volume_dict(kv_store_mgr, nearby_zipcode, num_bedrooms, start_date, end_date))
            
            return json_resp

        elif params_dict["_query_type"] == "average":

            json_resp = []
            for nearby_city in nearby_cities:
                json_resp.append(get_city_list_average_dict(kv_store_mgr, nearby_city[0], nearby_city[1], num_bedrooms, start_date, end_date))

            get_city_list_average_dict_completed = time.time()

            for nearby_zipcode in nearby_zipcodes:
                json_resp.append(get_zipcode_list_average_dict(kv_store_mgr, nearby_zipcode, num_bedrooms, start_date, end_date))
            
            get_zipcode_list_average_dict_completed = time.time()

            # TODO print to [info] instead of [error]
            
            logging.info("------------- Start Data Warehouse Access -----------------")
            logging.info("get nearby cities from MySQL (GPS calculations) took: " + str(get_city_and_nearby_cities_complete - get_data_start) + "s")
            logging.info("get nearby zipcodes from MySQL (GPS calculations) took: " + str(get_nearby_zipcodes_complete - get_city_and_nearby_cities_complete) + "s")
            logging.info("get city average listings from KV Store (10 city lookup and manipulation) took: " + str(get_city_list_average_dict_completed - get_nearby_zipcodes_complete) + "s")
            logging.info("get zipcode average listings from KV Store (10 zipcode lookup and manipulation) took: " + str(get_zipcode_list_average_dict_completed - get_city_list_average_dict_completed) + "s")
            logging.info("Total time took: " +  str(time.time() - get_data_start) + "s")
            logging.info("------------- Done with Data Warehouse Access -----------------")
            
            return json_resp


    elif params_dict["_subject_type"] == "county":

        pri_key_list_ref = ["state_code ='" + params_dict['state_code'] + "'", "county = '" + params_dict['county'] + "'"]
        col_list = ["state_code", "county"]
        rad_mi = '100'
        target_table = "info_county"
        ref_table = "info_county"
        nearby_counties = db_mgr.get_nearby_places(db_mgr.conn, ref_table, pri_key_list_ref, target_table, col_list, rad_mi)

        if nearby_counties is None:
            return {"query result":"No result found","county": params_dict['county'], "state_code":params_dict['state_code']}
        
        if params_dict["_query_type"] == "volume":

            json_resp = []
            for nearby_county in nearby_counties:
                json_resp.append(get_county_list_volume_dict(kv_store_mgr, nearby_county[0], nearby_county[1], num_bedrooms, start_date, end_date))

            return json_resp

        elif params_dict["_query_type"] == "average":

            json_resp = []
            for nearby_county in nearby_counties:
                json_resp.append(get_county_list_average_dict(kv_store_mgr, nearby_county[0], nearby_county[1], num_bedrooms, start_date, end_date))

            return json_resp


    elif params_dict["_subject_type"] == "zipcode":

        pri_key_list_ref = ["zipcode = " + params_dict['zipcode']]
        col_list = ["zipcode"]
        rad_mi = '25'
        target_table = "info_zipcode"
        ref_table = "info_zipcode"
        nearby_zipcodes = db_mgr.get_nearby_places(db_mgr.conn, ref_table, pri_key_list_ref, target_table, col_list, rad_mi)

        print nearby_zipcodes
    
        if nearby_zipcodes is None:
            return {"query result":"No result found","zipcode": params_dict['zipcode']}

        # pad zipcodes with 0 (MySQL schema has zipcodes as ints)
        nearby_zipcodes = [str(100000 + int(x[0]))[1:] for x in nearby_zipcodes]
        
        if params_dict["_query_type"] == "volume":

            json_resp = []
            for nearby_zipcode in nearby_zipcodes:
                json_resp.append(get_zipcode_list_volume_dict(kv_store_mgr, nearby_zipcode, num_bedrooms, start_date, end_date))
            
            return json_resp

        elif params_dict["_query_type"] == "average":

            json_resp = []
            for nearby_zipcode in nearby_zipcodes:
                json_resp.append(get_zipcode_list_average_dict(kv_store_mgr, nearby_zipcode, num_bedrooms, start_date, end_date))
            
            return json_resp

    elif params_dict["_subject_type"] == "state":
        
        ref_table = "info_state"
        target_table = "info_state"
        pri_key_list_ref = ["state_code = '" + params_dict['state_code'] + "'"]
        rad_mi = '1000'
        col_list = ["state_code"]
        nearby_states = db_mgr.get_nearby_places(db_mgr.conn, ref_table, pri_key_list_ref, target_table, col_list, rad_mi)
        nearby_states = [x[0] for x in nearby_states] # ignore x[1] which is distance away
        if nearby_states is None:
            return {"query result":"No result found","state": params_dict['state_code']}

        if params_dict["_query_type"] == "volume":

            json_resp = []
            for state in nearby_states:
                json_resp.append(get_state_list_volume_dict(kv_store_mgr, state, num_bedrooms, start_date, end_date))
            
            return json_resp

        elif params_dict["_query_type"] == "average":

            json_resp = []
            for state in nearby_states:
                json_resp.append(get_state_list_average_dict(kv_store_mgr, state, num_bedrooms, start_date, end_date))
            
            return json_resp
        
    elif params_dict["_subject_type"] == "county":
        
        ref_table = "info_county"
        target_table = "info_county"
        pri_key_list_ref = ["state_code = '" + params_dict['state_code'] + "'","county = '" + params_dict['county'] + "'"]
        rad_mi = '100'
        col_list = ["state_code","county"]
        nearby_counties = db_mgr.get_nearby_places(db_mgr.conn, ref_table, pri_key_list_ref, target_table, col_list, rad_mi)
        nearby_counties = [x[0] for x in nearby_counties] # ignore x[1] which is distance away
        if nearby_states is None:
            return {"query result":"No result found","state": params_dict['state_code'],"county": params_dict['county']}

        if params_dict["_query_type"] == "volume":

            json_resp = []
            for county in nearby_counties:
                json_resp.append(get_county_list_volume_dict(kv_store_mgr, state, county, num_bedrooms, start_date, end_date))
            
            return json_resp

        elif params_dict["_query_type"] == "average":

            json_resp = []
            for county in nearby_counties:
                json_resp.append(get_county_list_average_dict(kv_store_mgr, state, county, num_bedrooms, start_date, end_date))
            
            return json_resp

        
def get_state_list_volume_dict(kv_store_mgr, state_code, num_bedrooms, start_date, end_date):

    lv_dict = {}
    geo_label = state_code.lower() 
    geo_type = 'ST'
    list_volume = kv_store_mgr.get_list_volume(geo_type, geo_label, num_bedrooms, start_date, end_date)

    lv_dict['list_volume'] = list_volume
    lv_dict['num_bedrooms'] = num_bedrooms
    lv_dict['start_date'] = start_date
    lv_dict['end_date'] = end_date
    lv_dict['state_code'] = state_code
    
    return lv_dict


def get_state_list_average_dict(kv_store_mgr, state_code, num_bedrooms, start_date, end_date):
    la_dict = {}
    geo_label = state_code.lower()
    geo_type = 'ST'
    list_average = kv_store_mgr.get_list_average(geo_type, geo_label, num_bedrooms, start_date, end_date)

    la_dict['list_average'] = list_average
    la_dict['num_bedrooms'] = num_bedrooms
    la_dict['start_date'] = start_date
    la_dict['end_date'] = end_date
    la_dict['state_code'] = state_code
    
    return la_dict


def get_city_list_volume_dict(kv_store_mgr, state_code, city, num_bedrooms, start_date, end_date):
    lv_dict = {}
    geo_label = state_code.lower() + '-' + '_'.join(city.split(' ')).lower()
    geo_type = 'CI'
    list_volume = kv_store_mgr.get_list_volume(geo_type, geo_label, num_bedrooms, start_date, end_date)

    lv_dict['list_volume'] = list_volume
    lv_dict['num_bedrooms'] = num_bedrooms
    lv_dict['start_date'] = start_date
    lv_dict['end_date'] = end_date
    lv_dict['city'] = city
    lv_dict['state_code'] = state_code
    
    return lv_dict


def get_city_list_average_dict(kv_store_mgr, state_code, city, num_bedrooms, start_date, end_date):
    la_dict = {}
    geo_label = state_code.lower() + '-' + '_'.join(city.split(' ')).lower()
    geo_type = 'CI'
    list_average = kv_store_mgr.get_list_average(geo_type, geo_label, num_bedrooms, start_date, end_date)

    la_dict['list_average'] = list_average
    la_dict['num_bedrooms'] = num_bedrooms
    la_dict['start_date'] = start_date
    la_dict['end_date'] = end_date
    la_dict['city'] = city
    la_dict['state_code'] = state_code
    
    return la_dict


def get_county_list_volume_dict(kv_store_mgr, state_code, county, num_bedrooms, start_date, end_date):
    lv_dict = {}
    geo_label = state_code.lower() + '-' + '_'.join(county.split(' ')).lower()
    geo_type = 'CO'
    list_volume = kv_store_mgr.get_list_volume(geo_type, geo_label, num_bedrooms, start_date, end_date)

    lv_dict['list_volume'] = list_volume
    lv_dict['num_bedrooms'] = num_bedrooms
    lv_dict['start_date'] = start_date
    lv_dict['end_date'] = end_date
    lv_dict['county'] = county
    lv_dict['state_code'] = state_code
    
    return lv_dict


def get_county_list_average_dict(kv_store_mgr, state_code, county, num_bedrooms, start_date, end_date):
    la_dict = {}
    geo_label = state_code.lower() + '-' + '_'.join(county.split(' ')).lower()
    geo_type = 'CO'
    list_average = kv_store_mgr.get_list_average(geo_type, geo_label, num_bedrooms, start_date, end_date)

    la_dict['list_average'] = list_average
    la_dict['num_bedrooms'] = num_bedrooms
    la_dict['start_date'] = start_date
    la_dict['end_date'] = end_date
    la_dict['county'] = county
    la_dict['state_code'] = state_code
    
    return la_dict


def get_zipcode_list_volume_dict(kv_store_mgr, zipcode, num_bedrooms, start_date, end_date):
    lv_dict = {}
    geo_type = 'ZP'
    list_volume = kv_store_mgr.get_list_volume(geo_type, zipcode, num_bedrooms, start_date, end_date)

    lv_dict['list_volume'] = list_volume
    lv_dict['num_bedrooms'] = num_bedrooms
    lv_dict['start_date'] = start_date
    lv_dict['end_date'] = end_date
    lv_dict['zipcode'] = zipcode
    
    return lv_dict


def get_zipcode_list_average_dict(kv_store_mgr, zipcode, num_bedrooms, start_date, end_date):
    la_dict = {}
    geo_type = 'ZP'
    list_average = kv_store_mgr.get_list_average(geo_type, zipcode, num_bedrooms, start_date, end_date)

    la_dict['list_average'] = list_average
    la_dict['num_bedrooms'] = num_bedrooms
    la_dict['start_date'] = start_date
    la_dict['end_date'] = end_date
    la_dict['zipcode'] = zipcode
    
    return la_dict


def is_valid_date(date):

    try:
        datetime.datetime.strptime(date, '%Y-%m-%d')
    except:
            return False

    return True


def check_params_dict_keys(params_dict, args):
    for arg in args:
        if arg not in params_dict:
            return False, "missing " + str(arg) + " in parameters dict"

    return True, "OK"


def is_ascii(param):
    try:
        param.decode('ascii')
    except:
        return False
    return True
    

