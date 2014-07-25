import json
import datetime
import time
import logging
# TODO make configurable
logging.basicConfig(filename='/Users/rirwin/theft-market/server/log/datastore_access_timing.log',level=logging.INFO)

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

        params_dict['city'] = '_'.join(params_dict['city'].split(' ')).lower()
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

def is_valid_city(city):
    for ch in city:
        if not ch.isalpha() and ch not in ['.','-','\'']:
            return False
    return True
        
@city_query_handler
def handle_data_city_average_query(kv_store_mgr, db_mgr, params_dict):

    # add a few params to params_dict
    params_dict["_subject_type"] = "city"
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


def get_data(kv_store_mgr, db_mgr, params_dict):

    start_date = params_dict['start_date']
    end_date = params_dict['end_date']
    num_bedrooms = params_dict['num_bedrooms']
    
    get_data_start = time.time()

    if params_dict["_subject_type"] == "city":

        nearby_cities = get_city_and_nearby_cities(db_mgr, params_dict['state_code'], params_dict['city'])
        get_city_and_nearby_cities_complete = time.time()

        if nearby_cities is None:
            return {"query result":"No result found","city": params_dict['city'], "state_code":params_dict['state_code']}
        
        nearby_zipcodes = get_nearby_zipcodes_from_city(db_mgr, params_dict['state_code'], params_dict['city'])
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

    elif params_dict["_subject_type"] == "zipcode":
        
        nearby_zipcodes = get_zipcode_and_nearby_zipcodes(db_mgr, params_dict['zipcode'])
    
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
        
        nearby_states = [params_dict['state_code']]#get_state_and_nearby_states(db_mgr, params_dict['state'])
    
        if nearby_states is None:
            return {"query result":"No result found","state": params_dict['state']}

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
    geo_type = 'CI'
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
    geo_type = 'CI'
    list_average = kv_store_mgr.get_county_list_average(geo_type, geo_label, num_bedrooms, start_date, end_date)

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
    

def get_nearby_places(dm, table_str, pri_key_list, rad_mi):

    res = dm.simple_select_query(dm.conn, table_str, "latitude, longitude",' and '.join(pri_key_list))
    if len(res) == 0:
        return None
    
    lat = str(res[0][0])
    lon = str(res[0][1])

    # TODO move to configuration or arg
    limit = '10'

    # TODO clean this up
    nearby_places = dm.simple_select_query(dm.conn, table_str + " HAVING distance < " + rad_mi + " ORDER BY distance LIMIT 0 , " + limit, "state_code, city, ( 3959 * acos( cos( radians(" + lat + ") ) * cos( radians( latitude ) ) * cos( radians( longitude ) - radians(" + lon +") ) + sin( radians(" + lat + ") ) * sin( radians( latitude ) ) ) ) AS distance")

    return nearby_places

def get_city_and_nearby_cities(dm, state_code, city):

    city_ = ' '.join(city.split('_'))
    city_res = dm.simple_select_query(dm.conn, "info_city", "latitude, longitude","city = '" + city_ + "' and state_code = '" + state_code + "'")

    if len(city_res) == 0:
        return None

    lat = str(city_res[0][0])
    lon = str(city_res[0][1])

    # TODO move to configuration
    rad_mi = '25'
    limit = '10'

    nearby_cities_res = dm.simple_select_query(dm.conn, "info_city HAVING distance < " + rad_mi + " ORDER BY distance LIMIT 0 , " + limit, "state_code, city, ( 3959 * acos( cos( radians(" + lat + ") ) * cos( radians( latitude ) ) * cos( radians( longitude ) - radians(" + lon +") ) + sin( radians(" + lat + ") ) * sin( radians( latitude ) ) ) ) AS distance")

    return nearby_cities_res


def get_zipcode_and_nearby_zipcodes(dm, zipcode):

    zipcode_res = dm.simple_select_query(dm.conn, "info_zipcode", "latitude, longitude","zipcode = '" + zipcode + "' limit 1")

    if len(zipcode_res) == 0:
        return None

    lat = str(zipcode_res[0][0])
    lon = str(zipcode_res[0][1])

    # TODO move to configuration
    rad_mi = '25'
    limit = '10'

    nearby_zipcode_res = dm.simple_select_query(dm.conn, "info_zipcode HAVING distance < " + rad_mi + " ORDER BY distance LIMIT 0 , " + limit, "zipcode, ( 3959 * acos( cos( radians(" + lat + ") ) * cos( radians( latitude ) ) * cos( radians( longitude ) - radians(" + lon +") ) + sin( radians(" + lat + ") ) * sin( radians( latitude ) ) ) ) AS distance")

    return nearby_zipcode_res


def get_nearby_zipcodes_from_city(dm, state_code, city):

    city_ = ' '.join(city.split('_'))
    city_res = dm.simple_select_query(dm.conn, "info_city", "latitude, longitude","city = '" + city_ + "' and state_code = '" + state_code + "' limit 1")

    if len(city_res) == 0:
        return None

    lat = str(city_res[0][0])
    lon = str(city_res[0][1])

    # TODO move to configuration
    rad_mi = '25'
    limit = '10'

    nearby_zipcode_res = dm.simple_select_query(dm.conn, "info_zipcode HAVING distance < " + rad_mi + " ORDER BY distance LIMIT 0 , " + limit, "zipcode, ( 3959 * acos( cos( radians(" + lat + ") ) * cos( radians( latitude ) ) * cos( radians( longitude ) - radians(" + lon +") ) + sin( radians(" + lat + ") ) * sin( radians( latitude ) ) ) ) AS distance")

    return nearby_zipcode_res



