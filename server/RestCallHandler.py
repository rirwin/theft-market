import json
import datetime
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
        if params_dict['state_code'].isalpha() is False or ''.join(params_dict['city'].split(' ')).isalpha() is False:
            return "non alphabet characters for city and/or state" + ''.join(params_dict['city'].split(' ')), 400

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
def handle_data_city_volume_query(hbase_mgr, db_mgr, params_dict):

    # add a few params to params_dict
    params_dict["_subject_type"] = "city"
    params_dict["_query_type"] = "volume"

    json_data = get_data(hbase_mgr, db_mgr, params_dict)

    return json.dumps(json_data)


@city_query_handler
def handle_data_city_average_query(hbase_mgr, db_mgr, params_dict):

    # add a few params to params_dict
    params_dict["_subject_type"] = "city"
    params_dict["_query_type"] = "average"

    json_data = get_data(hbase_mgr, db_mgr, params_dict)

    return json.dumps(json_data)


@zipcode_query_handler
def handle_data_zipcode_volume_query(hbase_mgr, db_mgr, params_dict):

    # add a few params to params_dict
    params_dict["_subject_type"] = "zipcode"
    params_dict["_query_type"] = "volume"

    json_data = get_data(hbase_mgr, db_mgr, params_dict)

    return json.dumps(json_data)


@zipcode_query_handler
def handle_data_zipcode_average_query(hbase_mgr, db_mgr, params_dict):

    # add a few params to params_dict
    params_dict["_subject_type"] = "zipcode"
    params_dict["_query_type"] = "average"

    json_data = get_data(hbase_mgr, db_mgr, params_dict)

    return json.dumps(json_data)


def get_data(hbase_mgr, db_mgr, params_dict):

    start_date = params_dict['start_date']
    end_date = params_dict['end_date']
    num_bedrooms = params_dict['num_bedrooms']


    if params_dict["_subject_type"] == "city":
        nearby_cities = get_city_and_nearby_cities(db_mgr, params_dict['state_code'], params_dict['city'])

        if nearby_cities is None:
            return {"query result":"No result found","city": params_dict['city'], "state_code":params_dict['state_code']}
        
        if params_dict["_query_type"] == "volume":

            json_resp = []
            for nearby_city in nearby_cities:
                json_resp.append(get_city_list_volume_dict(hbase_mgr, nearby_city[0], nearby_city[1], num_bedrooms, start_date, end_date))
            
            return json_resp

        elif params_dict["_query_type"] == "average":

            json_resp = []
            for nearby_city in nearby_cities:
                json_resp.append(get_city_list_average_dict(hbase_mgr, nearby_city[0], nearby_city[1], num_bedrooms, start_date, end_date))
            
            return json_resp

    elif params_dict["_subject_type"] == "zipcode":
        
        nearby_zipcodes = get_zipcode_and_nearby_zipcodes(db_mgr, params_dict['zipcode'])
    
        # pad zipcodes with 0 (MySQL schema has zipcodes as ints)
        nearby_zipcodes = [str(100000 + int(x[0]))[1:] for x in nearby_zipcodes]
    
        if nearby_zipcodes is None:
            return {"query result":"No result found","zipcode": params_dict['zipcode']}
        
        if params_dict["_query_type"] == "volume":

            json_resp = []
            for nearby_zipcode in nearby_zipcodes:
                json_resp.append(get_zipcode_list_volume_dict(hbase_mgr, nearby_zipcode, num_bedrooms, start_date, end_date))
            
            return json_resp

        elif params_dict["_query_type"] == "average":

            json_resp = []
            for nearby_zipcode in nearby_zipcodes:
                json_resp.append(get_zipcode_list_average_dict(hbase_mgr, nearby_zipcode, num_bedrooms, start_date, end_date))
            
            return json_resp

        

def get_city_list_volume_dict(hbase_mgr, state_code, city, num_bedrooms, start_date, end_date):
    lv_dict = {}
    city_ = '_'.join(city.split(' ')).lower()
    list_volume = hbase_mgr.get_city_list_volume(state_code.lower(), city_, num_bedrooms, start_date, end_date)

    lv_dict['list_volume'] = list_volume
    lv_dict['num_bedrooms'] = num_bedrooms
    lv_dict['start_date'] = start_date
    lv_dict['end_date'] = end_date
    lv_dict['city'] = city
    lv_dict['state_code'] = state_code
    
    return lv_dict


def get_city_list_average_dict(hbase_mgr, state_code, city, num_bedrooms, start_date, end_date):
    la_dict = {}
    city_ = '_'.join(city.split(' ')).lower()
    list_volume = hbase_mgr.get_city_list_average(state_code.lower(), city_, num_bedrooms, start_date, end_date)

    la_dict['list_average'] = list_volume
    la_dict['num_bedrooms'] = num_bedrooms
    la_dict['start_date'] = start_date
    la_dict['end_date'] = end_date
    la_dict['city'] = city
    la_dict['state_code'] = state_code
    
    return la_dict


def get_zipcode_list_volume_dict(hbase_mgr, zipcode, num_bedrooms, start_date, end_date):
    lv_dict = {}

    list_volume = hbase_mgr.get_zipcode_list_volume(zipcode, num_bedrooms, start_date, end_date)

    lv_dict['list_volume'] = list_volume
    lv_dict['num_bedrooms'] = num_bedrooms
    lv_dict['start_date'] = start_date
    lv_dict['end_date'] = end_date
    lv_dict['zipcode'] = zipcode
    
    return lv_dict


def get_zipcode_list_average_dict(hbase_mgr, zipcode, num_bedrooms, start_date, end_date):
    la_dict = {}

    list_volume = hbase_mgr.get_zipcode_list_average(zipcode, num_bedrooms, start_date, end_date)

    la_dict['list_average'] = list_volume
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



