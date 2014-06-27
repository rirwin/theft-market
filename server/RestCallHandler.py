import json
import datetime
### Functions for handling queries


def handle_data_city_volume_query(hbase_mgr, db_mgr, params):
    
    try:
        params_dict = eval(params)
    except:
        return "400 - malformed parameters dictionary", 400
    
    valid, msg = check_params_dict_keys(params_dict, ['state_code','city','num_bedrooms','start_date','end_date'])

    if valid is False:
        return msg, 400

    if is_ascii(params_dict['state_code']) is False or is_ascii(params_dict['city']) is False:
        return "non ascii characters for city and/or state", 400

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

    # add a few params to params_dict
    params_dict["_subject_type"] = "city"
    params_dict["_query_type"] = "volume"

    json_data = get_data(hbase_mgr, db_mgr, params_dict)
    

    return json.dumps(json_data)


def get_data(hbase_mgr, db_mgr, params_dict):

    start_date = params_dict['start_date']
    end_date = params_dict['end_date']
    num_bedrooms = params_dict['num_bedrooms']
    
    if params_dict["_query_type"] == "volume":
        if params_dict["_subject_type"] == "city":
            
            nearby_cities = get_nearby_cities(db_mgr, params_dict['state_code'], params_dict['city'])

            json_resp = []
            for nearby_city in nearby_cities:
                json_resp.append(get_city_list_volume_dict(hbase_mgr, nearby_city[0], nearby_city[1], num_bedrooms, start_date, end_date))
            

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
    

def get_nearby_cities(dm, state_code, city):

    city_ = ' '.join(city.split('_'))
    city_res = dm.simple_select_query(dm.conn, "info_city", "latitude, longitude","city = '" + city_ + "' and state_code = '" + state_code + "'")

    lat = str(city_res[0][0])
    lon = str(city_res[0][1])

    # TODO move to configuration
    rad_mi = '25'
    limit = '10'

    nearby_cities_res = dm.simple_select_query(dm.conn, "info_city HAVING distance < " + rad_mi + " ORDER BY distance LIMIT 0 , " + limit, "state_code, city, ( 3959 * acos( cos( radians(" + lat + ") ) * cos( radians( latitude ) ) * cos( radians( longitude ) - radians(" + lon +") ) + sin( radians(" + lat + ") ) * sin( radians( latitude ) ) ) ) AS distance")

    return nearby_cities_res
