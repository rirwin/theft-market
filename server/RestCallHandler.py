import json
import datetime
### Functions for handling queries

def handle_data_city_volume_query(hbase_mgr, params):
    
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

    data = get_data(hbase_mgr, params_dict)
    
    #return json.dumps(data)
    return str(data)


def get_data(hbase_mgr, params_dict):
    if params_dict["_query_type"] == "volume":
        if params_dict["_subject_type"] == "city":
            return hbase_mgr.get_city_list_volume(params_dict['state_code'], params_dict['city'], params_dict['num_bedrooms'], params_dict['start_date'], params_dict['end_date'])
        


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
    

