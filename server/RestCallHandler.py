import json

### Functions for handling queries

def handle_data_city_volume_query(hbase_mgr, params):
    
    try:
        params_dict = eval(params)
    except:
        return "400 - malformed parameters dictionary", 400

    valid, msg = check_params_dict_keys(params_dict, ['state_code','city','num_bedrooms','start_date','end_date'])
    
    if valid is False:
        return msg, 400

    

    return params + " are your parameters"


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
    

