import redis
import datetime

# collection of exception handlers and logging wrappers
import wrappers

# redis keys are stored
# <namespace>|<k_bedrooms>|<geographic_label>
# namespace for state is ST, city: CI, county CO, zipcode ZP
#
# The value is a map with keys of <date: YYYY_MM_DD>
# the value is {'a':avg_list_price, 'n':num_listings}

# geo label for state is just the state_code (XX)
# geo label for a city is state_code-city_label_with_spaces
# geo label for zipcode is just number XXXXX
# geo label for a county is the same as city with county label

# having the namespace with geo labels prevents county and city names from colliding

class RedisManager:

    def __init__(self):
        self.init_redis_conn()


    @wrappers.general_function_handler
    def init_redis_conn(self):
        self.conn = redis.StrictRedis(host='localhost',port=6379, db=0)


    @wrappers.general_function_handler
    def get_list_volume(self, namespace, geo_label, num_bedrooms, start_date, end_date):

        row_key = '|'.join([namespace, str(num_bedrooms), geo_label])
        print row_key
        data = self.conn.hgetall(row_key)

        start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        
        filtered_keys = [k for k in data.keys() if datetime.datetime.strptime(k,"%Y-%m-%d") < end_datetime and datetime.datetime.strptime(k,"%Y-%m-%d") > start_datetime]

        sum_ = 0
        for key in filtered_keys:
            sum_ += int(eval(data[key])['n'])

        return sum_

        
    @wrappers.general_function_handler
    def get_list_average(self, namespace, geo_label, num_bedrooms, start_date, end_date):

        row_key = '|'.join([namespace, str(num_bedrooms), geo_label])
        data = self.conn.hgetall(row_key)

        start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        
        filtered_keys = [k for k in data.keys() if datetime.datetime.strptime(k,"%Y-%m-%d") < end_datetime and datetime.datetime.strptime(k,"%Y-%m-%d") > start_datetime]

        num = 0
        denom = 0

        for key in filtered_keys:
            num += int(eval(data[key])['a'])*int(eval(data[key])['n'])
            denom += int(eval(data[key])['n'])
        if denom == 0:
            return 0
        else:
            return round(float(num)/float(denom))


    @wrappers.general_function_handler
    def insert_json_doc_records(self, json_doc):
        if json_doc['doc_type'] == 'state_record':
            geo_label = json_doc['state_code']
            namespace = 'ST'
        elif json_doc['doc_type'] == 'county_record':
            geo_label = json_doc['state_code'] + '-' + '_'.join(json_doc['county'].lower().split(' '))
            namespace = 'CO'
        elif json_doc['doc_type'] == 'zipcode_record':
            geo_label = json_doc['zipcode']
            namespace = 'ZP'
        elif json_doc['doc_type'] == 'city_record':
            geo_label = json_doc['state_code'] + '-' + '_'.join(json_doc['city'].lower().split(' '))
            namespace = 'CI'

        geo_label = geo_label.lower()

        for bed_i in json_doc['stats']:
            row_dict = {}
            for week_i in json_doc['stats'][bed_i]:
                rec = json_doc['stats'][bed_i][week_i]
                row_dict[week_i] = "{'a':" + str(rec['a']) + ",'n':" + str(rec['n']) + "}"
                
            row_key = namespace + '|' + str(bed_i) + '|' + geo_label
            self.conn.hmset(row_key, row_dict)


def main():

    # TODO add config of connection items
    rm = RedisManager()


if '__main__' == __name__:
    main()

