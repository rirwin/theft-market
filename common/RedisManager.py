import redis
import datetime

# collection of exception handlers and logging wrappers
import wrappers

# redis keys are stored
# <namespace>|<k_bedrooms>|<geographic_label>| ...
# <average of listings:a or number of listings:n>|<date: YYYY_MM_DD>
# namespace for state is ST, city: CY, county CO, zipcode ZC

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

        keys = self.conn.keys('|'.join([namespace, str(num_bedrooms), geo_label,'n']) + '*')

        start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        
        filtered_keys = []
        for k in keys:
            attrs = k.split('|')
            key_datetime = datetime.datetime(attrs[-1],"%Y-%m-%d")
            if key_datetime > start_datetime and key_datetime < end_datetime:
                filtered_keys.append(k)

        sum_ = 0
        for key in filtered_keys:
            sum_ += int(self.conn.get(key))

        return sum_

        
    @wrappers.general_function_handler
    def get_list_average(self, namespace, geo_label, num_bedrooms, start_date, end_date):
        keys = self.conn.keys('|'.join([namespace, str(num_bedrooms), geo_label,'a']) + '*')     

        start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        
        filtered_keys = []
        for k in keys:
            attrs = k.split('|')
            key_datetime = datetime.datetime(attrs[-1],"%Y-%m-%d")
            if key_datetime > start_datetime and key_datetime < end_datetime:
                filtered_keys.append(k)

        num = 0
        denom = 0
        for a_key in filtered_keys:
            n_key = '|'.join(a_key.split('|')[:-2]) + '|n|' + a_key.split('|')[-1]
            n = int(self.conn.get(n_key))
            a = int(self.conn.get(a_key))
            num += n*a
            denom += n

        if denom == 0:
            return 0
        else:
            return num/demon


    @wrappers.general_function_handler
    def set_listing(self, namespace, geo_label, num_bedrooms, list_datetime, list_average, list_volume):
        try:
            int(num_bedrooms)
            float(list_average) # int or float is fine
            int(list_volume)
        except:
            print 'At least one input was malformed input', 
            return

        a_key = '|'.join([namespace, str(num_bedrooms), geo_label,'a',list_datetime])
        n_key = '|'.join([namespace, str(num_bedrooms), geo_label,'n',list_datetime])
        #print a_key, list_average
        self.conn.set(a_key, list_average)
        self.conn.set(n_key, list_volume)


def main():

    # TODO add config
    rm = RedisManager()


if '__main__' == __name__:
    main()

