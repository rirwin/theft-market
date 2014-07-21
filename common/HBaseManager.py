import happybase
import datetime
import threading

# collection of exception handlers and logging wrappers
import wrappers

class HBaseManager:

    def __init__(self):
        self.init_hbase_conn()
        self.establish_data_tables()
        self.init_table_handles()


    @wrappers.general_function_handler
    def init_hbase_conn(self):
        self.conn = happybase.Connection('localhost')


    @wrappers.general_function_handler
    def init_table_handles(self):
        self.state_stats_table = self.conn.table('state_stats')
        self.city_stats_table = self.conn.table('city_stats')
        self.county_stats_table = self.conn.table('county_stats')
        self.zipcode_stats_table = self.conn.table('zipcode_stats')


    @wrappers.general_function_handler
    def establish_data_tables(self):

        # parameters for all hbase tables
        # TODO put this schema in configuration

        families = {'cf':dict(max_versions=1)}

        # each in try-catch block because happybase api does not 
        # have establish table, and throws exception
        try:
            self.conn.create_table('state_stats',families)
        except:
            pass

        try:
            self.conn.create_table('city_stats',families)
        except:
            pass

        try:
            self.conn.create_table('county_stats',families)
        except:
            pass

        try:
            self.conn.create_table('zipcode_stats',families)
        except:
            pass
        

    @wrappers.general_function_handler
    def get_city_list_volume(self, state_code, city, num_bedrooms, start_date, end_date):
        
        row_key = '-'.join([state_code, city, str(num_bedrooms)])

        data =  self.city_stats_table.row(row_key)

        start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d')

        filtered_keys = [k for k in data.keys() if k.startswith('cf:') \
                             and datetime.datetime.strptime(k[3:],"%Y-%m-%d") < end_datetime \
                             and datetime.datetime.strptime(k[3:],"%Y-%m-%d") > start_datetime]

        sum_ = 0
        for key in filtered_keys:
            sum_ += int(eval(data[key])['n'])

        return sum_

        
    @wrappers.general_function_handler
    def get_city_list_average(self, state_code, city, num_bedrooms, start_date, end_date):
        
        row_key = '-'.join([state_code, city, str(num_bedrooms)])

        data =  self.city_stats_table.row(row_key)

        start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d')

        # Pythonic way to extract relevant columns of row
        filtered_keys = [k for k in data.keys() if k.startswith('cf:') \
                             and datetime.datetime.strptime(k[3:],"%Y-%m-%d") < end_datetime \
                             and datetime.datetime.strptime(k[3:],"%Y-%m-%d") > start_datetime]

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
    def get_zipcode_list_volume(self, zipcode, num_bedrooms, start_date, end_date):
        
        row_key = '-'.join([zipcode, str(num_bedrooms)])

        data =  self.zipcode_stats_table.row(row_key)

        start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d')

        filtered_keys = [k for k in data.keys() if k.startswith('cf:') \
                             and datetime.datetime.strptime(k[3:],"%Y-%m-%d") < end_datetime \
                             and datetime.datetime.strptime(k[3:],"%Y-%m-%d") > start_datetime]

        sum_ = 0
        for key in filtered_keys:
            sum_ += int(eval(data[key])['n'])

        return sum_

        
    @wrappers.general_function_handler
    def get_zipcode_list_average(self, zipcode, num_bedrooms, start_date, end_date):
        
        row_key = '-'.join([zipcode, str(num_bedrooms)])

        data =  self.zipcode_stats_table.row(row_key)

        start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d')

        # Pythonic way to extract relevant columns of row
        filtered_keys = [k for k in data.keys() if k.startswith('cf:') \
                             and datetime.datetime.strptime(k[3:],"%Y-%m-%d") < end_datetime \
                             and datetime.datetime.strptime(k[3:],"%Y-%m-%d") > start_datetime]

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
    def simple_put(self, table, row_key, row_dict):
        self.lock.acquire()
        if table == "state_stats":
            print "in simple_put"
            print row_key
            print row_dict
            self.state_stats_table.put(row_key, row_dict)
        self.lock.release()


def main():

    # TODO add config
    hm = HBaseManager()


if '__main__' == __name__:
    main()

