import happybase
import datetime


# collection of exception handlers and logging wrappers
import wrappers

class HBaseManager:

    def __init__(self):
        self.init_hbase()

    #@wrappers.general_function_handler
    def init_hbase(self):
        self.conn = happybase.Connection('localhost')
        self.state_stats_table = self.conn.table('state_stats')
        self.city_stats_table = self.conn.table('city_stats')
        self.county_stats_table = self.conn.table('county_stats')
        self.zipcode_stats_table = self.conn.table('zipcode_stats')

    @wrappers.general_function_handler
    def create_data_tables(self):
        # parameters for all hbase tables
        # TODO put this in configuration
        families = {'cf':dict(max_versions=1)}
        self.conn.create_table('state_stats')
        self.conn.create_table('city_stats')
        self.conn.create_table('county_stats')
        self.conn.create_table('zipcode_stats')
        

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



def main():

    # TODO add config
    hm = HBaseManager()
    hm.create_data_tables()

if '__main__' == __name__:
    main()

