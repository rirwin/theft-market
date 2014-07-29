import happybase
import datetime
import pprint

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
    def get_list_volume(self, geo_type, geo_label, num_bedrooms, start_date, end_date):

        if geo_type == 'ST':
            table = self.state_stats_table
        elif geo_type == 'CI':
            table = self.city_stats_table
        elif geo_type == 'CO':
            table = self.county_stats_table
        elif geo_type == 'ZP':
            table = self.county_stats_table

        row_key = '|'.join([str(num_bedrooms), geo_label])

        data =  table.row(row_key)

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
    def get_list_average(self, state_code, city, num_bedrooms, start_date, end_date):

        if geo_type == 'ST':
            table = self.state_stats_table
        elif geo_type == 'CI':
            table = self.city_stats_table
        elif geo_type == 'CO':
            table = self.county_stats_table
        elif geo_type == 'ZP':
            table = self.county_stats_table

        
        row_key = '|'.join([str(num_bedrooms), geo_label])

        data = table.row(row_key)

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
    def insert_json_doc_records(self, json_doc):

        if json_doc['doc_type'] == 'state_record':
            geo_label = json_doc['state_code']
            table = self.state_stats_table
        elif json_doc['doc_type'] == 'county_record':
            geo_label = json_doc['state_code'] + '-' + '_'.join(json_doc['county'].split(' '))
            table = self.county_stats_table
        elif json_doc['doc_type'] == 'zipcode_record':
            geo_label = json_doc['zipcode']
            table = self.zipcode_stats_table
        elif json_doc['doc_type'] == 'city_record':
            geo_label = json_doc['state_code'] + '-' + '_'.join(json_doc['city'].split(' '))
            table = self.city_stats_table

        geo_label = geo_label.lower()

        for bed_i in json_doc['stats']:
            row_dict = {}
            for week_i in json_doc['stats'][bed_i]:
                rec = json_doc['stats'][bed_i][week_i]
                row_dict['cf:' + week_i] = "'{'a':" + str(rec['a']) + ",'n':" + str(rec['n']) + "}'"
                
            row_key = str(bed_i) + '|' + geo_label
            table.put(row_key, row_dict)
            

def main():

    # TODO add config
    hm = HBaseManager()


if '__main__' == __name__:
    main()

