import MySQLdb as mysqldb
import sys
import ConfigParser
import json
import logging

# collection of exception handlers and logging wrappers
import wrappers

class DatabaseManager:

    @wrappers.logger
    @wrappers.general_function_handler
    def __init__(self, config_path):
        self.init_database(config_path)
        

    @wrappers.logger
    @wrappers.general_function_handler
    def init_database(self, config_path):
        config = ConfigParser.ConfigParser()
        config.read(config_path + "/theft-metastore.conf")
        logging.basicConfig(level = config.get("main", "loglevel") )
        
        # set up database
        self.dbprog = config.get("main", "database-prog")
        if self.dbprog == "mysql":
            self.conn = self.init_mysql_conn(config)
            if self.conn is None:
                logging.error("\nCannot open a connection to theft-market meta-store database. Exiting.\n")
                sys.exit(1)

        path_schema_file = config_path + "/" + config.get("main", "schema-file")
        self.schema_dict = json.load(open(path_schema_file))


    @wrappers.logger
    @wrappers.general_function_handler
    def init_mysql_conn(self, config):

        database_ = config.get("mysql", "database")
        username_ = config.get("mysql", "username")
        password_ = config.get("mysql", "password")
        host_ = config.get("mysql", "host")
        port_ = config.get("mysql", "port")
        conn = mysqldb.connect(db = database_, user = username_, passwd = password_, host = host_, port = int(port_))
        return conn


    @wrappers.logger
    @wrappers.general_function_handler
    def reset_all_tables(self):
        for table_i in self.schema_dict:
            self.reset_table(table_i)


    @wrappers.logger
    @wrappers.general_function_handler
    def reset_data_metadata_tables(self):
        for table_i in self.schema_dict:
            if table_i.startswith("data_"):
                self.reset_table(table_i)


    @wrappers.logger
    @wrappers.general_function_handler
    def reset_table(self, table_str):
        self.drop_table(self.conn, table_str)
        table_schema = self.schema_dict[table_str]

        schema_str = DatabaseManager.translate_schema_array(self.dbprog, table_schema)
        self.create_table(self.conn, table_str, schema_str)


    @wrappers.logger
    @wrappers.database_function_handler
    def drop_table(self, cursor, table_str):
        cursor.execute("drop table if exists " + table_str) 


    @wrappers.logger
    @wrappers.database_function_handler
    def create_table(self, cursor, table_str, schema_str):
        print "create table " + table_str + schema_str
        cursor.execute("create table " + table_str + schema_str) 


    @wrappers.logger
    @wrappers.database_function_handler
    def simple_select_query(self, cursor, table_str, selected_columns_str, where_cond_str=""):
        if where_cond_str is not "":
            cursor.execute("select " + selected_columns_str + " from " + table_str + " where " + where_cond_str) 
        else:
            cursor.execute("select " + selected_columns_str + " from " + table_str) 
        return cursor.fetchall()


    @wrappers.logger
    @wrappers.database_function_handler
    def simple_insert_query(self, cursor, table_str, values_str):
        cursor.execute("insert into " + table_str + " values " + values_str) 


    @wrappers.logger
    @wrappers.database_function_handler
    def get_nearby_places(self, cursor, table_str_ref, pri_key_list_ref, table_str_target, col_list, rad_mi):
        res = cursor.execute("select latitude, longitude from " + table_str_ref + " where " + ' and '.join(pri_key_list_ref))

        res = cursor.fetchall()
        if len(res) == 0:
            return None
    
        lat = str(res[0][0])
        lon = str(res[0][1])

        cols_str = ','.join(col_list)
        cursor.execute("select " + cols_str + ", ( 3959 * acos( cos( radians(" + lat + ") ) * cos(\
 radians( latitude ) ) * cos( radians( longitude ) - radians(" + lon +") ) + sin( radians(" + lat + ") ) * sin( radians( latitude ) ) ) ) AS distance from " + table_str_target + " HAVING distance < " + rad_mi + " ORDER BY distance LIMIT 0 , 10")

        return cursor.fetchall()


    @wrappers.logger
    @wrappers.database_function_handler
    def establish_timestamp_and_most_recent_week(self, cursor, table_str, most_recent_week, date_fetched, pri_key_match_list):

        cursor.execute("select count(*) from " + table_str + " where " + ' and '.join(pri_key_match_list))
        res = cursor.fetchone()[0]
        if res == 1:
            cursor.execute("select most_recent_week from " + table_str + " where " + ' and '.join(pri_key_match_list))
            curr_latest_week = cursor.fetchone()[0]
            if datetime.datetime.strptime(curr_latest_week, '%Y-%m-%d') < datetime.datetime.strptime(most_recent_week, '%Y-%m-%d'):
                cursor.execute("update " + table_str + " set timestamp = NOW(), most_recent_week = '" + most_recent_week + ", date_fetched = '" + date_fetched + "'  where " + ' and '.join(pri_key_match_list)) 
        else:
            # remove all entries
            if res > 0:
                cursor.execute("delete from " + table_str + "' where " + ' and '.join(pri_key_match_list))
            val_str = "("
            for key_i in pri_key_match_list:
                val_str += key_i.split('=')[1] + ","
            val_str += "'" + most_recent_week + "','" + date_fetched + "', NOW())"
            cursor.execute("insert into " + table_str + " values " + val_str)
        

    @staticmethod
    @wrappers.logger
    @wrappers.general_function_handler
    def translate_schema_array(dbprog, table_schema):
        schema_arr = table_schema["schema"]
        if dbprog == "mysql":
            schema_str = "("
            for col_i in schema_arr:
                if col_i[1] == "varchar":
                    schema_str +=  col_i[0] + " varchar(128), "
                else:
                    schema_str +=  col_i[0] + " " + col_i[1] + ", "
        
        schema_str += "PRIMARY KEY" + table_schema["primary_key"] + ", "
        schema_str += "UNIQUE KEY" + table_schema["unique_key"] + " )"
        
        return schema_str


    @staticmethod
    @wrappers.logger
    @wrappers.database_function_handler
    def drop_table_static(cursor, table_str):
        cursor.execute("drop table if exists " + table_str)

 
def main():
    # Full path (incl. file name) to database credentials
    config_path = "../conf/"
    dm = DatabaseManager(config_path)
    
    dm.reset_all_tables()

    # resets tables that start with "data_"
    # dm.reset_data_metadata_tables()

if '__main__' == __name__:
    main()

