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
    def reset_table(self, table_str):
        self.drop_table(self.conn, table_str)
        schema_arr = self.schema_dict[table_str]
        schema_str = DatabaseManager.translate_schema_array(self.dbprog, schema_arr)
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
        #return cursor.fetchall()[0][0]
        return cursor.fetchall()


    @wrappers.logger
    @wrappers.database_function_handler
    def simple_insert_query(self, cursor, table_str, values_str):
        cursor.execute("insert into " + table_str + " values " + values_str) 
        

    @staticmethod
    @wrappers.logger
    @wrappers.general_function_handler
    def translate_schema_array(dbprog, schema_arr):
        if dbprog == "mysql":
            schema_str = "("
            for col_i in schema_arr:
                if col_i[1] == "varchar":
                    schema_str +=  col_i[0] + " varchar(256), "
                else:
                    schema_str +=  col_i[0] + " " + col_i[1] + ","
        
        return schema_str[:-1] + ")"


    @staticmethod
    @wrappers.logger
    @wrappers.database_function_handler
    def drop_table_static(cursor, table_str):
        cursor.execute("drop table if exists " + table_str)

 
# unit-test
if '__main__' == __name__:

    # Full path (incl. file name) to database credentials
    config_path = "../conf/"

    dm = DatabaseManager(config_path)

    dm.reset_all_tables()
    print dm.simple_select_query(dm.conn, "info_state", "*")
    print dm.simple_insert_query(dm.conn, "info_state", "('New Hampshire', 42.290192, -71.853737)")
    print dm.simple_select_query(dm.conn, "info_state", "*")    


    print "Program continued to end"
