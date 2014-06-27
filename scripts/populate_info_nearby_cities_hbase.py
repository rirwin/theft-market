import sys

sys.path.append("../common/")
import DatabaseManager
import HBaseManager


config_path = "../conf/"
dm = DatabaseManager.DatabaseManager(config_path)
hm = HBaseManager.HBaseManager()
sys.exit(0)
rad_mi = '25'
limit = '10'

cities = dm.simple_select_query(dm.conn, "info_city", "city, state_code, latitude, longitude")
for city in cities:
   
    city_lc = '_'.join(city[0].split(' ')).lower()
    state_code_lc = city[1].lower()
    lat = str(city[2])
    lon = str(city[3])
   
    res = dm.simple_select_query(dm.conn, "info_city HAVING distance < " + rad_mi + " ORDER BY distance LIMIT 0 , " + limit, "state_code, city, ( 3959 * acos( cos( radians(" + lat + ") ) * cos( radians( latitude ) ) * cos( radians( longitude ) - radians(" + lon + ") ) + sin( radians(" + lat + ") ) * sin( radians( latitude ) ) ) ) AS distance")
    
    state_code_lc = 'ma'
    city_lc = 'newton'

    row_key = '-'.join([state_code_lc, city_lc])

    start_scan = row_key +'-' + str(1)
    end_scan = row_key +'-' + str(99)

    print start_scan, end_scan
    for key, data in hm.city_stats_table_26june14.scan(row_start=start_scan, row_stop=end_scan, columns='i'):
        print key, data
        sys.exit(0)
    
    #for near_city in res:

    #    other_row_key_base = near_city[0].lower() + '-' + '_'.join(near_city[1].split(' ')).lower()

        #if other_row_key_base +'-' + str(1) is in hbase, make key 
        #print near_city[0], near_city[1], near_city[2]
        
        # create a second column family in hbase for stats about each city

    sys.exit(0)
