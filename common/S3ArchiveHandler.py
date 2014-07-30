import ConfigParser
import boto
#import datetime
import time
from boto.s3.connection import S3Connection

class S3ArchiveHandler:

    def __init__(self, config_path):
        config = ConfigParser.ConfigParser()
        config.read(config_path + "/s3-access.conf")
        s3_access = config.get("main", "access")
        s3_secret = config.get("main", "secret")
        self.info_archive_filename = config.get("main", "info-archive")
        self.xml_data_archive_filename = config.get("main", "xml-data-archive")
        self.local_data_dir = config.get("main", "local-data-dir")
        self.conn = S3Connection(s3_access, s3_secret)
        self.bucket = self.conn.get_bucket('theft-market')


    def get_info_sql_from_s3(self):
        filename = self.info_archive_filename
        key = self.bucket.get_key(filename, validate=False)
        print "Getting sql statements to recreate info tables"
        fp = open(self.local_data_dir + '/' + filename,'w')
        key.get_file(fp)


    def get_most_recent_key_name_with_pattern(self, pattern):

        max_date = time.strptime("2000-01-01","%Y-%m-%d")        
        max_key = None
        keys = self.bucket.list(pattern)
        for key in keys:
            
            # gets date when keys are text_text_YYYY-MM-DD.file_extension
            key_date = time.strptime(key.name.split('_')[-1].split('.')[0], "%Y-%m-%d")
            if key_date > max_date:
                max_key = key
                max_date = key_date

        return max_key
            

    def get_most_recent_xml_archive_from_s3(self):
        
        key = self.get_most_recent_key_name_with_pattern("xml_archive_")
        filename = key.name # keeps the same name
        print "Getting Archive:", filename
        fp = open(self.local_data_dir + '/' + filename,'w')
        key.get_file(fp)


    def send_xml_archive_to_s3(self, archive_full_path):
        print archive_full_path



def main():

    s3ah = S3ArchiveHandler('../conf/')
    s3ah.get_info_sql_from_s3()
    s3ah.get_most_recent_xml_archive_from_s3()


if '__main__' == __name__:
    main()
