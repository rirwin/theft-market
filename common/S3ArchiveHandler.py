import ConfigParser
import boto
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


    def get_info_sql(self):
        filename = self.info_archive_filename
        key = self.bucket.get_key(filename, validate=False)
        fp = open(self.local_data_dir + '/' + filename,'w')
        key.get_file(fp)


    def get_xml_archive(self):
        
        # TODO list keys and parse datetime on
        # for weekly operation
        # [key.split('_')[-1] for key in keys]
        filename = self.xml_data_archive_filename
        key = self.bucket.get_key(filename, validate=False)
        fp = open(self.local_data_dir + '/' + filename,'w')
        key.get_file(fp)


def main():

    s3ah = S3ArchiveHandler('../conf/')
    s3ah.get_info_sql()
    s3ah.get_xml_archive()


if '__main__' == __name__:
    main()
