import ConfigParser
import boto
import time
import tarfile
import os
import sys
from boto.s3.connection import S3Connection

class S3ArchiveHandler:

    def __init__(self, config_path):
        config = ConfigParser.ConfigParser()
        config.read(config_path + "/s3-access.conf")
        s3_access = config.get("main", "access")
        s3_secret = config.get("main", "secret")
        self.info_archive_filename = config.get("main", "info-archive")
        self.local_data_dir = config.get("main", "local-data-dir")
        self.archive_bucket_dir = config.get("main", "archive-bucket-dir")

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
        keys = self.bucket.list(pattern) # this can get slow
        for key in keys:
            
            # gets date when keys are pattern_YYYY-MM-DD.file_extension
            key_date = time.strptime(key.name.split('_')[-1].split('.')[0], "%Y-%m-%d")
            if key_date > max_date:
                max_key = key
                max_date = key_date

        return max_key
            

    def get_most_recent_xml_archive_from_s3(self):
        
        key = self.get_most_recent_key_name_with_pattern(self.archive_bucket_dir + "/xml_archive")
        if key is None:
            print "No archives found in", self.archive_bucket_dir, "with pattern xml_archive"
            return
        
        # get filename of archive, keep the same name
        filename = key.name.split('/')[-1]  
        print "Getting Archive:", filename
        fp = open(self.local_data_dir + '/' + filename,'w')
        key.get_file(fp,  cb=percent_cb, num_cb=10)
        print # newline
        return filename


    def create_archive_of_dir(self, archive_name, directory):
        if not archive_name.endswith(".tar.gz"):
            print "archive name should end in tar.gz"
            return 

        current_dir = os.getcwd() # save current dir
        
        try:
            # limit actions to the data directory
            os.chdir(self.local_data_dir)
            tar = tarfile.open(archive_name, "w:gz")
            print "creating tarfile of", directory
            tar.add(directory)
            tar.close()
            archive_name_created = archive_name
        except:
            archive_name_created = None

        os.chdir(current_dir) # go back to previous dir
        return archive_name_created


    def create_data_archive(self):
        curr_date = time.strftime("%Y-%m-%d")
        archive_name = "xml_archive_" + curr_date + ".tar.gz"
        archive_name_created = self.create_archive_of_dir(archive_name, "theft-market")
        return archive_name_created


    def extract_archive_file(self, archive_name):
        if not archive_name.endswith(".tar.gz"):
            print "archive name should end in tar.gz"
            return 

        dir_ = os.getcwd() # save current dir
        
        # limit actions to the data directory
        os.chdir(self.local_data_dir)

        if archive_name not in os.listdir("."):
            print "Archive", archive_name, "not in", self.local_data_dir
            return

        try:
            tar = tarfile.open(archive_name)
            tar.extractall()
        except:
            print "Problems extracting", archive_name, "to", self.local_data_dir
        
        os.chdir(dir_) # go back to previous dir


    def send_file_to_s3(self, rel_local_dir, file_name):
        
        # restrict actions to local data directory
        file_dir = self.local_data_dir + '/' + rel_local_dir
        print "Reading local file:", file_dir + '/' + file_name
        if file_name not in os.listdir(file_dir):
            print "File", file_name, "not in", file_dir
            return

        full_path = os.path.join(file_dir, file_name)
        full_key_name = os.path.join(rel_local_dir, time.strftime("%Y-%m-%d"), file_name)
        print "Making S3 key:", full_key_name
        k = self.bucket.new_key(full_key_name)
        k.set_contents_from_filename(full_path, cb=percent_cb, num_cb=10)
        print # newline

def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()


def init_process(s3ah):
    s3ah.get_info_sql_from_s3()
    full_archive_name = s3ah.get_most_recent_xml_archive_from_s3()

    if full_archive_name is not None:
        archive_filename = full_archive_name.split('/')[-1]
        s3ah.extract_archive_file(archive_filename)    


def create_archive_and_send(s3ah):
    archive_name = s3ah.create_data_archive()
    s3ah.send_file_to_s3('' , archive_name)


def main():

    s3ah = S3ArchiveHandler('../conf/')
    #init_process(s3ah)
    #create_archive_and_send(s3ah)

    # send tsv to S3
    #s3ah.send_file_to_s3('/tsv/state/', 'avg_state_listings.tsv')
    #s3ah.send_file_to_s3('/tsv/city/', 'avg_city_listings.tsv')
    #s3ah.send_file_to_s3('/tsv/county/', 'avg_county_listings.tsv')
    #s3ah.send_file_to_s3('/tsv/zipcode/', 'avg_zipcode_listings.tsv')

if '__main__' == __name__:
    main()
