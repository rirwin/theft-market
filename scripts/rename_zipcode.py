import shutil
import os
import glob

base_path = ("/Users/rirwin/data/theft-market/zipcode/")
files = glob.glob("/Users/rirwin/data/theft-market/zipcode/*/*/*xml")
for file_ in files:
    if file_.endswith(".xml"):
        #print file_
        filename = file_.split('/')[-1]
        date_str_old = file_.split('/')[-2]
        zipcode = file_.split('/')[-3]
        date_str_new = date_str_old[10:].replace('_','-')
        print file_
        os.mkdir(base_path + zipcode + '/' + date_str_new + '/')
        shutil.move(file_,base_path + zipcode + '/' + date_str_new + '/' + zipcode + ".xml")
        
