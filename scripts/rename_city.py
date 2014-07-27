import shutil
import os
import glob

base_path = ("/Users/rirwin/data/theft-market/city/")
files = glob.glob("/Users/rirwin/data/theft-market/city/*/*/*xml")
for file_ in files:
    if file_.endswith(".xml"):
        #print file_
        filename = file_.split('/')[-1]
        date_str_old = file_.split('/')[-2]
        city_ = file_.split('/')[-1].split('.')[0]
        state = file_.split('/')[-3]
        date_str_new = date_str_old[10:].replace('_','-')
        print file_
        print '->'
        print base_path + state + '/' + city_ + '/' + date_str_new + '/' + filename
        try:
            os.mkdir(base_path + state + '/' + city_ + '/')
            os.mkdir(base_path + state + '/' + city_ + '/' + date_str_new + '/')
        except:
            pass
    
        shutil.move(file_, base_path + state + '/' + city_ + '/' + date_str_new + '/' + filename)
        
