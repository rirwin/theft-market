theft-market
============

Insight Data Engineering


INSTALL

Scripted Install


Manual Install

(0) Get AWS node and CDH5 cluster setup 
    Platform tested on 1 large (for master) and 3 medium that have magnetic spining disks.
    We used ubuntu 12.04 images.

(1)
    # Ubuntu
    apt-get/yum packages (apache2, mysql)
    sudo apt-get install python-dev python-pip
    sudo apt-get install mysql-client mysql-server	# remember to set master password
    sudo apt-get install apache2 libapache2-mod-wsgi
    sudo apt-get install rubygems

    # Fluentd's install script
    curl -L http://toolbelt.treasuredata.com/sh/install-ubuntu-precise.sh | sh
    sudo /etc/init.d/td-agent status
    # Don't worry if not running, we'll fix later


    # Cent-OS
    sudo yum install python-devel python-pip
    sudo yum install httpd mod_wsgi		# equivalent package of apache2 and wsgi mod in centos
    sudo yum install rubygems.noarc    

    # Fluentd install script
    curl -L http://toolbelt.treasuredata.com/sh/install-redhat.sh | sh
    sudo /etc/init.d/td-agent status
    # Don't worry if not running, we'll fix later
        

(2) pip packages (flask)
    sudo pip install flask
    sudo pip install happybase


(3) FluentD configuration

    # On CDH5 Hue takes port 8888, which conflicts with td-agent's default configuration.
    # Check to make sure no service is using port 8118.  This command should return nothing
    netstat -aon | grep ":8118"
    # If it doesn't return empty, find another number other than 8118 to use.
   
    # WebHDFS configuration
    # save conf file just in case, and overwrite configuration
    sudo cp /etc/td-agent/td-agent.conf /etc/td-agent/td-agent.conf.bak
    sudo cp theft-market/conf/fluentd/td-agent.conf /etc/td-agent/td-agent.conf
    
    # Restart td-agent
    sudo /etc/init.d/td-agent restart
        
    # This file enabled WebHDFS and needs the cluster a webhdfs plugin
    #sudo gem install fluent-plugin-webhdfs   # I think this doesn't work?? Necessary or not??

    # Hand edit and add to hdfs-site.xml file
    sudo vi /etc/hadoop/conf/hdfs-site.xml 
    Add webhdfs property tags:
    <property> <!--added for fluentd webhdfs -->
      <name>dfs.webhdfs.enabled</name>
      <value>true</value>
    </property>
    <property> <!--added for fluentd webhdfs -->
      <name>dfs.support.append</name>
      <value>true</value>
    </property>
    <property> <!--added for fluentd webhdfs -->
      <name>dfs.support.broken.append</name>
      <value>true</value>
    </property>

    An example is found in 'theft-market/hdfs/hdfs-site.xml', but do not copy the whole file

    Restart cluster in Cloudera Manager.  This will take about 10 minutes.


(4) MySQL config


(5) Flask/apache configuration


OPERATION

(1) get data in zip file, unzip
(2) start "info" crawler to get US metadata
(3) start fluentd
(4) start thrift server
(5) run script to read in unzipped data
(6) run mapreduce job to put in nice format
(7) Hive script to setup external tables

Step (5) enables web api
Step (6) enables hive queries


