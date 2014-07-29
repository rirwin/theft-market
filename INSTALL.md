Installation/Configuration of Theft Market
============

**Documentation is a work in progress**


Scripted Install - To be developed


Manual Install

1.  Get AWS node and CDH5 cluster setup 
    Platform tested on 1 large (for master) and 3 medium that have magnetic spinning disks.
    We used ubuntu 12.04 images.  I've tried to include the docs on how to get it running 
    on CentOS or AWS Linux AMI, but it's not fully tested.


2. OS Packages
 
  a.  Ubuntu

      $ sudo apt-get update
      $ sudo apt-get install git python-dev python-pip mysql-client mysql-server apache2 libapache2-mod-wsgi rubygems emacs python-mysqldb redis-server

    Fluentd's install script provided by fluentd (if planning to use Hue wait until after Hue is initialized)
    
      $ curl -L http://toolbelt.treasuredata.com/sh/install-ubuntu-precise.sh | sh
      $ sudo /etc/init.d/td-agent status
    
    Don't worry if not running, we'll fix later

  b.  Cent-OS 
    
      $ sudo yum install python-devel python-pip
      $ sudo yum install httpd mod_wsgi		# equivalent package of apache2 and wsgi mod in centos
      $ sudo yum install rubygems.noarc    

    Fluentd install script (provided by fluentd)
    
      $ curl -L http://toolbelt.treasuredata.com/sh/install-redhat.sh | sh
      $ sudo /etc/init.d/td-agent status
    
    Don't worry if not running, we'll fix later
        

3.  Python packages
  
        $ sudo pip install flask happybase fluent-logger cython redis boto
	$ sudo pip install boto --upgrade

4.  Git this repository if not already done so

        $ git clone https://github.com/rirwin/theft-market

5.  FluentD configuration

    On CDH5 Hue takes port 8888, which conflicts with td-agent's default configuration.
    Check to make sure no service is using port 8118.  This command should return nothing
    
        $ netstat -aon | grep ":8118"
    
    If it doesn't return empty, find another number other than 8118 to use.
   
    WebHDFS configuration
    save conf file just in case, and overwrite configuration
    
        $ sudo cp /etc/td-agent/td-agent.conf /etc/td-agent/td-agent.conf.bak
        $ sudo cp theft-market/conf/fluentd/td-agent.conf /etc/td-agent/td-agent.conf
    
    Then change the ip addresses to match the internal address of the master node
    
        $ sudo emacs /etc/td-agent/td-agent.conf
        
        Change all lines:
        host ip-172-31-15-74.us-west-1.compute.internal -> your internal address

    Restart td-agent and ensure it is running
    
        $ sudo /etc/init.d/td-agent restart
        $ sudo /etc/init.d/td-agent status
        
    Hand edit and add to hdfs-site.xml file
    
        $ sudo emacs /etc/hadoop/conf/hdfs-site.xml
    
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

    An example is found in 'theft-market/hdfs/hdfs-site.xml', but do not copy the whole file because IP addresses are different (among other things). 
    
    Restart cluster in Cloudera Manager.  This will take about 10 minutes.  Get a coffee!

    Create the data directory in HDFS:
    
         $ sudo su hdfs
         $ hdfs dfs -mkdir /data
         $ hdfs dfs -chmod +w /data
         $ exit
         
5.  MySQL config
 
    Configure access to MySQL and match this to the file in theft-market/conf/theft-metastore.conf
    Here I re-use the configuration for the vagrant development box

         $ echo "CREATE DATABASE theft" | mysql -u root -p
         $ echo "GRANT ALL ON theft.* TO vagrant@localhost IDENTIFIED BY 'pass'" | mysql -u root -p

6.  Flask/Apache configuration

    Apache server:
    Copy the file from conf/apache2/default to /etc/apache2/sites-available/default
    Edit the WSGIScriptAlias file to point to where ever your wsgi file is located.
    Make sure that the symbolic link from /etc/apache2/sites-enabled/<some_file> points to the above file

    In the file theft-market/server/wsgi/server.wsgi edit the paths to
    point to the server directory and theft-market home directory
    accordingly.

    Restart Apache
    
      $ sudo service apache2 restart


