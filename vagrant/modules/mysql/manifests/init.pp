class mysql {

  package {
    "mysql-server": ensure => installed;
    "mysql-client": ensure => installed;
    "python-mysqldb": ensure => installed;
    "mysql-workbench": ensure => installed;
  }

  file {
    "/usr/local/sbin/init_mysql":
      content => template("mysql/init_mysql.erb"),
      mode => 0555;
  }

  exec {
    "init_mysql":
      command => "/usr/local/sbin/init_mysql",
      require => [
        File["/usr/local/sbin/init_mysql"],
        Package["mysql-server"],
        Package["mysql-client"]
      ];
  }
}
