node default {

  ## Defaults

  # Always run apt-get update before trying to install packages
  Package {
    require => Exec["aptitude_update"],
  }

  $db_name = "theft"
  $db_password = "pass"

  include "theft_node"
}

class theft_node {

  include "aptitude"
  include "emacs"
  include "mysql"
}
