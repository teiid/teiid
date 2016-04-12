The translator is not added by default to the server configuration.  It will need to be
configured in order to access a remote Infinispan Cache using hotrod.

use jboss-cli script to execute:  add-ispn-hotrod-translator.cli

example:  JBOSS_HOME/bin/jboss-cli.sh -c --file=add-ispn-hotrod-translator.cli
