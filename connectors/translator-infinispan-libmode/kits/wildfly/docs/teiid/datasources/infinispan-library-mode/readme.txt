The ispn-lib-mode translator is not added by default to the server configuration.  It will need to be
configured in order to access Infinispan Cache.

use jboss-cli script to execute:  add-ispn-lib-mode-translator.cli

example:  JBOSS_HOME/bin/jboss-cli.sh -c --file=add-ispn-lib-mode-translator.cli
