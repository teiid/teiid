This distribution has all the necessary jar and many optional files needed to 
deploy Teiid in the embedded mode. Note that, Teiid embedded does not require 
JBoss AS to run. This also brings in many issues to be resolved in the host 
environment, like

- Connections to your sources
- Transaction manager/Connection pools (Narayana/IronJacamar is shown in the
  examples)
- Security

The user is responsible for providing alternative provisions for the related
features to execute properly.

Access through the remote Admin API is not possible, but the EmbeddedServer does
implement the Admin interface.

 