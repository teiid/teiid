There are two parts to configuring an infinispan data source, the JDG modules 
and the datasource.  You can skip the deployment of the JDG modules if its 
already done.

Step 1:  Deploying JDG Modules

	-	JDG 6.x EAP Modules Library Mode kit
	
		You can obtain JDG kit distributions on Red Hat's Customer Portal at
		https://access.redhat.com/jbossnetwork/restricted/listSoftware.html


Step 2:  Configuring the data source (CLI or Edit configuration)

	Option 1)	Run CLI Script (recommended and server must be started and Step 1 above completed)
	
		-	Example:  JBOSS_HOME/bin/jboss-cli.sh -c --file=create-infinspan-lib-mode-ds.cli   --properties=create-infinispan-lib-mode-ds.properties
		

	Option 2) 	Edit configuration file
	
		-	Edit the standalone or domain configuration file
		-	Copy the contents from one of the infinispan-lib-mode-*-ds.xml files
			
