There are two parts to configuring an infinispan data source, the JDG modules 
and the datasource.  You can skip the deployment of the JDG modules if its 
already done.

Step 1:  Deploying JDG Modules

	-	JDG 6.x EAP Modules Library Mode kit
	
		You can obtain JDG kit distributions on Red Hat's Customer Portal at
		https://access.redhat.com/jbossnetwork/restricted/listSoftware.html


Step 2:  Configuring the data source (CLI or Edit configuration)		

	Option 1)	Run CLI Script (recommended and server must be started and Step 1 above completed)
	
		-       edit the create-infinspan-lib-mode-ds.cli, substituting the ${..} variables with your specific settings.  See create-infinispan-lib-mode-ds.properties for examples.
        -       execute script:         JBOSS_HOME/bin/jboss-cli.sh -c --file=create-infinspan-lib-mode-ds.cli   		


	Option 2)	Run CLI Script, using properties file to drive configuration

NOTE:  this first requires editing the JBOSS_HOME/bin/jboss-cli.xml file and changing the setting, <resolve-parameter-values>false</resolve-parameter-values>, to true  
	
		-	execute script:  JBOSS_HOME/bin/jboss-cli.sh -c --file=create-infinspan-lib-mode-ds.cli   --properties=create-infinispan-lib-mode-ds.properties


	Option 3) 	Manually Edit configuration file	
	
		-	Edit the standalone or domain configuration file
		-	Copy the contents from one of the infinispan-lib-mode-*-ds.xml files
			
			
