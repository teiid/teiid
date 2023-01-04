Either edit the configuration or use the cli to create a hdfs datasource.  See the following examples:

* if you are in standalone mode edit the "standalone-teiid.xml" file and add 
the contents of the hdfs-ds.xml under "resource-adapters" subsystem section.

* edit the create-hdfs-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-hdfs-ds.cli   --properties=create-hdfs-ds.properties