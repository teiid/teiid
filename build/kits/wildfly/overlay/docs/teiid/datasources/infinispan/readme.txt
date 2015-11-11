NOTE:  Need to configure the translator to use.  Run either:
infinispan-cache     -  add-infinispan-cache-translator.cli
infinispan-cache-dsl -  add-infinispan-cache-dsl-translator.cli

There are three connection strategies for an Infinispan datasource:
1. JNDI lookup - uses the CacheJndiName config property and expects an 
   Infinispan CacheContainer to be located at the given JNDI name.
2. Remote via a HotRod client - uses the HotRodClientPropertiesFile config 
   property to specify an absolute file location or the RemoteServerList 
   property to specify a semi-colon separated list of server:port combinations.
3. Directly using a local cache - by setting the 
   ConfigurationFileNameForLocalCache to point to a configuration file.

The datasource also needs access to the cache classes, which can be done by 
setting Module config property to the name of the AS module containing the cache
classes.

The CacheTypeMap config property must also be set with additional metadata
about the cache classes.  It provides a mapping from cache name to cache
class.  When using a dynamic vdb, it should also specify the pk field
name for each of the cache classes. 

To install the infinispan connector, choose 1 of the 2 following options:

1.  edit the "standalone-teiid.xml" file or "doamin-teiid.xml" file and add 
the contents of one of the infinispan-*-ds.xml under "resource-adapters" subsystem section.

2.  edit the create-ldap-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-infinspan-ds.cli   --properties=create-infinispan-ds.properties
