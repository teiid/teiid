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