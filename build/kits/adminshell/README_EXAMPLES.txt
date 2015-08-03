Adminshell Example Scripts


The scripts located in the example directory are here to assist in
scripting tasks to support various activities. 

**************
 Life Cycle Management
**************

- DeployAndVerifyVDB.groovy  	:  used to deploy a VDB to a server and verifies its status
- DeployJar.groovy 				:  used to deploy a jar (i.e., JDBC driver, UDF jar, etc.) as a module to the server.
	Note: the name of the jar will be the name of the module to reference.
- SetConnectionTypeForVDB		:  used to control access to VDB based depending on its connection available.  The states are:

NONE		- disallow new connections.  
BY_VERSION	- the default setting. Allow connections only if the version is specified or if this is the earliest BY_VERSION vdb and there are no vdbs marked as ANY.
ANY			- allow connections with or without a version specified. 	


**************
 Monitoring
**************

- CacheStats.groovy			: useful to examine the current cache stats to see if caching is getting used as often as it should.


**************
 VDB configuration
**************

- AssignDatasourceToModel.groovy	: provides the ability to swap an assigned data source in a VDB that is currently deployed.  


**************
 Other Useful Scripts
**************

- PrintAllVDBAndDatasources.groovy	:  prints all deployed VDB's and their assigned data sources. This provides another option, other than the adminconsole, for viewing what is currently deployed to the server.
- PrintVDBInfo.groovy		: prints the details for a given VDB, includes permissions/roles defined
- RestartServer.groovy		: provides the ability to trigger the restarting of a server
