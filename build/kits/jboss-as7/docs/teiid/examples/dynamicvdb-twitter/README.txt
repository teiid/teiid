Demonstrates how to use the WS Translator to call web services. This example also shows 
how to define view tables in dynamic vdbs.

See https://dev.twitter.com/docs/api for information on the Twitter's SOAP/REST services.  

Copy the following files to the <jboss.home>/standalone/deployments directory.
	- twitter-vdb.xml

Edit the "standalone-teiid.xml" file in the "<jboss.home>/standalone/configuration" add "weather-ds.xml" file's 
contents in resource adapter subsystem. This connection also can be created using JBoss CLI as
./jboss-cli.sh (to do this first you need to start the JBoss AS)

connect
/subsystem=resource-adapters/resource-adapter=twitterDS:add(archive=teiid-connector-ws.rar, transaction-support=NoTransaction)
/subsystem=resource-adapters/resource-adapter=twitterDS/connection-definitions=twitterDS:add(jndi-name=java:/twitterDS, class-name=org.teiid.resource.adapter.ws.WSManagedConnectionFactory, enabled=true, use-java-context=true)
/subsystem=resource-adapters/resource-adapter=twitterDS/connection-definitions=twitterDS/config-properties=EndPoint:add(value=http://search.twitter.com/search.json)
/subsystem=resource-adapters/resource-adapter=twitterDS:activate 

Start the JBoss AS

Mkae sure the "twitter" vdb is deployed in "active" state.

Use the simple client (located one directory above) example run script, e.g. 

$./run.sh localhost 31000 twitter "select * from tweet where query= 'jboss'"

NOTE - depending on your OS/Shell the quoting/escaping required to run the example can be
complicated.  It would be better to install a Java client, such as SQuirreL, to run the 
queries below.  

