Swagger uses the "webserice" resource adapter for connecting with source, take look at the webservice 
resource adapter for configuration.  

If the swagger translator isn't configured, install the swagger translator by running the CLI script that located in docs/teiid/datasources/swagger/add-swagger-translator.cli

To run:

-  cd ${server_install}/bin
-  ./jboss-cli.sh --connect --file=../docs/teiid/datasources/swagger/add-swagger-translator.cli


