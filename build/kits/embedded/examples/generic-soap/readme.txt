This example demonstrates using the WS Translator to call a generic soap service. 

Before executing the run script, first we need deploy 'stateService' to JBoss Server, the 'stateService' is a JAX-WS compatible web service, which supply service for GetAllStateInfo and GetStateInfo via StateCode. Build 'stateService' via:
	cd stateService/
	mvn clean install
this will generate deployment jar 'StateService.jar'. Deploy 'StateService.jar' to a running JBoss server(Assume JBoss EAP 6 run on localhost), WSDL File can be viewed via 
	http://localhost:8080/StateService/stateService/StateServiceImpl?WSDL

Note that JBoss Server be omitted by any VDB based operation.
