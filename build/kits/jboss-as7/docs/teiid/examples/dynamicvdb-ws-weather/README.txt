Demonstrates how to use the WS Translator to call web services.
See http://www.nws.noaa.gov/forecasts/xml/ for information on the National Weather 
Service's SOAP/REST services.  

Copy the following files to the <jboss.home>/server/default/deploy directory.
	- weather-vdb.xml
	- weather-ds.xml

Start the JBoss Container

Use the simple client example run script, e.g. 

$./run.sh localhost 31000 weather "some query"

NOTE - depending on your OS/Shell the quoting/escaping required to run the example can be
complicated.  It would be better to install a Java client, such as SQuirreL, to run the 
queries below.  

Example queries:

1. REST access of the default endpoint augmented by a query string.  The query string is 
formed with the querystring function, which ensures proper encoding of the 
name/value pairs.  The invoke procedure has a return parameter, called result that contains
the XML value of the result document.  This document is then fed into the XMLTABLE function
to extract row values.  Note that the default invocation binding has been set in the vdb xml
to HTTP, which is the proper setting for REST.
   
select t.* from 
	(call weather.invoke(action=>'GET', 
		endpoint=>querystring('', '38.99,-77.02 39.70,-104.80 47.6,-122.30' as listLatLon, 
			'time-series' as product, '2004-01-01T00:00:00' as "begin", 
			'2013-04-20T00:00:00' as "end", 'maxt' as maxt, 'mint' as  mint)
	)) w, 
	XMLTABLE('/dwml/data/location' passing w.result 
		columns "location-key" string, lattitude string path 'point/@latitude', 
			longitude string path 'point/@longitude') t

2. SOAP11 RPC call providing all of the parameter values for the invoke procedure.  With a SOAP
invocation, the action is used to convey the SOAPAction header value if needed.  Also note
the use of the endpoint here with an absolute URL, which will be used instead of the default 
on the datasource.  We are also using the DefaultServiceMode of MESSAGE (see the weather-vdb.xml file)
to use the full SOAP message request and response.

select xmlserialize(document w.result as string) from 
	(call weather.invoke(action=>'http://graphical.weather.gov/xml/SOAP_server/ndfdXMLserver.php#LatLonListZipCode', 
		endpoint=>'http://graphical.weather.gov/xml/SOAP_server/ndfdXMLserver.php',
		binding=>'SOAP11',
		request=>'
			<SOAP-ENV:Envelope SOAP-ENV:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/">
				<SOAP-ENV:Body>
					<ns8077:LatLonListZipCode xmlns:ns8077="uri:DWMLgen">
						<listZipCodeList xsi:type="xsd:string">20910 25414</listZipCodeList>
					</ns8077:LatLonListZipCode>
				</SOAP-ENV:Body>
			</SOAP-ENV:Envelope>')) as w

See the Reference or retrieve the JDBC DatabaseMetadata on the invoke procedure for a full description of the parameters.
