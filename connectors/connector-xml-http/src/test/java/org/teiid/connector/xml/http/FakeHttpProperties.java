package org.teiid.connector.xml.http;


@SuppressWarnings("nls")
public class FakeHttpProperties {
	
	public static HTTPManagedConnectionFactory getDefaultXMLRequestProps() {
    	HTTPManagedConnectionFactory env = new HTTPManagedConnectionFactory();
		env.setConnectorStateClass("com.metamatrix.connector.xml.http.HTTPConnectorState");
		env.setAccessMethod(HTTPConnectorState.GET);
		env.setParameterMethod(HTTPConnectorState.PARAMETER_XML_REQUEST);
		env.setUri("http://0.0.0.0:8673");
		// testHTTPProps.setProperty(HTTPConnectorState.PROXY_URI,
		// "http://0.0.0.0:8673");
		env.setRequestTimeout(60);
		env.setXMLParmName("XMLRequest");
		env.setTrustDeserializerClass("org.teiid.connector.xml.http.DefaultTrustDeserializer");
		return env;
	}

}
