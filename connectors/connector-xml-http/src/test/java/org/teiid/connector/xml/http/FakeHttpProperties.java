package org.teiid.connector.xml.http;

import java.util.Properties;

import org.teiid.connector.xml.SecureConnectorState;
import org.teiid.connector.xml.XMLConnectorState;

@SuppressWarnings("nls")
public class FakeHttpProperties {
	public static Properties getDefaultXMLRequestProps() {
		Properties defaultHTTPProps = getDefaultHttpProps();
		defaultHTTPProps.setProperty(HTTPConnectorState.PARAMETER_METHOD,HTTPConnectorState.PARAMETER_XML_REQUEST);
		return defaultHTTPProps;
	}

	public static Properties getDefaultHttpProps() {
		Properties testHTTPProps = new Properties();
		testHTTPProps.setProperty(XMLConnectorState.STATE_CLASS_PROP, "com.metamatrix.connector.xml.http.HTTPConnectorState");
		testHTTPProps.setProperty(HTTPConnectorState.ACCESS_METHOD,HTTPConnectorState.GET);
		testHTTPProps.setProperty(HTTPConnectorState.PARAMETER_METHOD,HTTPConnectorState.PARAMETER_XML_REQUEST);
		testHTTPProps.setProperty(HTTPConnectorState.URI, "http://0.0.0.0:8673");
		// testHTTPProps.setProperty(HTTPConnectorState.PROXY_URI,
		// "http://0.0.0.0:8673");
		testHTTPProps.setProperty(HTTPConnectorState.REQUEST_TIMEOUT, "60");
		testHTTPProps.setProperty(HTTPConnectorState.XML_PARAMETER_NAME,"XMLRequest");
		testHTTPProps.setProperty(HTTPConnectorState.HTTP_BASIC_USER, "");
		testHTTPProps.setProperty(HTTPConnectorState.HTTP_BASIC_PASSWORD, "");
		testHTTPProps.setProperty(SecureConnectorState.SECURITY_DESERIALIZER_CLASS,"org.teiid.connector.xml.http.DefaultTrustDeserializer");
		return testHTTPProps;

	}
	
  
  public static Properties getDefaultHTTPProps() {
      Properties testHTTPProps = new Properties();
      testHTTPProps.setProperty(HTTPConnectorState.URI, "http://localhost:8673"); //$NON-NLS-1$
      testHTTPProps.setProperty(HTTPConnectorState.REQUEST_TIMEOUT, "60");	 //$NON-NLS-1$
      testHTTPProps.setProperty(XMLConnectorState.STATE_CLASS_PROP, "com.metamatrix.connector.xml.http.HTTPConnectorState"); //$NON-NLS-1$
      testHTTPProps.setProperty(HTTPConnectorState.HTTP_BASIC_USER, "");
      testHTTPProps.setProperty(HTTPConnectorState.HTTP_BASIC_PASSWORD, "");
      testHTTPProps.setProperty(SecureConnectorState.SECURITY_DESERIALIZER_CLASS, "com.metamatrix.connector.xml.http.DefaultTrustDeserializer");
      testHTTPProps.setProperty(HTTPConnectorState.PARAMETER_METHOD, HTTPConnectorState.PARAMETER_NAME_VALUE);
      testHTTPProps.setProperty(HTTPConnectorState.ACCESS_METHOD, HTTPConnectorState.GET);
      return testHTTPProps;
   }	

}
