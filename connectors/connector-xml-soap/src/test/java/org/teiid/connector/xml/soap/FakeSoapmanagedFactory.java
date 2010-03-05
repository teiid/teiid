package org.teiid.connector.xml.soap;

import java.util.Properties;

import com.metamatrix.common.util.PropertiesUtils;

public class FakeSoapmanagedFactory {
	public static Properties createSOAPState() {
		Properties soapProps = new Properties();
		soapProps.setProperty(SOAPConnectorStateImpl.ENCODING_STYLE_PROPERTY_NAME, SOAPConnectorStateImpl.RPC_ENC_STYLE);
		soapProps.setProperty(SOAPConnectorStateImpl.CONNECTOR_EXCEPTION_ON_SOAP_FAULT, "true");
		return soapProps;
	}
	
	public static SOAPManagedConnectionFactory createSOAPStateHTTPBasic() {
		Properties props = createSOAPState();
//        props.setProperty(SoapConnectorProperties.AUTHORIZATION_TYPE, SecurityToken.HTTP_BASIC_AUTH); 
//        props.setProperty(SoapConnectorProperties.USERNAME, "foo"); //$NON-NLS-1$ 
//        props.setProperty(SoapConnectorProperties.PASSWORD, "foopassword"); //$NON-NLS-1$ 
        
        SOAPManagedConnectionFactory env = new SOAPManagedConnectionFactory();
        PropertiesUtils.setBeanProperties(env, props, null);
		return env;
	}
}
