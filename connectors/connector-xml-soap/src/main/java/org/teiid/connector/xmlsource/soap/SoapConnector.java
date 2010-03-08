package org.teiid.connector.xmlsource.soap;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Set;

import javax.wsdl.Definition;
import javax.wsdl.Service;
import javax.wsdl.WSDLException;
import javax.wsdl.factory.WSDLFactory;
import javax.wsdl.xml.WSDLReader;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.basic.BasicConnector;
import org.teiid.connector.xmlsource.XMLSourcePlugin;


public class SoapConnector extends BasicConnector {

    private SoapManagedConnectionFactory config;
    private SoapService service = null; 

	@Override
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		super.initialize(env);

		this.config = (SoapManagedConnectionFactory)env;

        String wsdl = this.config.getWsdl();
        String portName = this.config.getPortName();
        String serviceName = this.config.getServiceName();
        
        // check if WSDL is supplied
        if (wsdl == null || wsdl.trim().length() == 0) {
            throw new ConnectorException(XMLSourcePlugin.Util.getString("wsdl_not_set")); //$NON-NLS-1$            
        }

        this.config.getLogger().logDetail(XMLSourcePlugin.Util.getString("loading_wsdl", new Object[] {wsdl})); //$NON-NLS-1$
		
		this.service = buildServiceStub(wsdl, serviceName, portName);
		
		this.config.getLogger().logInfo("Loaded for SoapConnector"); //$NON-NLS-1$
	}
    

    public Connection getConnection() throws ConnectorException {
        return new SoapConnection(this.config, this.service, SecurityToken.getSecurityToken(this.config));
    }

    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities()  {
    	return SoapSourceCapabilities.class;
    }
    
    private SoapService buildServiceStub(String wsdl, String serviceName, String portName) throws ConnectorException {
        try {            
            // first parse the WSDL file
        	WSDLReader wsdlReader = WSDLFactory.newInstance().newWSDLReader();
        	Definition wsdlDefinition = wsdlReader.readWSDL(wsdl);
        	
        	
        	Map services = wsdlDefinition.getServices();
        	Service wsdlService = (Service)services.values().iterator().next();
        	if (serviceName != null) {
        		Set<String> keys = services.keySet();
        		for (String key:keys) {
        			if (key.equalsIgnoreCase(serviceName)) {
        				wsdlService = (Service)services.get(key);
        			}
        		}
        	}
            return new SoapService(new URL(wsdl), wsdlService, portName, wsdlDefinition.getTargetNamespace()); 
        } catch (WSDLException e) {
            throw new ConnectorException(e, XMLSourcePlugin.Util.getString("failed_loading_wsdl", new Object[] {wsdl})); //$NON-NLS-1$
        } catch (MalformedURLException e) {
        	throw new ConnectorException(e, XMLSourcePlugin.Util.getString("failed_loading_wsdl", new Object[] {wsdl})); //$NON-NLS-1$
        }
    }
    
    SoapService getService() {
    	return this.service;
    }
}

