/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.connector.xmlsource.soap;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.rpc.ServiceException;

import org.apache.axis.AxisEngine;
import org.apache.axis.ConfigurationException;
import org.apache.axis.EngineConfiguration;
import org.apache.axis.Handler;
import org.apache.axis.SimpleChain;
import org.apache.axis.encoding.TypeMappingRegistry;
import org.apache.axis.handlers.soap.SOAPService;
import org.apache.axis.wsdl.gen.Parser;


/** 
 * This is SOAP Service which extends the Axis service call so that we can dynamically
 * include the handlers into the request and response flows.
 */
public class SoapService extends org.apache.axis.client.Service {
    SecurityToken securityToken;
    
    public SoapService(Parser parser, QName serviceName, SecurityToken secToken) throws ServiceException {
        super(parser, serviceName);
        this.securityToken = secToken;
    }
    
    @Override
	protected EngineConfiguration getEngineConfiguration() {        
        EngineConfiguration config =  super.getEngineConfiguration();
        return new ClientEngineConfiguration(config);
    }   

    /**
     * Engine Configuration class which has the request and response flow handlers 
     */
    class ClientEngineConfiguration implements EngineConfiguration{
        EngineConfiguration config = null;
        
        public ClientEngineConfiguration(EngineConfiguration config) {
            this.config = config;
        }
        
        public void configureEngine(AxisEngine engine) throws ConfigurationException {
            config.configureEngine(engine);
        }
        
        public void writeEngineConfig(AxisEngine engine) throws ConfigurationException {
            config.writeEngineConfig(engine);
        }
        
        public Handler getHandler(QName name) throws ConfigurationException {
            return config.getHandler(name);
        }
        
        public SOAPService getService(QName name) throws ConfigurationException {
            return config.getService(name);
        }
        
        public SOAPService getServiceByNamespaceURI(String uri) throws ConfigurationException {
            return config.getServiceByNamespaceURI(uri);
        }
        
        public Handler getTransport(QName name) throws ConfigurationException {
            return config.getTransport(name);
        }
        
        public TypeMappingRegistry getTypeMappingRegistry() throws ConfigurationException {
            return config.getTypeMappingRegistry();
        }
        
        public Handler getGlobalRequest() throws ConfigurationException {
            SimpleChain requestHandler = (SimpleChain)config.getGlobalRequest();
            if (requestHandler == null) {
                requestHandler = new SimpleChain();
            }
            // if the se
            if (securityToken instanceof WSSecurityToken) {
                requestHandler.addHandler(new org.apache.ws.axis.security.WSDoAllSender());
            }
            return requestHandler;
        }
        
        public Handler getGlobalResponse() throws ConfigurationException {
            return config.getGlobalResponse();
        }
        
        public Hashtable getGlobalOptions() throws ConfigurationException {
            return config.getGlobalOptions();
        }
        
        public Iterator getDeployedServices() throws ConfigurationException {
            return config.getDeployedServices();
        }
        
        public List getRoles() {
            return config.getRoles();
        }                   
    }
}