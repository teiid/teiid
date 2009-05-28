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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import javax.wsdl.Binding;
import javax.wsdl.BindingOperation;
import javax.wsdl.Operation;
import javax.wsdl.Port;
import javax.wsdl.Service;
import javax.wsdl.extensions.ExtensibilityElement;
import javax.wsdl.extensions.soap.SOAPAddress;
import javax.wsdl.extensions.soap.SOAPBinding;
import javax.wsdl.extensions.soap.SOAPOperation;
import javax.xml.namespace.QName;
import javax.xml.rpc.ServiceException;

import org.apache.axis.client.Call;
import org.apache.axis.wsdl.gen.Parser;
import org.apache.axis.wsdl.symbolTable.BindingEntry;
import org.apache.axis.wsdl.symbolTable.Parameters;
import org.apache.axis.wsdl.symbolTable.ServiceEntry;
import org.apache.axis.wsdl.symbolTable.SymTabEntry;
import org.apache.axis.wsdl.symbolTable.SymbolTable;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xmlsource.XMLSourceConnection;
import com.metamatrix.connector.xmlsource.XMLSourcePlugin;


/** 
 * A SOAP based connection object
 */
public class SoapConnection extends XMLSourceConnection {    
    // instance variables
    boolean connected = false;       
    SoapService service = null; // wsdl service
    Map operationsMap = new HashMap();
    SecurityToken securityToken = null;
    
    /** 
     * @param env
     * @throws ConnectorException
     */
    public SoapConnection(ConnectorEnvironment env) throws ConnectorException {
        super(env);
        connect();
    }
    
    @Override
    public ProcedureExecution createProcedureExecution(IProcedure command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
        return new SoapExecution(command, this.env, metadata, executionContext, this);
    }

    /** 
     * @see com.metamatrix.connector.xmlsource.XMLSourceConnection#isConnected()
     */
    @Override
	public boolean isConnected() {
        return this.connected;
    }

    /** 
     * @see com.metamatrix.connector.xmlsource.XMLSourceConnection#release()
     */
    @Override
	public void close() {
        disconnect();
        super.close();        
    }
    
    void connect() throws ConnectorException {
        Properties props = this.env.getProperties();        
        String wsdl = props.getProperty(SoapConnectorProperties.WSDL);
        String portName = props.getProperty(SoapConnectorProperties.PORT_NAME);
        
        // check if WSDL is supplied
        if (wsdl == null || wsdl.trim().length() == 0) {
            throw new ConnectorException(XMLSourcePlugin.Util.getString("wsdl_not_set")); //$NON-NLS-1$            
        }
        
        try {
            XMLSourcePlugin.logInfo(this.env.getLogger(), "loading_wsdl", new Object[] {wsdl}); //$NON-NLS-1$
            
            // first parse the WSDL file
            Parser wsdlParser = new Parser();
            wsdlParser.run(wsdl);

            // Find the service from WSDL
            Service wsdlService = getService(wsdlParser.getSymbolTable());

            // WS-Security handler provider
            this.securityToken = SecurityToken.getSecurityToken(this.env, null);
            
            // now create a AXIS based service 
            this.service  = new SoapService(wsdlParser, wsdlService.getQName(), this.securityToken);            
            Port port = selectPort(wsdlService.getPorts(), portName);
            
            // find all the available operations
            this.operationsMap = buildOperations(wsdlParser.getSymbolTable(), port);            
            
            this.connected = true;            
        } catch (Exception e) {
            throw new ConnectorException(e, XMLSourcePlugin.Util.getString("failed_loading_wsdl", new Object[] {wsdl})); //$NON-NLS-1$
        }        
        

    }

    /** 
     * Build a map of available operations in the WSDL 
     * @param wsdlParser
     */
    private Map buildOperations(SymbolTable symbolTable, Port usePort) {
        HashMap map = new HashMap();
        Binding binding = usePort.getBinding();
        BindingEntry bEntry = symbolTable.getBindingEntry(binding.getQName());
              
        // find out from the soap binding style what type of service is this.
        // if the style is document then it is doc-litral
        String style = null; 
        for (Iterator i = binding.getExtensibilityElements().iterator(); i.hasNext();) {
            final ExtensibilityElement extElement = (ExtensibilityElement)i.next();
            if (extElement instanceof SOAPBinding) {
                style = ((SOAPBinding)extElement).getStyle();
            }
        }
        
        // if the style not found on the binding then look for it on the operation.
        if (style == null) {
            for(Iterator i = binding.getBindingOperations().iterator(); i.hasNext();) {
                BindingOperation operation = (BindingOperation)i.next();
                List extElements = operation.getExtensibilityElements();
                for (final Iterator extIter = extElements.iterator(); extIter.hasNext();) {
                    final ExtensibilityElement extElement = (ExtensibilityElement)extIter.next();
                    if (extElement instanceof SOAPOperation) {
                        style = ((SOAPOperation)extElement).getStyle();
                    }
                } // for
            }
        }
        
        // now loop through all the available operations and make a note of them
        for (Iterator i = bEntry.getOperations().iterator(); i.hasNext();) {
            Operation operation = (Operation) i.next();

            Parameters parameters = bEntry.getParameters(operation);

            // create operation with the available details
            ServiceOperation so = new ServiceOperation(this.env, operation.getName(), parameters, usePort.getName(), style);
       
            // collect all the services available
            map.put(operation.getName(), so);
        }        
        
        return map;
    }
    
    /**
     * disconnect the connection; 
     */
    void disconnect() {
        this.connected = false;   
        this.operationsMap.clear();
    }
    
    /**
     * Get system table entry from wsdl for given element. 
     * @return
     */
    private Service getService(SymbolTable table) throws ServiceNotFoundException{
        Class serviceClazz = ServiceEntry.class;
        HashMap map = table.getHashMap();
        for (Iterator iter = map.values().iterator(); iter.hasNext();) {
            Vector v = (Vector) iter.next();                        
            for (int i = 0; i < v.size(); ++i) {
                SymTabEntry tabEntry = (SymTabEntry) v.elementAt(i);
                if (serviceClazz.isInstance(tabEntry)) {
                    ServiceEntry serviceEntry = (ServiceEntry)tabEntry;
                    return serviceEntry.getService();
                }
            }            
        }
        throw new ServiceNotFoundException();
    }    
    
    /**
     * Get Port to be used for the  
     * @param ports
     * @param portName
     * @return
     * @throws Exception
     */
    private Port selectPort(Map ports, String portName) throws Exception {
        Iterator valueIterator = ports.keySet().iterator();
        while (valueIterator.hasNext()) {
            String name = (String) valueIterator.next();

            if ((portName == null) || (portName.length() == 0)) {
                Port port = (Port) ports.get(name);
                List list = port.getExtensibilityElements();

                for (int i = 0; (list != null) && (i < list.size()); i++) {
                    Object obj = list.get(i);
                    if (obj instanceof SOAPAddress) {
                        return port;
                    }
                }
            } else if ((name != null) && name.equals(portName)) {
                return (Port) ports.get(name);
            }
        }
        return null;
    }    
                
    /**
     * Find the operation to execute 
     * @param name
     * @return operation if found; exception otherwise
     * @throws OperationNotFoundException
     */
    public ServiceOperation findOperation(String procedureName) throws OperationNotFoundException{

        // Check the Operation Name
        ServiceOperation operation = (ServiceOperation)operationsMap.get(procedureName);
        if (operation == null) {
            throw new OperationNotFoundException(procedureName);
        }
        // now create a stub for the operation and set on the object.
        try {                        
            Call call = (Call)service.createCall(QName.valueOf(operation.portName), QName.valueOf(procedureName));
            securityToken.handleSecurity(call);
            
            // now assign this call to the operation object.
            operation.setStub(call);
        } catch (ServiceException e) {
            throw new OperationNotFoundException(e);
        }        
        return operation;
    }
 
    
    // 
    static class ServiceNotFoundException extends Exception{
        // nothing, just a marker
    }
    static class OperationNotFoundException extends Exception{
        public OperationNotFoundException(String e) {super(e);}
        public OperationNotFoundException(Throwable e) {super(e);}
        // nothing, just marker
    }    
}
