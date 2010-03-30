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

package org.teiid.connector.xml.soap;

import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import javax.xml.transform.Source;
import javax.xml.ws.Dispatch;


/** 
 * This is SOAP Service which extends the Axis service call so that we can dynamically
 * include the handlers into the request and response flows.
 */
public class SoapService  {
    private URL wsdlLocation;
    private Service wsdlService;
    private Port port;
    private String style;
    private String targetnameSpace;
    
    public SoapService(URL wsdlLocation, Service service, String portName, String namespace) {
    	this.wsdlService = service;
    	this.wsdlLocation = wsdlLocation;
    	this.port = selectPort(service.getPorts(), portName);
        this.style = buildStyle(this.port);
        this.targetnameSpace = namespace;
    }
    
    
	public QName getServiceName() {
		return this.wsdlService.getQName();
	}       
    
    /**
     * Get Port to be used for the  
     * @param ports
     * @param portName
     * @return
     * @throws Exception
     */
    private Port selectPort(Map ports, String portName) {
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
     * Build a map of available operations in the WSDL 
     * @param wsdlParser
     */
    private String buildStyle(Port usePort) {
        Binding binding = usePort.getBinding();
        
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
        return style;
    }    
    
    public ServiceOperation findOperation(String name) throws OperationNotFoundException {
    	Binding binding =  this.port.getBinding();
    	List<BindingOperation> bindingOperations = binding.getBindingOperations();
    	
        for (BindingOperation bo:bindingOperations) {
        	Operation operation = bo.getOperation();
        	if (operation.getName().equals(name)) {
        		return new ServiceOperation(operation.getName(), this.port.getName(), this.style, createDispatch(), this.targetnameSpace);  
        	}
        }   
        throw new OperationNotFoundException(name);
    }

    private Dispatch<Source> createDispatch() {
        // now create a stub for the operation and set on the object.
    	javax.xml.ws.Service executionService = javax.xml.ws.Service.create(this.wsdlLocation, this.wsdlService.getQName());
    	Dispatch<Source> dispatch = executionService.createDispatch(new QName(this.wsdlService.getQName().getNamespaceURI(), this.port.getName(), "teiid"), Source.class, javax.xml.ws.Service.Mode.PAYLOAD);
    	return dispatch;
    }
     
    static class OperationNotFoundException extends Exception{
        public OperationNotFoundException(String e) {super(e);}
        public OperationNotFoundException(Throwable e) {super(e);}
        // nothing, just marker
    }
    
}