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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Vector;

import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;

import org.apache.axis.client.Call;
import org.apache.axis.encoding.ser.ElementDeserializerFactory;
import org.apache.axis.encoding.ser.ElementSerializerFactory;
import org.apache.axis.message.RPCElement;
import org.apache.axis.message.SOAPBodyElement;
import org.apache.axis.utils.XMLUtils;
import org.apache.axis.wsdl.symbolTable.Parameter;
import org.apache.axis.wsdl.symbolTable.Parameters;
import org.apache.axis.wsdl.symbolTable.TypeEntry;
import org.teiid.connector.api.ConnectorEnvironment;
import org.w3c.dom.Element;

import com.metamatrix.connector.xmlsource.XMLSourcePlugin;


/**
 * Represents the operation of a service 
 */
public class ServiceOperation {
    private static final String DOCUMENT = "document"; //$NON-NLS-1$
    
    String name;   
    Parameters parameters;
    Call stub;
    String portName;
    boolean usesComplexType = false;
    String style;
    ConnectorEnvironment env;
    
    /**
     * ctor 
     * @param name
     * @param parms
     */
    ServiceOperation(ConnectorEnvironment env, String name, Parameters parms, String portName, String style) {
        this.name = name;
        this.parameters = parms;
        this.portName = portName;
        this.style = style;
        this.env = env;
        
        // check the input parameters types if they are simple or complex
        // type;
        for (int i = 0; i < this.parameters.list.size(); i++) {
            Parameter p = (Parameter) this.parameters.list.get(i);
            TypeEntry type = p.getType();
            if (!type.isBaseType()) {
                this.usesComplexType = true;
            }
        }        
    }
       
    /**
     * A stub to procedure; for execution. 
     * @param stub
     */
    void setStub(Call stub) {
        this.stub = stub;
    }
    
    int getQueryTimeout() {
        String timeout = this.env.getProperties().getProperty(SoapConnectorProperties.QUERY_TIMEOUT);
        if (timeout != null && timeout.length() > 0) {
            return Integer.parseInt(timeout)*1000;
        }
        return -1;
    }
    
    String getEndPoint() {
        return this.env.getProperties().getProperty(SoapConnectorProperties.END_POINT);
    }
        
    /**
     * is this a doc literal service operation 
     * @return true if yes; false otherwise.
     */
    boolean isDocLiteral() {
        return (this.style != null && this.style.equalsIgnoreCase(DOCUMENT));        
    }
    
    /**
     * execute the Service with given arguments
     * @param args - arguments to the service
     * @return return value; null on void return
     */
    public Source execute(Object[] args, Map outs) throws ExcutionFailedException {
        Object doc = null;
        try {
            // If there is alternate endpoint use it.
            if (getEndPoint() != null && getEndPoint().length() > 0) {
                this.stub.setTargetEndpointAddress(getEndPoint());
            }
            
            // If query timeout is set use it
            if (getQueryTimeout() != -1) {
                this.stub.setTimeout(new Integer(getQueryTimeout()));
            }
                        
            // check the param count of the submitted operation, in doc-literal
            // there should always be only one input
            if (isDocLiteral()) {
                if (args.length != 1) {
                    throw new ExcutionFailedException(XMLSourcePlugin.Util.getString("wrong_number_params", new Object[] {new Integer(1), new Integer(args.length)})); //$NON-NLS-1$
                }
            }
            else {
                int requiredParamCount = this.stub.getOperation().getNumInParams();
                if (requiredParamCount != args.length) {
                    throw new ExcutionFailedException(XMLSourcePlugin.Util.getString("wrong_number_params", new Object[] {new Integer(requiredParamCount), new Integer(args.length)})); //$NON-NLS-1$
                }
            }

            // set input types
            // if we are using complex types, since we do not know how to serialize
            // deserilize the objects, we need to pass them as the direct soap body
            // elements; this is useful for doc/litral stuff where we do not need to
            // put toghether soap envelope.
            boolean messageStyleInvoke = false;
            if (this.usesComplexType || isDocLiteral()) {
                ArrayList bodyElements = new ArrayList();
                for (int i = 0; i < args.length; i++) {
                    // we may little sofisticated streamer here, but this will work for now.
                    InputStream element = new ByteArrayInputStream(args[i].toString().getBytes());
                    bodyElements.add(new SOAPBodyElement(element)); 
                }
                args = bodyElements.toArray(new Object[bodyElements.size()]);
                messageStyleInvoke = true;
            }

            // set output type; if it is complex type just use it as element object;
            Parameter returnParam = this.parameters.returnParam;
            if ( returnParam != null) {
                if(!returnParam.getType().isBaseType()) {
                    this.stub.registerTypeMapping(org.w3c.dom.Element.class, returnParam.getType().getQName(), new ElementSerializerFactory(),new ElementDeserializerFactory());
                }
            }            
            
            // invoke the service
            doc = this.stub.invoke(args);                                        
            Map outputs = this.stub.getOutputParams();
            if(outputs != null) {
                outs.putAll(outputs);
            }
            
            // if the sevice is executed with message style, we would get back raw xml
            // in out message 
            if (messageStyleInvoke) {
                doc = ((Vector)doc).get(0);
            }
            
            // if the result is an xml element convert into string
            if (doc instanceof RPCElement) {
                return new DOMSource((RPCElement)doc);
            }
            else if (doc instanceof Element) {
                return new DOMSource((Element)doc);                
            }
            
            // if we executed a RPC service, and we received a scalar as output
            // then we need to wrap this in the xml type
            if (!isDocLiteral() && doc != null && returnParam != null 
                            && returnParam.getType().isBaseType()) {
                StringBuffer sb = new StringBuffer();
                sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><return type=\""); //$NON-NLS-1$
                sb.append(doc.getClass().getName());
                sb.append("\">"); //$NON-NLS-1$
                sb.append(XMLUtils.xmlEncodeString(doc.toString()));
                sb.append("</return>"); //$NON-NLS-1$
                return new StreamSource(new StringReader(sb.toString()));
            }
            
        } catch (RemoteException e) {
            throw new ExcutionFailedException(e);
        }        
        return null;
    }
    
    // marker class
    static class ExcutionFailedException extends Exception{       
        public ExcutionFailedException(Throwable e) {
            super(e);
        }
        public ExcutionFailedException(String msg) {
            super(msg);
        }        
    }    
    
    
}
