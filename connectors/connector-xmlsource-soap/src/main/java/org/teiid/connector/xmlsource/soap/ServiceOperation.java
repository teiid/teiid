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

package org.teiid.connector.xmlsource.soap;

import java.io.StringReader;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.Dispatch;

import org.teiid.connector.xmlsource.XMLSourcePlugin;




/**
 * Represents the operation of a service 
 */
public class ServiceOperation {
    private static final String DOCUMENT = "document"; //$NON-NLS-1$
    
    private String name;   
    private String portName;
    private String style;
    private int queryTimeout;
    private String endPoint;
    private Dispatch<Source> dispatch;
    private String targetNamespace;
    
    ServiceOperation(String name, String portName, String style, Dispatch<Source> dispatch, String targetNamespace) {
        this.name = name;
        this.portName = portName;
        this.style = style;
        this.dispatch = dispatch;
        this.targetNamespace = targetNamespace;
    }
    
    String getName() {
    	return this.name;
    }
       
    int getQueryTimeout() {
    	return this.queryTimeout;
    }
    
    void setQueryTimeout(int timeout) {
    	this.queryTimeout = timeout;
    }
    
    String getEndPoint() {
        return this.endPoint;
    }
    
    void setEndPoint(String endPoint) {
    	this.endPoint = endPoint;
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
    public Source execute(Object[] args, SecurityToken token) throws ExcutionFailedException {
        if (isDocLiteral()) {
            if (args.length != 1) {
                throw new ExcutionFailedException(XMLSourcePlugin.Util.getString("wrong_number_params", new Object[] {Integer.valueOf(1), Integer.valueOf(args.length)})); //$NON-NLS-1$
            }
        }
        else {
        	throw new ExcutionFailedException(XMLSourcePlugin.Util.getString("support_only_doc_literal")); //$NON-NLS-1$
        }
        return this.dispatch.invoke(buildRequest(args));
    }
    
    Source buildRequest(Object[] args){
    	StringBuilder sb = new StringBuilder();
    	sb.append("<tns1:").append(this.name);
    	sb.append(" xmlns:tns1=\"").append(this.targetNamespace).append("\">");
    	
    	for (Object obj:args) {
    		sb.append(obj.toString());
    	}
    	
    	sb.append("</tns1:").append(this.name).append(">");

    	System.out.println(sb.toString());
    	
    	return new StreamSource(new StringReader(sb.toString()));
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
