/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.soap.sqlquerywebservice.service;

import javax.xml.namespace.QName;

import org.apache.axis2.AxisFault;

/**
 * This is an exception class that is thrown from the SqlQueryWebService implementation that repesents a SOAP fault.
 * 
 * @since 4.2
 */
public class SqlQueryWebServiceFault extends AxisFault{

    /*
     * This is a namespace for the faultcodes for the metamatrix server api. It will be used when a fault is thrown from the api
     * as the namespace for the faultcode in the returned fault.
     */
    public static final String SOAP_11_FAULTCODES_NAMESPACE = "http://schemas.xmlsoap.org/soap/envelope/"; //$NON-NLS-1$
    public static final String SOAP_11_STANDARD_CLIENT_FAULT_CODE = "Client"; //$NON-NLS-1$
    public static final String SOAP_11_STANDARD_SERVER_FAULT_CODE = "Server"; //$NON-NLS-1$

    
   /**
    * @param message
    * @param cause
    */
    public SqlQueryWebServiceFault(String message, QName code) {
		super(message, code);
	}

  
    public static final SqlQueryWebServiceFault create(boolean client, Throwable e) {

        String faultCodeString = null;

        /*
         * if the exception being thrown is due to the inputs that the client gave us, then the fault code should be 'Client' per
         * the SOAP 1.1 specification. If there is some 'internal' server side reason for not being able to fulfill the request,
         * then the fault code should be 'Server' per the SOAP 1.1 specification.
         */
        if (client) {
            faultCodeString = SOAP_11_STANDARD_CLIENT_FAULT_CODE;
        } else {
            /*
             * if this is not a MetaMatrixProcessingException we make the assumption that this exception
             * is not a result of some invalid client input, but that it is an exception thrown because 
             * of an invalid server state.
             */
            faultCodeString = SOAP_11_STANDARD_SERVER_FAULT_CODE;
        }


        QName faultCode = new QName(SOAP_11_FAULTCODES_NAMESPACE, faultCodeString);

        
        SqlQueryWebServiceFault fault = new SqlQueryWebServiceFault(e.getMessage(), faultCode);

        return fault;
    }

}
