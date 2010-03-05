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

/**
 * CurrencyExchangeBindingSkeleton.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package com.metamatrix.www.CurrencyExchangeService;

public class CurrencyExchange implements org.apache.axis.wsdl.Skeleton {
    private static java.util.Map _myOperations = new java.util.Hashtable();
    private static java.util.Collection _myOperationsList = new java.util.ArrayList();

    /**
    * Returns List of OperationDesc objects with this name
    */
    public static java.util.List getOperationDescByName(java.lang.String methodName) {
        return (java.util.List)_myOperations.get(methodName);
    }

    /**
    * Returns Collection of OperationDescs
    */
    public static java.util.Collection getOperationDescs() {
        return _myOperationsList;
    }

    static {
        org.apache.axis.description.OperationDesc _oper;
        org.apache.axis.description.ParameterDesc [] _params;
        _params = new org.apache.axis.description.ParameterDesc [] {
            new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "country1"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"), java.lang.String.class, false, false),  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "country2"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"), java.lang.String.class, false, false),  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        };
        _oper = new org.apache.axis.description.OperationDesc("getRate", _params, new javax.xml.namespace.QName("", "Result")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        _oper.setReturnType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "float")); //$NON-NLS-1$ //$NON-NLS-2$
        _oper.setElementQName(new javax.xml.namespace.QName("urn:xmethods-CurrencyExchange", "getRate")); //$NON-NLS-1$ //$NON-NLS-2$
        _oper.setSoapAction(""); //$NON-NLS-1$
        _myOperationsList.add(_oper);
        if (_myOperations.get("getRate") == null) { //$NON-NLS-1$
            _myOperations.put("getRate", new java.util.ArrayList()); //$NON-NLS-1$
        }
        ((java.util.List)_myOperations.get("getRate")).add(_oper); //$NON-NLS-1$
    }


    public float getRate(java.lang.String country1, java.lang.String country2) throws java.rmi.RemoteException
    {        
        return 1.0f;
    }

}
