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

package com.metamatrix.soap.service;

import java.util.Properties;

import junit.framework.TestCase;

import org.apache.axis2.AxisFault;

import com.metamatrix.soap.util.WSDLServletUtil;

/**
 * 
 */
public class TestDataServiceWebServiceImpl extends TestCase {
    
    private DataServiceWebServiceImpl impl = new DataServiceWebServiceImpl();
    
    
    public void testGetAuthenticationProperties() {
        DataServiceWebServiceImpl impl = new DataServiceWebServiceImpl();
        Properties props = impl.getAuthenticationProperties("test", "this"); //$NON-NLS-1$ //$NON-NLS-2$

        if (!("test".equals(props.getProperty(DataServiceWebServiceImpl.USER_NAME)) && "this".equals(props.getProperty(DataServiceWebServiceImpl.PASSWORD)))) { //$NON-NLS-1$ //$NON-NLS-2$
            fail();
        } 

    }
        
    public void testGetQueryTimeoutWOutValue() {
    	
    	int queryTimeout = getDataServiceWebServiceImpl().getQueryTimeout();
        assertEquals(0,queryTimeout);
    }
    
    public void testCreateFault() {
    	try{
    		getDataServiceWebServiceImpl().createSOAPFaultMessage(new Exception("Something bad happened"),"Really, something bad happened","Server"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	}catch(AxisFault fault){
    		assertEquals("Really, something bad happened",fault.getMessage()); //$NON-NLS-1$	
    	}
    	
    }

    public void testGetQueryTimeoutWNonIntegerValue() {
        
        System.setProperty(WSDLServletUtil.MM_WEBSERVICE_QUERY_TIMEOUT, "V");//$NON-NLS-1$
        int queryTimeout = getDataServiceWebServiceImpl().getQueryTimeout();
        assertEquals(0,queryTimeout);
    }
    
    public void testGetQueryTimeoutWGoodValue() {
        
        System.setProperty(WSDLServletUtil.MM_WEBSERVICE_QUERY_TIMEOUT, "600000");//$NON-NLS-1$
        int queryTimeout = getDataServiceWebServiceImpl().getQueryTimeout();
        assertEquals(600000,queryTimeout);
    }
    
    public void testGetQueryTimeoutWNULLValue() {
        
        System.setProperty(WSDLServletUtil.MM_WEBSERVICE_QUERY_TIMEOUT, "");//$NON-NLS-1$
        int queryTimeout = getDataServiceWebServiceImpl().getQueryTimeout();
        assertEquals(0,queryTimeout);
    }
    
    private DataServiceWebServiceImpl getDataServiceWebServiceImpl() {
        return impl;
    }

}
