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

package com.metamatrix.connector.xml.base;

import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.IQuery;

/**
 * created by JChoate on Jun 16, 2005
 *
 */
public class TestXMLExecution extends TestCase {

	private String m_vdbPath = null;	
    
    public TestXMLExecution() {
    	super();
    	checkVdbPath();
    }
    
	private void checkVdbPath() {
    	if (m_vdbPath == null) {
    		m_vdbPath = ProxyObjectFactory.getStateCollegeVDBLocation();	
    	}        		
	}


	public TestXMLExecution(String test) {
        super(test);
        checkVdbPath();
    }
    
    public void testNext() {
        String queryString = "select Company_id from Company where Company_id is not null order by Company_id";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(query, m_vdbPath);
        assertNull(execution.getInfo());
        try {
            execution.execute();
            assertNotNull(execution.next());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }
    
    public void testNextBatchExceedSize() {
        String queryString = "select Name from Employee where Name in ('george')";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(query, m_vdbPath);
        assertNull(execution.getInfo());
        try {
            execution.execute();
            List result = execution.next();
            assertNotNull("Batch is null", result);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }   

    public void testNextBatchMultiCriteria() {
        String queryString = "select state, zip From Conference where Company_id in ('Widgets Inc.') and Department_id in ('QA')";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
    	XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(query, m_vdbPath);
        assertNull(execution.getInfo());
        try {
            execution.execute();
            List result = execution.next();
            assertNotNull("Batch is null", result);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }     
    
    public void testGetInfo() {
        String queryString = "select Company_id from Company where Company_id is not null order by Company_id";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
    	XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(query, m_vdbPath);
        assertNull(execution.getInfo());
        try {
            execution.execute();
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
        assertNotNull("ExecutionInfo is null", execution.getInfo());
    }
}
