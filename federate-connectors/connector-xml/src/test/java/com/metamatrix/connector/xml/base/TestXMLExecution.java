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

import junit.framework.TestCase;

import com.metamatrix.connector.xml.XMLConnection;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IQuery;

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
    
    public void testInit() {        
        
        XMLExecution execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        assertNotNull("XMLExecutionImpl is null", execution);
    }

    public void testExecute() {
        
        XMLExecutionImpl execution = ProxyObjectFactory.getXMLExecution(m_vdbPath, "oh", "no");
        String queryString = "select Company_id from Company where Company_id is not null order by Company_id";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
        final int maxBatch = 1000;
        try {
            execution.execute(query, maxBatch);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    
    }

    public void testEmptyNextBatch() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        try {
            Batch batch = execution.nextBatch();
            assertNotNull("The batch is null", batch);
            assertEquals(0, batch.getRowCount());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }        
    }
    
    public void testNextBatch() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        assertNull(execution.getInfo());
        String queryString = "select Company_id from Company where Company_id is not null order by Company_id";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
        final int maxBatch = 1000;
        try {
            execution.execute(query, maxBatch);
            Batch batch = execution.nextBatch();
            assertNotNull("Batch is null", batch);
            assertTrue(batch.getRowCount() > 0);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }
    
    public void testNextBatchExceedSize() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        assertNull(execution.getInfo());
        String queryString = "select Name from Employee where Name in ('george')";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
        final int maxBatch = 1;
        try {
            execution.execute(query, maxBatch);
            Batch batch = execution.nextBatch();
            assertNotNull("Batch is null", batch);
            assertTrue(batch.getRowCount() > 0);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }   

    public void testNextBatchMultiCriteria() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        assertNull(execution.getInfo());
        String queryString = "select state, zip From Conference where Company_id in ('Widgets Inc.') and Department_id in ('QA')";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
        final int maxBatch = 1;
        try {
            execution.execute(query, maxBatch);
            Batch batch = execution.nextBatch();
            assertNotNull("Batch is null", batch);
            assertTrue("The query results are the wrong size", batch.getRowCount() > 0);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }     
    
    public void testClose() {
        XMLExecution execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        try {
            execution.close();          
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testCancel() {
        XMLExecution execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
         try {
            execution.cancel();            
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testGetConnection() {
        XMLExecution execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        XMLConnection conn = execution.getConnection();
        assertNotNull("XMLConnectionImpl is null", conn);
    }

    public void testSetConnection() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
        execution.setConnection(conn);
        assertEquals(conn, execution.getConnection());
    }

    public void testGetInfo() {
        XMLExecutionImpl execution = ProxyObjectFactory.getDefaultXMLExecution(m_vdbPath);
        assertNull(execution.getInfo());
        String queryString = "select Company_id from Company where Company_id is not null order by Company_id";
        IQuery query = ProxyObjectFactory.getDefaultIQuery(m_vdbPath, queryString);
        final int maxBatch = 1000;        
        try {
            execution.execute(query, maxBatch);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
        assertNotNull("ExecutionInfo is null", execution.getInfo());
    }
}
