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

package com.metamatrix.server.integration;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLXML;
import java.sql.Statement;

import junit.framework.TestCase;

import org.teiid.jdbc.TeiidDriver;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.MMSQLException;

/**
 */
public class TestXQueryServices extends TestCase {

    public TestXQueryServices(String testName) {
    	super(testName);
    }
    
    /*
     * TOOD: it appears that this xquery vdb was built so that the procedure call was modeled with a return parameter.
     * This is then not an effective test of xquery, but shows how a procedure without a resultset should behave.
     */
    public void testXQueryCall() throws Exception {
    	Class.forName(TeiidDriver.class.getName());
    	Connection conn = DriverManager.getConnection("jdbc:metamatrix:xq@" + UnitTestUtil.getTestDataPath() + "/xquery/xquery.properties;txnAutoWrap=OFF;user=test" ); //$NON-NLS-1$ //$NON-NLS-2$
    	CallableStatement cs = conn.prepareCall("{? = call xqs.test}"); //$NON-NLS-1$
    	assertFalse(cs.execute());
    	SQLXML xml = cs.getSQLXML(1);
    	assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?><test/>", xml.toString()); //$NON-NLS-1$
    	
    	Statement stmt = conn.createStatement();
    	try {
    		stmt.executeQuery("exec xqs.test()"); //$NON-NLS-1$
    		fail("exception expected"); //$NON-NLS-1$
    	} catch (MMSQLException e) {
    		assertEquals("Statement does not return a result set.", e.getMessage()); //$NON-NLS-1$
    	}
    	conn.close();
    }

}
