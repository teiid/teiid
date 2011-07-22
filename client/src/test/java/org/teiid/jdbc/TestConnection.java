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

package org.teiid.jdbc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;

import java.sql.SQLException;
import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.client.DQP;
import org.teiid.client.security.LogonResult;
import org.teiid.client.security.SessionToken;
import org.teiid.client.util.ResultsFuture;
import org.teiid.client.xa.XATransactionException;
import org.teiid.client.xa.XidImpl;
import org.teiid.net.ServerConnection;


public class TestConnection extends TestCase {

	protected static final String STD_DATABASE_NAME         = "QT_Ora9DS"; //$NON-NLS-1$
    protected static final int STD_DATABASE_VERSION      = 1; 
    
    static String serverUrl = "jdbc:teiid:QT_Ora9DS@mm://localhost:7001;version=1;user=metamatrixadmin;password=mm"; //$NON-NLS-1$

    public TestConnection(String name) {
        super(name);
    }
    
    static class  InnerDriver extends TeiidDriver {
    	String iurl = null;
    	public InnerDriver(String url) {
    		iurl = url;
    	}

    	public void parseUrl(Properties props) throws SQLException {
 				super.parseURL(iurl, props);
    	}
    }
    
    public static ConnectionImpl getMMConnection() {
    	return getMMConnection(serverUrl);  	
    }
    
    public static ConnectionImpl getMMConnection(String url) {
    	ServerConnection mock = mock(ServerConnection.class);
    	DQP dqp = mock(DQP.class);
    	try {
			stub(dqp.start((XidImpl)Mockito.anyObject(), Mockito.anyInt(), Mockito.anyInt())).toAnswer(new Answer() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					return ResultsFuture.NULL_FUTURE;
				}
			});
			stub(dqp.rollback((XidImpl)Mockito.anyObject())).toAnswer(new Answer() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					return ResultsFuture.NULL_FUTURE;
				}
			});
			stub(dqp.rollback()).toAnswer(new Answer() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					return ResultsFuture.NULL_FUTURE;
				}
			});
		} catch (XATransactionException e) {
			throw new RuntimeException(e);
		}
		
		Properties props = new Properties();
		
		try {
			new InnerDriver(url).parseUrl(props);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    	stub(mock.getService(DQP.class)).toReturn(dqp);
    	
    	stub(mock.getLogonResult()).toReturn(new LogonResult(new SessionToken(1, "admin"), STD_DATABASE_NAME,STD_DATABASE_VERSION , "fake")); //$NON-NLS-1$
    	return new ConnectionImpl(mock, props, url);
    }

    public void testGetMetaData() throws Exception {
        assertNotNull(getMMConnection().getMetaData());
    }

    public void testGetSchema() throws Exception {
        assertEquals("Actual schema is not equql to the expected one. ", STD_DATABASE_NAME, getMMConnection().getVDBName()); //$NON-NLS-1$
    }

    public void testNativeSql() throws Exception {
        String sql = "SELECT * FROM BQT1.SmallA"; //$NON-NLS-1$
        assertEquals("Actual schema is not equql to the expected one. ", sql, getMMConnection().nativeSQL(sql)); //$NON-NLS-1$
    }

    /** test getUserName() through DriverManager */
    public void testGetUserName2() throws Exception {        
        assertEquals("Actual userName is not equal to the expected one. ", "admin", getMMConnection().getUserName()); //$NON-NLS-1$ //$NON-NLS-2$
    }
      
    /** test isReadOnly default value on Connection */
    public void testIsReadOnly() throws Exception {
        assertEquals(false, getMMConnection().isReadOnly());
    }

    /** test setReadOnly on Connection */
    public void testSetReadOnly1() throws Exception {
    	ConnectionImpl conn = getMMConnection();
        conn.setReadOnly(true);
        assertEquals(true, conn.isReadOnly());
    }

    /** test setReadOnly on Connection during a transaction */
    public void testSetReadOnly2() throws Exception {
    	ConnectionImpl conn = getMMConnection();
        conn.setAutoCommit(false);
        try {
            conn.setReadOnly(true);
            fail("Error Expected"); //$NON-NLS-1$
        } catch (SQLException e) {
            // error expected
        }
    }
    
    /**
     * Test the default of the JDBC4 spec semantics is true
     */
    public void testDefaultSpec() throws Exception {
        assertEquals("true",
        		(getMMConnection().getExecutionProperties().getProperty(ExecutionProperties.JDBC4COLUMNNAMEANDLABELSEMANTICS) == null ? "true" : "false"));
    } 
    
    /**
     * Test turning off the JDBC 4 semantics
     */
    public void testTurnOnSpec() throws Exception {
        assertEquals("true", getMMConnection(serverUrl + ";useJDBC4ColumnNameAndLabelSemantics=true").getExecutionProperties().getProperty(ExecutionProperties.JDBC4COLUMNNAMEANDLABELSEMANTICS));
    }    
    
    /**
     * Test turning off the JDBC 4 semantics
     */
    public void testTurnOffSpec() throws Exception {
        assertEquals("false", getMMConnection(serverUrl + ";useJDBC4ColumnNameAndLabelSemantics=false").getExecutionProperties().getProperty(ExecutionProperties.JDBC4COLUMNNAMEANDLABELSEMANTICS));
    }

}
