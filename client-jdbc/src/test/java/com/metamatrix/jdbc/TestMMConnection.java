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

package com.metamatrix.jdbc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;

import java.sql.SQLException;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.api.ServerConnectionFactory;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.platform.security.api.LogonResult;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.util.ProductInfoConstants;

public class TestMMConnection extends TestCase {

	protected static final String STD_DATABASE_NAME         = "QT_Ora9DS"; //$NON-NLS-1$
    protected static final String STD_DATABASE_VERSION      = "1"; //$NON-NLS-1$
    
    static String serverUrl = "jdbc:metamatrix:QT_Ora9DS@mm://localhost:7001;version=1;user=metamatrixadmin;password=mm"; //$NON-NLS-1$

    public TestMMConnection(String name) {
        super(name);
    }
    
    public static MMConnection getMMConnection() {
    	ServerConnection mock = mock(ServerConnection.class);
    	stub(mock.getService(ClientSideDQP.class)).toReturn(mock(ClientSideDQP.class));
    	Properties props = new Properties();
    	props.setProperty(BaseDataSource.VDB_NAME, STD_DATABASE_NAME);
    	props.setProperty(BaseDataSource.VDB_VERSION, STD_DATABASE_VERSION);
    	props.setProperty(BaseDataSource.USER_NAME, "metamatrixadmin"); //$NON-NLS-1$
    	Properties productInfo = new Properties();
    	productInfo.setProperty(ProductInfoConstants.VIRTUAL_DB, STD_DATABASE_NAME);
    	productInfo.setProperty(ProductInfoConstants.VDB_VERSION, STD_DATABASE_VERSION);
    	stub(mock.getLogonResult()).toReturn(new LogonResult(new SessionToken(new MetaMatrixSessionID(1), "metamatrixadmin"), productInfo, "fake")); //$NON-NLS-1$
    	return new MMConnection(mock, props, serverUrl);
    }

    public void testGetMetaData() throws Exception {
        assertNotNull(getMMConnection().getMetaData());
    }

    public void testGetSchema() throws Exception {
        assertEquals("Actual schema is not equql to the expected one. ", STD_DATABASE_NAME, getMMConnection().getSchema()); //$NON-NLS-1$
    }

    public void testNativeSql() throws Exception {
        String sql = "SELECT * FROM BQT1.SmallA"; //$NON-NLS-1$
        assertEquals("Actual schema is not equql to the expected one. ", sql, getMMConnection().nativeSQL(sql)); //$NON-NLS-1$
    }

    /** test getUserName() through DriverManager */
    public void testGetUserName2() throws Exception {        
        assertEquals("Actual userName is not equal to the expected one. ", "metamatrixadmin", getMMConnection().getUserName()); //$NON-NLS-1$ //$NON-NLS-2$
    }
      
    /** test isReadOnly default value on Connection */
    public void testIsReadOnly() throws Exception {
        assertEquals(false, getMMConnection().isReadOnly());
    }

    /** test setReadOnly on Connection */
    public void testSetReadOnly1() throws Exception {
    	MMConnection conn = getMMConnection();
        conn.setReadOnly(true);
        assertEquals(true, conn.isReadOnly());
    }

    /** test setReadOnly on Connection during a transaction */
    public void testSetReadOnly2() throws Exception {
    	MMConnection conn = getMMConnection();
        conn.setAutoCommit(false);
        try {
            conn.setReadOnly(true);
            fail("Error Expected"); //$NON-NLS-1$
        } catch (SQLException e) {
            // error expected
        }
    }
}
