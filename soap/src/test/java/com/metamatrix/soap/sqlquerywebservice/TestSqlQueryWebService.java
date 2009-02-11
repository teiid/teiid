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

package com.metamatrix.soap.sqlquerywebservice;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.common.api.MMURL;
import com.metamatrix.jdbc.MMConnection;
import com.metamatrix.jdbc.MMStatement;
import com.metamatrix.jdbc.api.ResultSet;
import com.metamatrix.soap.security.Credential;
import com.metamatrix.soap.service.ConnectionSource;
import com.metamatrix.soap.sqlquerywebservice.helper.ConnectionlessRequest;
import com.metamatrix.soap.sqlquerywebservice.helper.CursorType;
import com.metamatrix.soap.sqlquerywebservice.helper.LogInParameters;
import com.metamatrix.soap.sqlquerywebservice.helper.RequestInfo;
import com.metamatrix.soap.sqlquerywebservice.helper.RequestType;
import com.metamatrix.soap.sqlquerywebservice.helper.Results;
import com.metamatrix.soap.sqlquerywebservice.helper.TransactionAutoWrapType;
import com.metamatrix.soap.sqlquerywebservice.service.SqlQueryWebService;
import com.metamatrix.soap.sqlquerywebservice.service.SqlQueryWebServiceFault;
import com.metamatrix.soap.sqlquerywebservice.service.SqlQueryWebService.CredentialProvider;

public class TestSqlQueryWebService extends TestCase {

	public void testBlockingExecute_NullLogInParameters() throws SqlQueryWebServiceFault {
		SqlQueryWebService sqws = new SqlQueryWebService();
		setFakeCredentialProvider(sqws);
		sqws.setConnectionSource(new ConnectionSource() {
			@Override
			public Connection getConnection(Properties connectionProperties)
					throws SQLException {
				assertNull(connectionProperties.getProperty(MMURL.CONNECTION.SERVER_URL));
				throw new SQLException("expected");
			}
		});
		
		ConnectionlessRequest connectionlessRequest = new ConnectionlessRequest();
		connectionlessRequest.setParameters(new LogInParameters());
		
		try {
			sqws.executeBlocking(connectionlessRequest);
			fail("exception expected");
		} catch (SqlQueryWebServiceFault f) {
			//this will be thrown after we have setup the initial connection properties
			assertEquals("expected", f.getMessage());
		}
	}
	
	public void testBlockingExecute_BadConnection() throws SqlQueryWebServiceFault {
		SqlQueryWebService sqws = new SqlQueryWebService();
		
		sqws.setConnectionSource(new ConnectionSource() {
			
			@Override
			public Connection getConnection(Properties connectionProperties)
					throws SQLException {
				throw new SQLException("expected");
			}
		});
		
		setFakeCredentialProvider(sqws);
		
		ConnectionlessRequest connectionlessRequest = new ConnectionlessRequest();
		connectionlessRequest.setParameters(new LogInParameters());
				
		try {
			sqws.executeBlocking(connectionlessRequest);
			fail("exception expected");
		} catch (SqlQueryWebServiceFault f) {
			assertEquals("expected", f.getMessage());
			assertEquals("Client", f.getFaultCode().getLocalPart());
		}
	}

	private void setFakeCredentialProvider(SqlQueryWebService sqws) {
		sqws.setCredentialProvider(new CredentialProvider() {
			@Override
			public Credential getCredentials() throws SqlQueryWebServiceFault {
				return new Credential("ted", "jones".getBytes());
			}
		});
	}
	
	public void testBlockingExecute() throws SqlQueryWebServiceFault {
		SqlQueryWebService sqws = new SqlQueryWebService();
		setFakeCredentialProvider(sqws);
		
		sqws.setConnectionSource(new ConnectionSource() {
			@Override
			public Connection getConnection(Properties connectionProperties)
					throws SQLException {
				assertEquals("ted", connectionProperties.getProperty(MMURL.CONNECTION.USER_NAME));
				MMConnection conn = Mockito.mock(MMConnection.class);
				MMStatement stmt = Mockito.mock(MMStatement.class);
				Mockito.stub(stmt.unwrap(MMStatement.class)).toReturn(stmt);
				Mockito.stub(stmt.getUpdateCount()).toReturn(1);
				Mockito.stub(conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)).toReturn(stmt);
				return conn;
			}
		});
		
		ConnectionlessRequest connectionlessRequest = new ConnectionlessRequest();
		connectionlessRequest.setParameters(new LogInParameters());
		RequestInfo ri = new RequestInfo();
		ri.setCursorType(new CursorType(String.valueOf(ResultSet.TYPE_FORWARD_ONLY)));
		ri.setRequestType(new RequestType(String.valueOf(0)));
		ri.setTransactionAutoWrapMode(new TransactionAutoWrapType(String.valueOf(0)));
		
		connectionlessRequest.setRequestInfo(ri);
				
		Results results = sqws.executeBlocking(connectionlessRequest);
		
		assertFalse(results.isHasData());
		assertEquals(new Integer(1), results.getUpdateCount());
	}
	
}
