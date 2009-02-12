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

import java.io.IOException;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.SQLException;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.ProcedureErrorInstructionException;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.jdbc.api.SQLStates;

public class TestMMSQLException extends TestCase {
  
	/*
	 * Test method for 'com.metamatrix.jdbc.MMSQLException.MMSQLException()'
	 */
	public void testMMSQLException() {
		MMSQLException e = new MMSQLException();
		String sqlState = e.getSQLState();
		Throwable cause = e.getCause();
		int errorCode = e.getErrorCode();
		Throwable nestedException = e.getCause();
		SQLException nextException = e.getNextException();

		assertTrue(
				"Expected MMSQLException.getSQLState() to return <null> but got \"" //$NON-NLS-1$
						+ sqlState + "\" instead.", sqlState == null); //$NON-NLS-1$
		assertTrue(
				"Expected MMSQLException.getCause() to return <null> but got [" //$NON-NLS-1$
						+ (cause != null ? cause.getClass().getName()
								: "<unknown>") + "] instead.", cause == null); //$NON-NLS-1$ //$NON-NLS-2$
		assertTrue(
				"Expected MMSQLException.getErrorCode() to return [0] but got [" //$NON-NLS-1$
						+ errorCode + "] instead.", errorCode == 0); //$NON-NLS-1$
		assertTrue(
				"Expected MMSQLException.getNestedException() to return <null> but got [" //$NON-NLS-1$
						+ (nestedException != null ? nestedException.getClass()
								.getName() : "<unknown>") + "] instead.", //$NON-NLS-1$ //$NON-NLS-2$
				nestedException == null);
		assertTrue(
				"Expected MMSQLException.getNextException() to return <null> but got a SQLException with message \"" //$NON-NLS-1$
						+ (nextException != null ? nextException.getMessage()
								: "") + "\" instead.", nextException == null); //$NON-NLS-1$ //$NON-NLS-2$
	}

	/*
	 * Test method for 'com.metamatrix.jdbc.MMSQLException.create(Throwable)'
	 * 
	 * Tests various simple exceptions to see if the expected SQLState is
	 * returend.
	 */
	public void testCreateThrowable_01() {
		testCreateThrowable(new CommunicationException(
				"A test MM Communication Exception"), //$NON-NLS-1$
				SQLStates.CONNECTION_EXCEPTION_STALE_CONNECTION);
		testCreateThrowable(
				new ConnectException("A test connection attempt exception"), //$NON-NLS-1$
				SQLStates.CONNECTION_EXCEPTION_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION);
		testCreateThrowable(
				new ConnectionException("A test MM Connection Exception"), //$NON-NLS-1$
				SQLStates.CONNECTION_EXCEPTION_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION);
		testCreateThrowable(new IOException(
				"A test Generic java.io.IOException"), //$NON-NLS-1$
				SQLStates.CONNECTION_EXCEPTION_STALE_CONNECTION);
		testCreateThrowable(
				new MalformedURLException(
						"A test java.net.MalformedURLException"), //$NON-NLS-1$
				SQLStates.CONNECTION_EXCEPTION_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION);
		testCreateThrowable(new MetaMatrixCoreException(
				"A test Generic MM Core Exception"), SQLStates.DEFAULT); //$NON-NLS-1$
		testCreateThrowable(new MetaMatrixException("A test MM Exception"), //$NON-NLS-1$
				SQLStates.DEFAULT);
		testCreateThrowable(new MetaMatrixProcessingException(
				"A test Generic MM Query Processing Exception"), //$NON-NLS-1$
				SQLStates.USAGE_ERROR);
		testCreateThrowable(new MetaMatrixRuntimeException(
				"A test MM Runtime Exception"), SQLStates.DEFAULT); //$NON-NLS-1$
		testCreateThrowable(new MMSQLException(
				"A test Generic MM SQL Exception"), SQLStates.DEFAULT); //$NON-NLS-1$
		testCreateThrowable(
				new NoRouteToHostException(
						"A test java.net.NoRouteToHostException"), //$NON-NLS-1$
				SQLStates.CONNECTION_EXCEPTION_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION);
		testCreateThrowable(new NullPointerException("A test NPE"), //$NON-NLS-1$
				SQLStates.DEFAULT);
		testCreateThrowable(new ProcedureErrorInstructionException(
				"A test SQL Procedure Error exception"), //$NON-NLS-1$
				SQLStates.VIRTUAL_PROCEDURE_ERROR);
		testCreateThrowable(new SocketTimeoutException(
				"A test socket timeout exception"), //$NON-NLS-1$
				SQLStates.CONNECTION_EXCEPTION_STALE_CONNECTION);
		testCreateThrowable(
				new UnknownHostException("A test connection attempt exception"), //$NON-NLS-1$
				SQLStates.CONNECTION_EXCEPTION_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION);
	}

	/*
	 * Test method for 'com.metamatrix.jdbc.MMSQLException.create(Throwable)'
	 * 
	 * Tests various nested exceptions to see if the expected SQLState is
	 * returend.
	 */
	public void testCreateThrowable_02() {
		testCreateThrowable(
				new CommunicationException(new ConnectException(
						"A test java.net.ConnectException"), //$NON-NLS-1$
						"Test Communication Exception with a ConnectException in it"), //$NON-NLS-1$
				SQLStates.CONNECTION_EXCEPTION_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION);
		testCreateThrowable(new CommunicationException(new SocketException(
				"A test java.net.SocketException"), //$NON-NLS-1$
				"Test Communication Exception with a SocketException in it"), //$NON-NLS-1$
				SQLStates.CONNECTION_EXCEPTION_STALE_CONNECTION);
		testCreateThrowable(
				new MetaMatrixException(new SocketTimeoutException(
						"A test java.net.SocketTimeoutException"), //$NON-NLS-1$
						"Test MetaMatrixException with a SocketTimeoutException in it"), //$NON-NLS-1$
				SQLStates.CONNECTION_EXCEPTION_STALE_CONNECTION);
	}
    
    public void testCreateThrowable3() {
        MMSQLException e = testCreateThrowable(
                            new MetaMatrixCoreException(
                                    new MetaMatrixRuntimeException(
                                            new SocketTimeoutException(
                                                    "A test MM Invalid Session Exception"), //$NON-NLS-1$
                                            "Test MetaMatrixRuntimeException with a InvalidSessionException in it"), //$NON-NLS-1$
                                    "Test MM Core Exception with an MM Runtime Exception in it and an InvalidSessionException nested within"), //$NON-NLS-1$
                            SQLStates.CONNECTION_EXCEPTION_STALE_CONNECTION);
        
        //test to ensure that wrapping mmsqlexceptions works
        MMSQLException e1 = MMSQLException.create(e, "new message"); //$NON-NLS-1$
        assertEquals("new message", e1.getMessage()); //$NON-NLS-1$
        testCreateThrowable(((MMSQLException)e1.getCause()).getCause(), SQLStates.CONNECTION_EXCEPTION_STALE_CONNECTION);
    }

	/*
	 * Helper method to test SQLState and general MMSQLException validation
	 */
	private MMSQLException testCreateThrowable(Throwable ecause, String esqlState) {
		MMSQLException e = MMSQLException.create(ecause);
		if (ecause.getClass() == MMSQLException.class) {
            ecause = null;
		}
		String sqlState = e.getSQLState();
		Throwable cause = e.getCause();
		int errorCode = e.getErrorCode();
		Throwable nestedException = e.getCause();
		SQLException nextException = e.getNextException();

		assertTrue("Expected MMSQLException.getSQLState() to return \"" //$NON-NLS-1$
				+ esqlState + "\" but got \"" + sqlState + "\" instead.", //$NON-NLS-1$ //$NON-NLS-2$
				sqlState.compareTo(esqlState) == 0);
		assertTrue("Expected MMSQLException.getCause() to return [" //$NON-NLS-1$
				+ (ecause != null ? ecause.getClass().getName() : "<null>") //$NON-NLS-1$
				+ "] but got [" //$NON-NLS-1$
				+ (cause != null ? cause.getClass().getName() : "<unknown>") //$NON-NLS-1$
				+ "] instead.", cause == ecause); //$NON-NLS-1$
		assertTrue(
				"Expected MMSQLException.getErrorCode() to return [0] but got [" //$NON-NLS-1$
						+ errorCode + "] instead.", errorCode == 0); //$NON-NLS-1$
		assertTrue("Expected MMSQLException.getNestedException() to return [" //$NON-NLS-1$
				+ (ecause != null ? ecause.getClass().getName() : "<null>") //$NON-NLS-1$
				+ "] but got [" //$NON-NLS-1$
				+ (nestedException != null ? nestedException.getClass()
						.getName() : "<unknown>") + "] instead.", //$NON-NLS-1$ //$NON-NLS-2$
				nestedException == ecause);
		assertTrue(
				"Expected MMSQLException.getNextException() to return <null> but got a SQLException with message \"" //$NON-NLS-1$
						+ (nextException != null ? nextException.getMessage()
								: "") + "\" instead.", nextException == null); //$NON-NLS-1$ //$NON-NLS-2$
		return e;
    }
    
    public void testCreate() {
        MMSQLException exception = MMSQLException.create(new Exception());
        
        assertEquals(exception.getMessage(), Exception.class.getName());
        assertNotNull(exception.getSQLState());
        assertEquals(exception.getSQLState(), "38000"); //$NON-NLS-1$
        
        assertEquals(exception, MMSQLException.create(exception));
    }
    
    public void testCreateFromSQLException() {
        SQLException sqlexception = new SQLException("foo", "21"); //$NON-NLS-1$ //$NON-NLS-2$
        
        SQLException nested = new SQLException("bar"); //$NON-NLS-1$
        
        sqlexception.setNextException(nested);
        
        String message = "top level message"; //$NON-NLS-1$
        
        MMSQLException exception = MMSQLException.create(sqlexception, message);
        
        assertEquals(exception.getMessage(), message);
        assertEquals(exception.getSQLState(), sqlexception.getSQLState());        
        assertEquals(exception.getNextException().getMessage(), sqlexception.getMessage());
        assertEquals(exception.getNextException().getNextException().getMessage(), nested.getMessage());
    }

}
