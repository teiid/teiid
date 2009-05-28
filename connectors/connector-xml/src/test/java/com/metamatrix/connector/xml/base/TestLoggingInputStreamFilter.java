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

package com.metamatrix.connector.xml.base;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;

import org.teiid.connector.api.ConnectorLogger;


public class TestLoggingInputStreamFilter extends TestCase {

	LoggingInputStreamFilter filter;
	TestLogger logger;
	private static final String TEST_VAL = "The quick brown fox jumps over the lazy dog";
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		logger = new TestLogger();
		InputStream stream = new ByteArrayInputStream(TEST_VAL.getBytes());
		filter = new LoggingInputStreamFilter(stream, logger);
	}
	
	public void testResetFail() {
		try {
			filter.reset();
		} catch(IOException e) {
			return;
		}
		fail("should have thrown IOException");
	}
	
	public void testsupportsMarkFalse() {
		assertFalse(filter.markSupported());
	}
	
	public void testRead() {
		try {
			boolean finished = false;
			int val;
			StringBuffer buffer = new StringBuffer();
			while (!finished) {
				val = filter.read();
				if(-1 != val) {
					buffer.append(new Character(((char)val)).toString());
				} else {
					finished = true;
				}
			}
			assertEquals(TEST_VAL, buffer.toString());
			assertEquals("XML Connector Framework: response body is: " + TEST_VAL, logger.getMessage());
		} catch(IOException e) {
			fail(e.getMessage());
		}
	}
	
	public void testReadArray() {
		try{
			byte[] res = new byte[TEST_VAL.length() + 1];
			int len = filter.read(res);
			assertEquals(TEST_VAL.length(), len);
			filter.close();
			assertEquals("XML Connector Framework: response body is: " + TEST_VAL, logger.getMessage());
			assertEquals(TEST_VAL, new String(res).trim());
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	public void testReadOffsetA() {
		try{
			byte[] res = new byte[TEST_VAL.length() + 1];
			int len = filter.read(res, 0, 5);
			filter.close();
			assertEquals(5, len);
			assertEquals("XML Connector Framework: response body is: " + "The q", logger.getMessage());
			assertEquals("The q", new String(res).trim());
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	public void testReadOffsetB() {
		try{
			byte[] res = new byte[TEST_VAL.length() + 1];
			int len = filter.read(res, 0, TEST_VAL.length());
			filter.close();
			assertEquals(TEST_VAL.length(), len);
			assertEquals("XML Connector Framework: response body is: " + TEST_VAL,
					logger.getMessage());
			assertEquals(TEST_VAL, new String(res).trim());
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	public void testReadOffsetC() {
		try{
			byte[] res = new byte[TEST_VAL.length() + 1];
			int len = filter.read(res, 0, TEST_VAL.length());
			assertEquals(TEST_VAL.length(), len);
			assertEquals(null, logger.getMessage());
			assertEquals(TEST_VAL, new String(res).trim());
			len = filter.read(res, 0, TEST_VAL.length());
			assertEquals(-1, len);
			assertEquals(TEST_VAL, new String(res).trim());
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSkip() {
			try {
				long count = filter.skip(100);
				assertEquals(TEST_VAL.length(), count);
				assertEquals("XML Connector Framework: response body is: " + TEST_VAL, logger.getMessage());
			} catch (IOException e) {
				fail(e.getMessage());
			}
		
	}

		
	private class TestLogger implements ConnectorLogger {
		
		String logMessage;
		
		public String getMessage() {
			return logMessage;
		}
		
		public void logDetail(String arg0) {
		}

		public void logError(String arg0) {
		}

		public void logError(String arg0, Throwable arg1) {
		}

		public void logInfo(String arg0) {
			logMessage = arg0;
			
		}

		public void logTrace(String arg0) {
		}

		public void logWarning(String arg0) {
		}

		@Override
		public boolean isDetailEnabled() {
			return false;
		}

		@Override
		public boolean isErrorEnabled() {
			return false;
		}

		@Override
		public boolean isInfoEnabled() {
			return false;
		}

		@Override
		public boolean isTraceEnabled() {
			return false;
		}

		@Override
		public boolean isWarningEnabled() {
			return false;
		}

		@Override
		public void logDetail(String message, Throwable error) {
			
		}

		@Override
		public void logInfo(String message, Throwable error) {
			
		}

		@Override
		public void logTrace(String message, Throwable error) {
			
		}

		@Override
		public void logWarning(String message, Throwable error) {
			
		}
		
	}
}
