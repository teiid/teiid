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

package com.metamatrix.connector.xml.cache;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.xml.CachingConnector;
import com.metamatrix.connector.xml.MockCachingConnector;

public class TestRequestRecord extends TestCase {

	ConnectorLogger logger = new SysLogger(false);
	CachingConnector mockConnector = new MockCachingConnector();
	
	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testRequestRecord() {
		RequestRecord testRecord;
		try {
			testRecord = new RequestRecord("test", mockConnector);
			assertTrue(testRecord.isEmpty());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testRequestRecordNullID() {
		RequestRecord testRecord;
		try {
			testRecord = new RequestRecord(null, mockConnector);
			assertTrue(testRecord.isEmpty());
		} catch (Exception e) {
			assertEquals("RequestRecord: RequestIDs cannot be null", e.getMessage());
		}
	}

	public void testRequestRecordNullConn() {
		RequestRecord testRecord;
		try {
			testRecord = new RequestRecord("test", null);
			assertTrue(testRecord.isEmpty());
		} catch (Exception e) {
			assertEquals("RequestRecord: The CachingConnector parameter cannot be null", e.getMessage());
		}
	}
	
	public void testAddRequestPart() {
		RequestRecord testRecord;
		try {
			testRecord = new RequestRecord("test", mockConnector);
			testRecord.addRequestPart("partID", "executionID", "sourceRequestID", "cacheKey", logger);
			assertFalse(testRecord.isEmpty());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void testAddDuplicateRequestPart() {
		RequestRecord testRecord;
		try {
			testRecord = new RequestRecord("test", mockConnector);
			testRecord.addRequestPart("partID", "executionID", "sourceRequestID", "cacheKey", logger);
			testRecord.addRequestPart("partID", "executionID", "sourceRequestID", "cacheKey", logger);
		} catch (Exception e) {
			assertEquals("Error - Cannot add a CacheRecord to a RequestPartRecord with an existing key", e.getMessage());
		}
	}
	
	public void testDeleteAllRequestParts() {
		try {
			RequestRecord testRecord = new RequestRecord("test", mockConnector);
			testRecord.addRequestPart("partID", "executionID", "sourceRequestID", "cacheKey", logger);
			testRecord.deleteRequestPart("partID", "executionID", logger);
			assertTrue(testRecord.isEmpty());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testDeleteOneRequestPart() {
		try {
			RequestRecord testRecord = new RequestRecord("test", mockConnector);
			testRecord.addRequestPart("partID", "executionID", "sourceRequestID", "cacheKey", logger);
			testRecord.addRequestPart("partID2", "executionID", "sourceRequestID2", "cacheKey2", logger);
			testRecord.deleteRequestPart("partID", "executionID", logger);
			assertFalse(testRecord.isEmpty());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}	

	public void testDeleteMultipleSourceRequest() {
		try {
			RequestRecord testRecord = new RequestRecord("test", mockConnector);
			testRecord.addRequestPart("partID", "executionID", "sourceRequestID", "cacheKey", logger);
			testRecord.addRequestPart("partID", "executionID", "sourceRequestID2", "cacheKey2", logger);
			testRecord.deleteRequestPart("partID", "executionID", logger);
			assertTrue(testRecord.isEmpty());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testIsEmptyTrue() {
		RequestRecord testRecord;
		try {
			testRecord = new RequestRecord("test", mockConnector);
			assertTrue(testRecord.isEmpty());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void testIsEmptyFalse() {
		RequestRecord testRecord;
		try {
			testRecord = new RequestRecord("test", mockConnector);
			testRecord.addRequestPart("partID", "sourceRequestID", "executionID", "cacheKey", logger);
			assertFalse(testRecord.isEmpty());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
}
