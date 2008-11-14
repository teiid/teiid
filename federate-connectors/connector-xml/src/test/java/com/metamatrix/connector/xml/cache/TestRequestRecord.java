/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.cache;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.connector.xml.CachingConnector;
import com.metamatrix.connector.xml.MockCachingConnector;
import com.metamatrix.data.api.ConnectorLogger;

public class TestRequestRecord extends TestCase {

	ConnectorLogger logger = new SysLogger();
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
