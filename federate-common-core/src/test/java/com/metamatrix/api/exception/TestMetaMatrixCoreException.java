package com.metamatrix.api.exception;

import java.sql.BatchUpdateException;
import java.sql.SQLException;

import junit.framework.TestCase;

import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.UnitTestUtil;

public class TestMetaMatrixCoreException extends TestCase {
	
	public void testSQLExceptionUnrolling() throws Exception {
		SQLException se = new BatchUpdateException("foo", new int[] {1});
		MetaMatrixCoreException mmce = new MetaMatrixCoreException(se);
		
		mmce = UnitTestUtil.helpSerialize(mmce);
		assertEquals(SQLException.class, mmce.getCause().getClass());
		assertEquals("foo", mmce.getMessage());
		assertEquals("java.sql.BatchUpdateException: foo", mmce.getCause().getMessage());
	}

}
