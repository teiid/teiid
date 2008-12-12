package com.metamatrix.jdbc;

import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;

import com.metamatrix.common.types.ClobImpl;

import junit.framework.TestCase;

public class TestDataTypeTransformer extends TestCase {
	
	public void testClobToStringConversion() throws Exception {
		Clob clob = new ClobImpl("foo".toCharArray()); //$NON-NLS-1$
		String value = DataTypeTransformer.transform(clob, String.class, "String"); //$NON-NLS-1$
		assertEquals("foo", value); //$NON-NLS-1$
	}
	
	public void testInvalidTransformation() throws Exception {
		try {
			DataTypeTransformer.transform(new Integer(1), Date.class, "Date"); //$NON-NLS-1$
			fail("exception expected"); //$NON-NLS-1$
		} catch (SQLException e) {
			assertEquals("Unable to transform the column value 1 to a Date.", e.getMessage()); //$NON-NLS-1$
		}
	}

}
