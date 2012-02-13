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

import static org.junit.Assert.*;

import java.sql.Clob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialClob;

import org.junit.Test;

@SuppressWarnings("nls")
public class TestDataTypeTransformer {
	
	@Test public void testClobToStringConversion() throws Exception {
		Clob clob = new SerialClob("foo".toCharArray()); //$NON-NLS-1$
		String value = DataTypeTransformer.getString(clob);
		assertEquals("foo", value); //$NON-NLS-1$
	}
	
	@Test public void testInvalidTransformation() throws Exception {
		try {
			DataTypeTransformer.getDate(Integer.valueOf(1)); 
			fail("exception expected"); //$NON-NLS-1$
		} catch (SQLException e) {
			assertEquals("Unable to transform the column value 1 to a Date.", e.getMessage()); //$NON-NLS-1$
		}
	}
	
	@Test public void testGetDefaultShort() throws Exception {
		assertEquals(0, DataTypeTransformer.getShort(null));
	}
	
	@Test public void testGetDefaultByte() throws Exception {
		assertEquals(0, DataTypeTransformer.getByte(null));
	}
	
	@Test public void testGetString() throws Exception {
		assertEquals("", DataTypeTransformer.getString(new SerialClob(new char[0])));
	}

	
}
