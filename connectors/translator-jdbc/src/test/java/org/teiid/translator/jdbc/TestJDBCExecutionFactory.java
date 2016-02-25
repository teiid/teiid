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
package org.teiid.translator.jdbc;

import static org.junit.Assert.*;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCExecutionFactory.StructRetrieval;

@SuppressWarnings("nls")
public class TestJDBCExecutionFactory {

	@Test public void testDatabaseCalender() throws Exception {
		final JDBCExecutionFactory jef = new JDBCExecutionFactory();
		jef.setDatabaseTimeZone("GMT"); //$NON-NLS-1$
		jef.start();
		
		final Calendar[] cals = new Calendar[2];
		
		Thread t1 = new Thread() {
			public void run() {
				cals[0] = jef.getDatabaseCalendar();
			}
		};
		t1.start();
		
		Thread t2 = new Thread() {
			public void run() {
				cals[1] = jef.getDatabaseCalendar();
			}
		};
		t2.start();
		t1.join();
		t2.join();
		
		assertNotSame(cals[0], cals[1]);
	}
	
	@Test public void testVersion() {
		JDBCExecutionFactory jef = new JDBCExecutionFactory();
		jef.setDatabaseVersion("Some db 1.2.3 (some build)");
		assertEquals("1.2.3", jef.getDatabaseVersion().toString());
		assertEquals(new Version(new Integer[] {1, 2, 3}), jef.getVersion());
		
		Version version = Version.getVersion("10.0");
		assertTrue(version.compareTo(Version.getVersion("9.1")) > 0);
		assertTrue(version.compareTo(Version.getVersion("10.0.1")) < 0);
	}
	
	@Test public void testStructRetrival() throws SQLException {
		JDBCExecutionFactory jef = new JDBCExecutionFactory();
		jef.setStructRetrieval(StructRetrieval.ARRAY);
		ResultSet rs = Mockito.mock(ResultSet.class);
		Struct s = Mockito.mock(Struct.class);
		Mockito.stub(rs.getObject(1)).toReturn(s);
		assertTrue(jef.retrieveValue(rs, 1, TypeFacility.RUNTIME_TYPES.OBJECT) instanceof Array);
	}
	
	@Test public void testBooleanRetrival() throws SQLException {
		JDBCExecutionFactory jef = new JDBCExecutionFactory();
		ResultSet rs = Mockito.mock(ResultSet.class);
		Mockito.stub(rs.getBoolean(1)).toReturn(false);
		Mockito.stub(rs.wasNull()).toReturn(true);
		assertNull(jef.retrieveValue(rs, 1, TypeFacility.RUNTIME_TYPES.BOOLEAN));
	}
	
	@Test public void testLiteralWithDatabaseTimezone() throws TranslatorException {
		TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT"));
		try {
			JDBCExecutionFactory jef = new JDBCExecutionFactory();
			jef.setDatabaseTimeZone("GMT+1");
			jef.start();
			assertEquals("2015-02-03 05:00:00.0", jef.formatDateValue(TimestampUtil.createTimestamp(115, 1, 3, 4, 0, 0, 0)));
		} finally {
			TimestampWithTimezone.resetCalendar(null);
		}
	}
	
	@Test public void testInitCaps() throws Exception {
		JDBCExecutionFactory jef = new JDBCExecutionFactory();
		Connection connection = Mockito.mock(Connection.class);
		DatabaseMetaData mock = Mockito.mock(DatabaseMetaData.class);
		Mockito.stub(connection.getMetaData()).toReturn(mock);
		Mockito.stub(mock.supportsGetGeneratedKeys()).toThrow(new SQLException());
		//should still succeed even if an exception is thrown from supportsGetGeneratedKeys
		jef.initCapabilities(connection);
	}
}
