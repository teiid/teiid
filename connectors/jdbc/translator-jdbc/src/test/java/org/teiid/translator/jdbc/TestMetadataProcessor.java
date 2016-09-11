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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.SystemMetadata;

@SuppressWarnings("nls")
public class TestMetadataProcessor {

	@Test public void testInvalidIndex() throws SQLException {
		JDBCMetdataProcessor processor = new JDBCMetdataProcessor();
		processor.setImportIndexes(true);
		processor.setWidenUnsingedTypes(false);
		MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
		DatabaseMetaData dmd = Mockito.mock(DatabaseMetaData.class);
		Table t = mf.addTable("c");
		JDBCMetdataProcessor.TableInfo ti = new JDBCMetdataProcessor.TableInfo("a", "b", "c", t);
		
		ResultSet rs = Mockito.mock(ResultSet.class);
		Mockito.stub(rs.next()).toAnswer(new Answer<Boolean>() {
			int count = 0;
			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				if (count++ == 0) {
					return true;
				}
				return false;
			}
		});
		//intentionally leave the column name null

		Mockito.stub(rs.getShort(7)).toReturn(DatabaseMetaData.tableIndexOther);
		Mockito.stub(dmd.getIndexInfo("a", "b", "c", false, true)).toReturn(rs);
		
		processor.getIndexes(mf, dmd, Arrays.asList(ti), false);
		Mockito.verify(rs).getString(9);
		assertTrue(t.getIndexes().isEmpty());
	}
	
	/**
	 * JDBC says to return an empty string, but some sources return null and we need to handle the null case anyways
	 */
	@Test public void testQuoteStringNull() {
		JDBCMetdataProcessor jmp = new JDBCMetdataProcessor();
		assertEquals("x", jmp.quoteName("x"));
	}
	
}
