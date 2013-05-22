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

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

@SuppressWarnings("nls")
public class TestRowBasedSecurity {

	@Test public void testSecurity() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		HardCodedExecutionFactory hcef = new HardCodedExecutionFactory() {
			public void getMetadata(MetadataFactory metadataFactory, Object conn) throws TranslatorException {
				Table t = metadataFactory.addTable("x");
				Column col = metadataFactory.addColumn("col", TypeFacility.RUNTIME_NAMES.STRING, t);
				metadataFactory.addColumn("col2", TypeFacility.RUNTIME_NAMES.STRING, t);
				metadataFactory.addPermission("y", t, null, null, null, null, null, null, "col = 'a'", null);
				metadataFactory.addColumnPermission("y", col, null, null, null, null, "null", null);
			}
			@Override
			public boolean isSourceRequiredForMetadata() {
				return false;
			}
		};
		hcef.addData("SELECT x.col, x.col2 FROM x", Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("c", "d")));
		es.addTranslator("hc", hcef);
		es.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("roles-vdb.xml")));
		
		Connection c = es.getDriver().connect("jdbc:teiid:z", null);
		Statement s = c.createStatement();
		ResultSet rs = s.executeQuery("select * from x");
		rs.next();
		assertEquals(null, rs.getString(1)); //masking
		assertEquals("b", rs.getString(2));
		assertFalse(rs.next()); //row filter
		es.stop();
	}
	
}
