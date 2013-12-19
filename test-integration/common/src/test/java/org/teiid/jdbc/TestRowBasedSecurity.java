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
import java.security.Identity;
import java.security.Principal;
import java.security.acl.Group;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Vector;

import javax.security.auth.Subject;

import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.Type;
import org.teiid.runtime.DoNothingSecurityHelper;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.HardCodedExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

@SuppressWarnings("nls")
public class TestRowBasedSecurity {

	private EmbeddedServer es;
	
	@After public void tearDown() {
		es.stop();
	}

	@Test public void testSecurity() throws Exception {
		es = new EmbeddedServer();
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		final Vector<Principal> v = new Vector<Principal>();
		v.add(new Identity("myrole") {});
		ec.setSecurityHelper(new DoNothingSecurityHelper() {
			@Override
			public Subject getSubjectInContext(String securityDomain) {
				Subject s = new Subject();
				Group g = Mockito.mock(Group.class);
				Mockito.stub(g.getName()).toReturn("Roles");
				Mockito.stub(g.members()).toReturn((Enumeration) v.elements());
				s.getPrincipals().add(g);
				return s;
			}
		});
		es.start(ec);
		HardCodedExecutionFactory hcef = new HardCodedExecutionFactory() {
			public void getMetadata(MetadataFactory metadataFactory, Object conn) throws TranslatorException {
				Table t = metadataFactory.addTable("x");
				Column col = metadataFactory.addColumn("col", TypeFacility.RUNTIME_NAMES.STRING, t);
				metadataFactory.addColumn("col2", TypeFacility.RUNTIME_NAMES.STRING, t);
				metadataFactory.addPermission("y", t, null, null, Boolean.TRUE, null, null, null, "col = 'a'", null);
				metadataFactory.addColumnPermission("y", col, null, null, null, null, "null", null);
				
				t = metadataFactory.addTable("y");
				col = metadataFactory.addColumn("col", TypeFacility.RUNTIME_NAMES.STRING, t);
				metadataFactory.addColumn("col2", TypeFacility.RUNTIME_NAMES.STRING, t);
				metadataFactory.addPermission("z", t, null, null, null, null, null, null, "col = 'e'", null);
				
				Table v = metadataFactory.addTable("v");
				metadataFactory.addPermission("y", v, null, null, Boolean.TRUE, null, null, null, null, null);
				col = metadataFactory.addColumn("col", TypeFacility.RUNTIME_NAMES.STRING, v);
				metadataFactory.addColumn("col2", TypeFacility.RUNTIME_NAMES.STRING, v);
				v.setTableType(Type.View);
				v.setVirtual(true);
				v.setSelectTransformation("/*+ cache(scope:session) */ select col, col2 from y");
			}
			@Override
			public boolean isSourceRequiredForMetadata() {
				return false;
			}
		};
		hcef.addData("SELECT x.col, x.col2 FROM x", Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("c", "d")));
		hcef.addData("SELECT y.col, y.col2 FROM y", Arrays.asList(Arrays.asList("e", "f"), Arrays.asList("h", "g")));
		es.addTranslator("hc", hcef);
		es.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("roles-vdb.xml")));
		
		Connection c = es.getDriver().connect("jdbc:teiid:z", null);
		Statement s = c.createStatement();
		ResultSet rs = s.executeQuery("select * from x");
		rs.next();
		assertEquals(null, rs.getString(1)); //masking
		assertEquals("b", rs.getString(2));
		assertFalse(rs.next()); //row filter
		rs.close();
		
		s = c.createStatement();
		rs = s.executeQuery("select lookup('myschema.x', 'col', 'col2', 'b')");
		rs.next();
		assertEquals(null, rs.getString(1)); //global scoped
		
		s = c.createStatement();
		rs = s.executeQuery("select count(col2) from v where col is not null");
		rs.next();
		assertEquals(1, rs.getInt(1));
		
		//different session with different roles
		v.clear();
		c = es.getDriver().connect("jdbc:teiid:z", null);
		s = c.createStatement();
		rs = s.executeQuery("select count(col2) from v where col is not null");
		rs.next();
		assertEquals(2, rs.getInt(1));
	}
	
}
