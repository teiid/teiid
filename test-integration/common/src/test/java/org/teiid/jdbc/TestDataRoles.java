/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.teiid.deployers.VDBRepository;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.MaterializationManager;

@SuppressWarnings("nls")
public class TestDataRoles {

	private static final class ExtendedEmbeddedServer extends EmbeddedServer {
		@Override
		public MaterializationManager getMaterializationManager() {
			return super.getMaterializationManager();
		}
		
		@Override
		public VDBRepository getVDBRepository() {
			return super.getVDBRepository();
		}
	}

	private ExtendedEmbeddedServer es;
	
	@After public void tearDown() {
		es.stop();
	}

	@Test public void testMaterializationWithSecurity() throws Exception {
		es = new ExtendedEmbeddedServer();
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		es.start(ec);
		es.deployVDB(new ByteArrayInputStream(new String("<vdb name=\"role-1\" version=\"1\">"
				+ "<model name=\"myschema\" type=\"virtual\">"
				+ "<metadata type = \"DDL\"><![CDATA[CREATE VIEW vw as select 'a' as col;]]></metadata></model>"
				+ "<data-role name=\"y\" any-authenticated=\"true\"/></vdb>").getBytes()));
    	Connection c = es.getDriver().connect("jdbc:teiid:role-1", null);
    	Statement s = c.createStatement();
    	
    	try {
    		s.execute("select * from vw");
    		Assert.fail();
    	} catch (SQLException e) {
    		//not authorized
    	}
    	es.getMaterializationManager().executeQuery(es.getVDBRepository().getLiveVDB("role-1"), "select * from vw");
	}
	
	@Test public void testExecuteImmediate() throws Exception {
		es = new ExtendedEmbeddedServer();
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		es.start(ec);
		es.deployVDB(new ByteArrayInputStream(new String("<vdb name=\"role-1\" version=\"1\">"
				+ "<model name=\"myschema\" type=\"virtual\">"
				+ "<metadata type = \"DDL\"><![CDATA[CREATE VIEW vw as select 'a' as col;]]></metadata></model>"
				+ "<data-role name=\"y\" any-authenticated=\"true\"/></vdb>").getBytes()));
    	Connection c = es.getDriver().connect("jdbc:teiid:role-1", null);
    	Statement s = c.createStatement();
    	s.execute("set autoCommitTxn off");
    	
    	try {
    		s.execute("begin execute immediate 'select * from vw'; end");
    		fail();
    	} catch (TeiidSQLException e) {
    		
    	}
    	
    	//should be valid
    	s.execute("begin execute immediate 'select 1'; end");
    	
    	//no temp permission
    	try {
    		s.execute("begin execute immediate 'select 1' as x integer into #temp; end");
    		fail();
    	} catch (TeiidSQLException e) {
    		
    	}
    	
    	//nested should not pass either
    	try {
	    	s.execute("begin execute immediate 'begin execute immediate ''select * from vw''; end'; end");
	    	fail();
    	} catch (TeiidSQLException e) {
    		
    	}
	}
	
	@Test public void testMetadataWithSecurity() throws Exception {
		es = new ExtendedEmbeddedServer();
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		es.start(ec);
		es.deployVDB(new ByteArrayInputStream(new String("<vdb name=\"role-1\" version=\"1\">"
				+ "<model name=\"s1\" type=\"virtual\">"
				+ "<metadata type = \"DDL\"><![CDATA["
				+ "CREATE VIEW t2 (col string primary key) options (x 'y') as select 'a' as col;\n"
				+ "CREATE VIEW t1 (col string, col_hidden string, primary key (col, col_hidden), foreign key (col) references t2) options (x 'y1') as select col, 'b' as col_hidden from t2;\n"
				+ "CREATE virtual procedure proc1 (param1 string) returns table (proc_col string) as begin end;\n"
				+ "]]></metadata></model>"
				+ "<model name=\"s2\" type=\"virtual\">"
				+ "<metadata type = \"DDL\"><![CDATA["
				+ "CREATE VIEW t3 as select 'a' as col, 'b' as col_hidden;\n"
				+ "CREATE virtual procedure proc2 (param2 string) returns table (proc_col string) as begin end;\n"
				+ "]]></metadata></model>"
				+ "<data-role name=\"y\" any-authenticated=\"true\">"
				+ "<permission><resource-name>s1</resource-name><allow-read>true</allow-read></permission>"
				+ "<permission><resource-name>s1.t1.col_hidden</resource-name><allow-read>false</allow-read></permission>"
				+ "<permission><resource-name>s1.t2</resource-name><allow-read>false</allow-read></permission>"
				+ "<permission><resource-name>sysadmin</resource-name><allow-read>true</allow-read></permission>"
				+ "</data-role></vdb>").getBytes()));
		
    	Connection c = es.getDriver().connect("jdbc:teiid:role-1", null);
    	Statement s = c.createStatement();
    	
    	ResultSet rs = s.executeQuery("select * from sys.tables where name like 't_'");
    	//only t1 should be visible
    	assertTrue(rs.next());
    	assertEquals("t1", rs.getString("name"));
    	assertFalse(rs.next());
    	
    	rs = s.executeQuery("select * from sys.schemas where name like 's_' order by name");
    	//both are visible for now
    	assertTrue(rs.next());
    	assertEquals("s1", rs.getString("name"));
    	assertTrue(rs.next());
    	assertEquals("s2", rs.getString("name"));
    	assertFalse(rs.next());
    	
    	rs = s.executeQuery("select * from sys.columns where tablename like 't_'");
    	//only col should be visible
    	assertTrue(rs.next());
    	assertEquals("col", rs.getString("name"));
    	assertFalse(rs.next());
    	
    	rs = s.executeQuery("select * from sys.procedures where name like 'proc_'");
    	//only proc1 should be visible
    	assertTrue(rs.next());
    	assertEquals("proc1", rs.getString("name"));
    	assertFalse(rs.next());
    	
    	rs = s.executeQuery("select * from sys.procedureparams where procedurename like 'proc_'");
    	//only proc1 should be visible
    	assertTrue(rs.next());
    	assertEquals("param1", rs.getString("name"));
    	assertTrue(rs.next());
    	assertEquals("proc_col", rs.getString("name"));
    	assertFalse(rs.next());
    	    	
    	rs = s.executeQuery("select * from sysadmin.usage where schemaname like 's_'");
    	//nothing should be visible
    	assertFalse(rs.next());
    	
    	rs = s.executeQuery("select * from sys.properties where name = 'x'");
    	assertTrue(rs.next());
    	assertEquals("y1", rs.getString("Value"));
    	assertFalse(rs.next());
    	
    	rs = s.executeQuery("select * from sys.keycolumns where tablename = 't1'");
    	//nothing should be visible
    	assertFalse(rs.next());
    	
    	rs = s.executeQuery("select * from sys.keys where tablename = 't1'");
    	//nothing should be visible
    	assertFalse(rs.next());
	}
	
}
