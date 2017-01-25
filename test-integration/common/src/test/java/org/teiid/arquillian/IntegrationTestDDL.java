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

package org.teiid.arquillian;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.sql.SQLException;

import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestDDL extends AbstractMMQueryTestCase {

	private Admin admin;
	
	@Before
	public void setup() throws Exception {
        admin = AdminFactory.getInstance().createAdmin("localhost",
                AdminUtil.MANAGEMENT_PORT, "admin", "admin".toCharArray());
	}
	
	@After
	public void teardown() throws AdminException {
		AdminUtil.cleanUp(admin);
		admin.close();
	}
	
	@Test
	public void testDDL() throws Exception {
		String ddl = "create database foo version '1';"
				+ "create foreign data wrapper loopback;"
				+ "create server NONE type 'NONE' foreign data wrapper loopback;"
				+ "create schema test server NONE;"
				+ "set schema test;"
				+ "CREATE FOREIGN TABLE G1 (e1 integer PRIMARY KEY, e2 varchar(25), e3 double)";

		this.admin.deploy("foo-vdb.ddl", new ByteArrayInputStream(ddl.getBytes()), false);
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo@mm://localhost:31000;user=user;password=user;autoFailover=true", null);
        
        execute("SELECT * FROM test.G1"); //$NON-NLS-1$
        assertRowCount(1);        
        try {
            execute("SELECT * FROM test.G2"); //$NON-NLS-1$
            fail("should have failed as there is no G2 Table");
        } catch (Exception e) {
        }
        
        // add table
        ddl = ddl + "CREATE FOREIGN TABLE G2 (e1 integer PRIMARY KEY, e2 varchar(25), e3 double)";
        this.admin.deploy("foo-vdb.ddl", new ByteArrayInputStream(ddl.getBytes()), false);
        
        // THIS SHOULD BE REMOVED, AUTOFAILOVER NOT WORKING
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo@mm://localhost:31000;user=user;password=user;autoFailover=true", null);

        // and execute, using the same connection
    	execute("SELECT * FROM test.G2"); //$NON-NLS-1$
        assertRowCount(1);
        printResults();
        closeConnection();
        admin.undeploy("foo-vdb.ddl");
	}

	@Test
	public void testOverrideTranslator() throws Exception {    
		String ddl = "create database foo;"
        + "create foreign data wrapper loopy type loopback OPTIONS(IncrementRows true, RowCount 500);"
        + "create server serverOne type 'NONE' foreign data wrapper loopy;"
        + "create schema test server serverOne;"
        + "set schema test;"
        + "CREATE FOREIGN TABLE G1 (e1 integer PRIMARY KEY, e2 varchar(25), e3 double);";
    
		this.admin.deploy("foo-vdb.ddl", new ByteArrayInputStream(ddl.getBytes()), false);
		
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo@mm://localhost:31000;user=user;password=user", null);
        
        execute("SELECT * FROM test.G1"); //$NON-NLS-1$
        assertRowCount(500);        
        closeConnection();
        admin.undeploy("foo-vdb.ddl");
    }

	@Test
    public void testVDBImport() throws Exception {        
		String ddl = "create database foo;"
		        + "create foreign data wrapper loopy type loopback OPTIONS(IncrementRows true, RowCount 500);"
		        + "create server serverOne type 'NONE' foreign data wrapper loopy;"
		        + "create schema test server serverOne;"
		        + "set schema test;"
		        + "CREATE FOREIGN TABLE G1 (e1 integer PRIMARY KEY, e2 varchar(25), e3 double);";
		String bar =  "create database BAR;"
		        + "IMPORT database foo VERSION '1';";
		    
		this.admin.deploy("foo-vdb.ddl", new ByteArrayInputStream(ddl.getBytes()), false);
		this.admin.deploy("bar-vdb.ddl", new ByteArrayInputStream(bar.getBytes()), false);
		
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:BAR@mm://localhost:31000;user=user;password=user", null);
        
        execute("SELECT * FROM test.G1"); //$NON-NLS-1$
        assertRowCount(500);        
        closeConnection();
        admin.undeploy("bar-vdb.ddl");
        admin.undeploy("foo-vdb.ddl");
    }    
    
	@Test
    public void testUdfClasspath() throws Exception {
        JavaArchive jar = ShrinkWrap.create(JavaArchive.class, "func.jar")
                  .addClasses(SampleFunctions.class);
        admin.deploy("func.jar", jar.as(ZipExporter.class).exportAsInputStream());
        
        String ddl = "create database \"dynamic-func\" OPTIONS(lib 'deployment.func.jar');"
        		+ "CREATE VIRTUAL schema test;"
        		+ "CREATE function func (val string) returns integer "
                + "options (JAVA_CLASS 'org.teiid.arquillian.SampleFunctions',  JAVA_METHOD 'doSomething');";
                
        this.admin.deploy("dynamic-vdb.ddl", new ByteArrayInputStream(ddl.getBytes()), false);
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:dynamic-func@mm://localhost:31000;user=user;password=user", null);
        
        execute("SELECT func('a')"); //$NON-NLS-1$
        assertRowCount(1);
        admin.undeploy("dynamic-vdb.ddl");
        closeConnection();
    }
	
	@Test(expected=SQLException.class)
    public void testDDLOverJDBCNoAuth() throws Exception {	 
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo2@mm://localhost:31000;user=dummy;password=user;autoFailover=true;vdbEdit=true", null);
	}
}
