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
import org.teiid.client.security.InvalidSessionException;
import org.teiid.core.TeiidProcessingException;
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
	
    public boolean execute(String sql) throws SQLException {
        try {
			return super.execute(sql, new Object[] {});
		} catch (SQLException e) {
//			if (e.getCause() instanceof InvalidSessionException){
//				e.printStackTrace();
//				System.out.println("********************exception swallowed IVS");
//				return false;
//			}
//			if (e.getCause() instanceof TeiidProcessingException && (e.getCause().getMessage().contains("TEIID30563"))) {
//				e.printStackTrace();
//				System.out.println("********************exception swallowed Cancell stmt");
//				return false;
//			}
			throw e;
		}
    }	
	
	/**
	 *  Test covers
     *  1) usage of Admin API to create the DB
     *  2) usage of DDL
     *  3) executing the created DB
     *  4) Modifying the existing DB 
     *  5) reusing the existing connection to issue the call to verify results
	 * @throws Exception
	 */
	@Test
	public void testDDL() throws Exception {	    
		admin.executeDDL(null, null, null, "create database foo", false);
		admin.executeDDL("foo", "1", null, "create foreign data wrapper loopback", false);
		admin.executeDDL("foo", "1", null, "create server NONE type 'NONE' foreign data wrapper loopback", false);
		admin.executeDDL("foo", "1", null, "create schema test server NONE", false);
		admin.executeDDL("foo", "1", "test", "CREATE FOREIGN TABLE G1 (e1 integer PRIMARY KEY, e2 varchar(25), e3 double)", false);
	
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo@mm://localhost:31000;user=user;password=user;autoFailover=true", null);
        
        execute("SELECT * FROM test.G1"); //$NON-NLS-1$
        assertRowCount(1);        
        try {
            execute("SELECT * FROM test.G2"); //$NON-NLS-1$
            fail("should have failed as there is no G2 Table");
        } catch (Exception e) {
        }
        
        closeConnection();
        
        // add table
        admin.executeDDL("foo", "1", "test", "CREATE FOREIGN TABLE G2 (e1 integer PRIMARY KEY, "
                + "e2 varchar(25), e3 double)", false);
        
        // and execute, using the same connection
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo@mm://localhost:31000;user=user;password=user;autoFailover=true", null);
        
    	execute("SELECT * FROM test.G2"); //$NON-NLS-1$
        assertRowCount(1);
        printResults();
        closeConnection();
        admin.executeDDL("foo", "1", null, "drop database foo", false);
	}
	@Test
    public void testOverrideTranslator() throws Exception {        
        admin.executeDDL(null, null, null, "create database foo", false);
        admin.executeDDL("foo", "1", null, "create foreign data wrapper loopy type loopback OPTIONS(IncrementRows true, RowCount 500)", false);
        admin.executeDDL("foo", "1", null, "create server serverOne type 'NONE' foreign data wrapper loopy", false);
        admin.executeDDL("foo", "1", null, "create schema test server serverOne", false);
        admin.executeDDL("foo", "1", "test", "CREATE FOREIGN TABLE G1 (e1 integer PRIMARY KEY, e2 varchar(25), e3 double)", false);
    
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo@mm://localhost:31000;user=user;password=user", null);
        
        execute("SELECT * FROM test.G1"); //$NON-NLS-1$
        assertRowCount(500);        
        admin.executeDDL("foo", "1", null, "drop database foo", false);
        closeConnection();
    }

    @Test
    public void testVDBImport() throws Exception {        
        admin.executeDDL(null, null, null, "create database foo", false);
        admin.executeDDL("foo", "1", null, "create foreign data wrapper loopy type loopback OPTIONS(IncrementRows true, RowCount 500)", false);
        admin.executeDDL("foo", "1", null, "create server serverOne type 'NONE' foreign data wrapper loopy", false);
        admin.executeDDL("foo", "1", null, "create schema test server serverOne", false);
        admin.executeDDL("foo", "1", "test", "CREATE FOREIGN TABLE G1 (e1 integer PRIMARY KEY, e2 varchar(25), e3 double)", false);
    
        admin.executeDDL(null, null, null, "create database BAR", false);
        admin.executeDDL("BAR", "1", null, "IMPORT database foo VERSION '1'", false);
        
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:BAR@mm://localhost:31000;user=user;password=user", null);
        
        execute("SELECT * FROM test.G1"); //$NON-NLS-1$
        assertRowCount(500);        
        admin.executeDDL("foo", "1", null, "drop database foo", false);
        closeConnection();
    }    
    
    @Test 
    public void testUdfClasspath() throws Exception {
        JavaArchive jar = ShrinkWrap.create(JavaArchive.class, "func.jar")
                  .addClasses(SampleFunctions.class);
        admin.deploy("func.jar", jar.as(ZipExporter.class).exportAsInputStream());
        
        admin.executeDDL(null, null, null, "create database \"dynamic-func\" OPTIONS(lib 'deployment.func.jar')", false);
        admin.executeDDL("dynamic-func", "1", null, "create VIRTUAL schema test", false);
        admin.executeDDL("dynamic-func", "1", "test", "CREATE function func (val string) returns integer "
                + "options (JAVA_CLASS 'org.teiid.arquillian.SampleFunctions',  JAVA_METHOD 'doSomething')", false);
                
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:dynamic-func@mm://localhost:31000;user=user;password=user", null);
        
        execute("SELECT func('a')"); //$NON-NLS-1$
        assertRowCount(1);
        admin.executeDDL("dynamic-func", "1", null, "drop database \"dynamic-func\"", false);
        closeConnection();
    }

    public void testDDLOverJDBC() throws Exception {	 
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo2@mm://localhost:31000;user=user;password=user;autoFailover=true;vdbEdit=create", null);
        
        execute("create foreign data wrapper loopback"); //$NON-NLS-1$
        execute("create server NONE type 'NONE' foreign data wrapper loopback");
        execute("create schema test server NONE");
        execute("CREATE FOREIGN TABLE G1 (e1 integer PRIMARY KEY, e2 varchar(25), e3 double)");
        
        execute("SELECT * FROM test.G1"); //$NON-NLS-1$
        assertRowCount(1);        
        try {
            execute("SELECT * FROM test.G2"); //$NON-NLS-1$
            fail("should have failed as there is no G2 Table");
        } catch (Exception e) {
        }
        
        try {
        // add table
        execute("foo", "1", "test", "CREATE FOREIGN TABLE G2 (e1 integer PRIMARY KEY, "
                + "e2 varchar(25), e3 double)", false);
        } catch (SQLException e) {
        	
        }
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo2@mm://localhost:31000;user=user;password=user;autoFailover=true;vdbEdit=create", null);
        
        // and execute, using the same connection
        execute("SELECT * FROM test.G2"); //$NON-NLS-1$
        assertRowCount(1);        
        execute("drop database foo2");
        closeConnection();
        admin.executeDDL("foo2", "1", null, "drop database foo2", false);
 	}
}
