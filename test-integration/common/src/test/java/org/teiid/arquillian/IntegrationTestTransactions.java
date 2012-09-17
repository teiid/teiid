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

import static org.junit.Assert.*;

import java.io.FileInputStream;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminFactory;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestTransactions extends AbstractMMQueryTestCase {

	private Admin admin;
	
	@Before
	public void setup() throws Exception {
		admin = AdminFactory.getInstance().createAdmin("localhost", 9999,	"admin", "admin".toCharArray());
	}
	
	@After
	public void teardown() throws AdminException {
		AdminUtil.cleanUp(admin);
		admin.close();
	}

	@Test
    public void testViewDefinition() throws Exception {
				
		admin.deploy("txn-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("txn-vdb.xml")));
		
		assertTrue(AdminUtil.waitForVDBLoad(admin, "txn", 1, 30));
		
		this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:txn@mm://localhost:31000;user=user;password=user", null);
		
		execute("create local temporary table temp (x integer)"); //$NON-NLS-1$
		execute("call proc()");
		execute("start transaction"); //$NON-NLS-1$
		execute("call proc()");
		execute("insert into temp (x) values (1)"); //$NON-NLS-1$
		execute("select * from temp");
		assertRowCount(1);
		execute("rollback");
		execute("select * from temp");
		assertRowCount(0);
		
		execute("select rand(1)"); //TODO - I think our rand function doesn't make sense
		this.internalConnection.setAutoCommit(false);
		
		execute("/*+ cache */ select rand()");
		internalResultSet.next();
		double d = internalResultSet.getDouble(1);
		
		execute("select rand(2)"); 
		execute("/*+ cache */ select rand()");
		internalResultSet.next();
		double d1 = internalResultSet.getDouble(1);
		assertEquals("Expected same in the txn", d, d1, 0);
		
		this.internalConnection.rollback(); 
		this.internalConnection.setAutoCommit(true);
		
		execute("select rand(3)"); 
		execute("/*+ cache */ select rand()");
		internalResultSet.next();
		double d2 = internalResultSet.getDouble(1);
		assertTrue("Expected different after rollback", d != d2);
		
		execute("select rand(4)"); 
		execute("/*+ cache */ select rand()");
		internalResultSet.next();
		d = internalResultSet.getDouble(1);
		assertEquals("Expected same after autoCommit", d, d2, 0);
    }

}
