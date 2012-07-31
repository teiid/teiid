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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.util.Properties;

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
public class IntegrationTestVDBSeviceCleanup extends AbstractMMQueryTestCase {

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
    public void testServiceCleanup() throws Exception {
		admin.deploy("service-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("service-vdb.xml")));
		
		createDS("ServiceDS");
		
		assertTrue(AdminUtil.waitForVDBLoad(admin, "service", 1, 3));
		
		assertNotNull(TeiidDriver.getInstance().connect("jdbc:teiid:service@mm://localhost:31000;user=user;password=user", null));
		
		admin.undeploy("service-vdb.xml");
		
		admin.deleteDataSource("ServiceDS");
		
		admin.deploy("service-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("service-vdb.xml")));
		
		createDS("ServiceDS");
		
		assertTrue(AdminUtil.waitForVDBLoad(admin, "service", 1, 3));
		
		assertNotNull(TeiidDriver.getInstance().connect("jdbc:teiid:service@mm://localhost:31000;user=user;password=user", null));
    }

	private void createDS(String deployName) throws AdminException {
		Properties props = new Properties();
		props.setProperty("connection-url","jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
		props.setProperty("user-name", "sa");
		props.setProperty("password", "sa");
		AdminUtil.createDataSource(admin, deployName, "h2", props);
	}

}
