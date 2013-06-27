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
public class IntegrationTestMultisource extends AbstractMMQueryTestCase {

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
    public void testSourceOperations() throws Exception {
				
		admin.deploy("multisource-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("arquillian/multisource-vdb.xml")));
		
		Properties props = new Properties();
		props.setProperty("ParentDirectory", UnitTestUtil.getTestDataFile("/arquillian/txt/").getAbsolutePath());
		props.setProperty("AllowParentPaths", "true");
		props.setProperty("class-name", "org.teiid.resource.adapter.file.FileManagedConnectionFactory");
		
		AdminUtil.createDataSource(admin, "test-file", "file", props);
		
		assertTrue(AdminUtil.waitForVDBLoad(admin, "multisource", 1, 3));
		
		this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:multisource@mm://localhost:31000;user=user;password=user", null);
		
		execute("exec getfiles('*.txt')", new Object[] {}); //$NON-NLS-1$
		assertRowCount(1);
		
		admin.addSource("multisource", 1, "MarketData", "text-connector1", "file1", "java:/test-file");
		
		execute("exec getfiles('*.txt')", new Object[] {}); //$NON-NLS-1$
		assertRowCount(2);
		
		admin.removeSource("multisource", 1, "MarketData", "text-connector");
		
		execute("exec getfiles('*.txt')", new Object[] {}); //$NON-NLS-1$
		assertRowCount(1);
    }
	
}
