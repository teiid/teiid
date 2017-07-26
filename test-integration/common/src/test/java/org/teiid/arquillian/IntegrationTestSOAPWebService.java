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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.Properties;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestSOAPWebService {

	private Admin admin;
	
	@Before
	public void setup() throws Exception {
        admin = AdminFactory.getInstance().createAdmin("localhost", 9999, "admin",
                "admin".toCharArray());
        assertNotNull(admin);
	}
	
	@After
	public void teardown() throws AdminException {
		AdminUtil.cleanUp(admin);
		admin.close();
	}

	@Test
	public void testVDBDeployment() throws Exception {
		Collection<?> vdbs = admin.getVDBs();
		assertTrue(vdbs.isEmpty());

		assertTrue(admin.getDataSourceTemplateNames().contains("webservice"));
        String raSource = "web-ds";
        assertFalse(admin.getDataSourceNames().contains(raSource));
        
        admin.deploy("addressing-service.war", new FileInputStream(UnitTestUtil.getTestDataFile("addressing-service.war")));
        
        Properties p = new Properties();
        p.setProperty("class-name", "org.teiid.resource.adapter.ws.WSManagedConnectionFactory");
        p.setProperty("EndPoint", "http://localhost:8080/jboss-jaxws-addressing/AddressingService");
        
        admin.createDataSource(raSource, "webservice", p);      
        
        assertTrue(admin.getDataSourceNames().contains(raSource));
		
		admin.deploy("soapsvc-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("soapsvc-vdb.xml")));
		vdbs = admin.getVDBs();
		assertFalse(vdbs.isEmpty());

		VDB vdb = admin.getVDB("WSMSG", 1);
		AdminUtil.waitForVDBLoad(admin, "WSMSG", 1, 3);

		vdb = admin.getVDB("WSMSG", 1);
		assertTrue(vdb.isValid());
		assertTrue(vdb.getStatus().equals(Status.ACTIVE));

        Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:WSMSG@mm://localhost:31000;user=user;password=user;", null);
        Statement stmt = conn.createStatement();
        String sql = "SELECT *\n" + 
                "FROM ADDRESSINGSERVICE.SAYHELLO\n" + 
                "WHERE MESSAGEID = 'UUID-100' AND SAYHELLO = 'Teiid'\n" + 
                "AND ADDRESSINGSERVICE.SAYHELLO.To = 'http://www.w3.org/2005/08/addressing/anonymous'\n" + 
                "AND ADDRESSINGSERVICE.SAYHELLO.ReplyTo = 'http://www.w3.org/2005/08/addressing/anonymous'\n" + 
                "AND ADDRESSINGSERVICE.SAYHELLO.Action = 'http://www.w3.org/2005/08/addressing/ServiceIface/sayHello'";
        ResultSet rs = stmt.executeQuery(sql);
        assertTrue(rs.next());
        assertEquals("Hello World!", rs.getString(1));
        conn.close();
	}
}
