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

package org.teiid.transport;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.postgresql.Driver;
import org.teiid.jdbc.TestMMDatabaseMetaData;
import org.teiid.transport.TestODBCSocketTransport.AnonSSLSocketFactory;
import org.teiid.transport.TestODBCSocketTransport.FakeOdbcServer;
import org.teiid.transport.TestODBCSocketTransport.Mode;

@SuppressWarnings("nls")
public class TestODBCSSL {
	
	FakeOdbcServer odbcServer = new FakeOdbcServer();
	
	@After public void tearDown() {
		odbcServer.stop();
	}
	
	@Test public void testSelectSsl() throws Exception {
		odbcServer.start(Mode.ENABLED);
		Driver d = new Driver();
		Properties p = new Properties();
		p.setProperty("user", "testuser");
		p.setProperty("password", "testpassword");
		p.setProperty("ssl", "true");
		p.setProperty("sslfactory", AnonSSLSocketFactory.class.getName());
		Connection conn = d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
		Statement s = conn.createStatement();
		assertTrue(s.execute("select * from tables order by name"));
		TestMMDatabaseMetaData.compareResultSet("TestODBCSocketTransport/testSelect", s.getResultSet());
		
		p.remove("ssl");
		try {
			conn = d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
			fail("should require ssl");
		} catch (SQLException e) {
			
		}
	}
	
	@Test(expected=SQLException.class) public void testLogin() throws Exception {
		odbcServer.start(Mode.LOGIN);
		Driver d = new Driver();
		Properties p = new Properties();
		p.setProperty("user", "testuser");
		p.setProperty("password", "testpassword");
		d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
	}
	
	@Test(expected=SQLException.class) public void testNonSSL() throws Exception {
		odbcServer.start(Mode.DISABLED);
		Driver d = new Driver();
		Properties p = new Properties();
		p.setProperty("user", "testuser");
		p.setProperty("password", "testpassword");
		p.setProperty("ssl", "true");
		p.setProperty("sslfactory", AnonSSLSocketFactory.class.getName());
		d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
	}

}
