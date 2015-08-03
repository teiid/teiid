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

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.CommandContext;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.WireProtocol;

@SuppressWarnings("nls")
public class TestInternalConnection {
	
	private static final String vdb = "<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA["
			+ "CREATE VIEW helloworld as SELECT 'HELLO WORLD';"
			+ "CREATE function func (val integer) returns string options (JAVA_CLASS '"+TestInternalConnection.class.getName()+"',  JAVA_METHOD 'doSomething');]]> </metadata></model></vdb>";
	EmbeddedServer es;
	
	@Before public void setup() {
		es = new EmbeddedServer();
	}
	
	@After public void teardown() {
		es.stop();
	}
	
	public static String doSomething(CommandContext cc, Integer val) throws SQLException {
		TeiidConnection tc = cc.getConnection();
		try {
			Statement s = tc.createStatement();
			ResultSet rs = s.executeQuery("select user(), expr1 from helloworld");
			rs.next();
			return rs.getString(1) + rs.getString(2) + val;
		} finally {
			tc.close();
		}
	}
	
	@Test public void testInternalRemote() throws Exception {
		SocketConfiguration s = new SocketConfiguration();
		InetSocketAddress addr = new InetSocketAddress(0);
		s.setBindAddress(addr.getHostName());
		s.setPortNumber(addr.getPort());
		s.setProtocol(WireProtocol.teiid);
		EmbeddedConfiguration config = new EmbeddedConfiguration();
		config.addTransport(s);
		es.start(config);
		es.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
		Connection conn = null;
		try {
			TeiidDriver driver = new TeiidDriver();
			Properties p = new Properties();
			p.setProperty("user", "me");
			conn = driver.connect("jdbc:teiid:test@mm://"+addr.getHostName()+":"+es.getPort(0), p);
			ResultSet rs = conn.createStatement().executeQuery("select func(1)");
			rs.next();
			assertEquals("me@teiid-securityHELLO WORLD1", rs.getString(1));
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}
	
	@Test public void testInternalLocal() throws Exception {
		EmbeddedConfiguration config = new EmbeddedConfiguration();
		es.start(config);
		es.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
		Connection conn = null;
		try {
			TeiidDriver driver = es.getDriver();
			conn = driver.connect("jdbc:teiid:test", null);
			ResultSet rs = conn.createStatement().executeQuery("select func(2)");
			rs.next();
			assertEquals("anonymous@teiid-securityHELLO WORLD2", rs.getString(1));
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}	

}
