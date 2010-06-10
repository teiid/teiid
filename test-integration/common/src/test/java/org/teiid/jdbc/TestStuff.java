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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.jdbc.util.ResultSetUtil;
@SuppressWarnings("nls")
public class TestStuff {
	
	@Ignore
	@Test public void testNoExec() throws Exception {
		Connection c = DriverManager.getConnection("jdbc:teiid:tpcrperf@mm://localhost:31000", "admin", "teiid");
		Statement s = c.createStatement();
		long start = System.currentTimeMillis();
		//s.execute("set noexec on");
		s.execute("set showplan debug");
		DatabaseMetaData dmd = c.getMetaData();
		
		//ResultSetUtil.printResultSet(s.executeQuery("select * from TPCR01_OraclePhys.NATION"), 20, false, System.out);
		ResultSetUtil.printResultSet(s.executeQuery("select * from tables"), 20, false, System.out);
		
		//ResultSet rs = s.executeQuery("SELECT ORDERS.O_ORDERKEY, ORDERS.O_ORDERDATE, ORDERS.O_CLERK, CUSTOMER.C_CUSTKEY, CUSTOMER.C_NAME FROM TPCR01_SqlServerVirt.TPCR01.ORDERS,TPCR01_SqlServerVirt.TPCR01.CUSTOMER WHERE (CUSTOMER.C_CUSTKEY = ORDERS.O_CUSTKEY) AND (O_ORDERKEY < 2)");
		System.out.println(System.currentTimeMillis() - start);
		
		System.out.println(s.unwrap(TeiidStatement.class).getDebugLog());
		System.out.println(s.unwrap(TeiidStatement.class).getPlanDescription());
		s.close();
		c.close();
	}
	
	
}
