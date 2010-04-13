/*
 * Copyright � 2000-2005 MetaMatrix, Inc.  All rights reserved.
 */
package com;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.teiid.jdbc.TeiidDriver;
import org.teiid.jdbc.TeiidStatement;


/**
 */
public class IntTest {

	public static void main(String[] args) throws Exception {
		Connection c = TeiidDriver.getInstance().connect("jdbc:teiid:TPCRperf@mm://localhost:31000;user=admin;password=teiid", null);
		Statement s = c.createStatement();
		try {
			s.execute("set showplan debug");
			ResultSet rs = s.executeQuery("SELECT ORDERS.O_ORDERKEY, ORDERS.O_ORDERDATE, ORDERS.O_CLERK, CUSTOMER.C_CUSTKEY, CUSTOMER.C_NAME FROM TPCR01_SqlServerVirt.TPCR01.ORDERS, TPCR01_OracleVirt.CUSTOMER WHERE CUSTOMER.C_CUSTKEY = ORDERS.O_CUSTKEY AND CUSTOMER.C_NATIONKEY = 1  AND CUSTOMER.C_ACCTBAL between 5 AND 2000 AND ORDERS.O_ORDERDATE < {ts'1998-01-01 00:00:00'} OPTION MAKEDEP TPCR01_SQLServerPhys.TPCR01.ORDERS");
			int i = 0;
			while (rs.next()) {
				i++;
			}
			System.out.println(s.unwrap(TeiidStatement.class).getPlanDescription());
			System.out.println(s.unwrap(TeiidStatement.class).getDebugLog());
			System.out.println(i);
		} finally {
			s.close();
			c.close();
		}
	}
	
}