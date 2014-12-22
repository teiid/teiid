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
package org.teiid.translator.hbase.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.teiid.language.SQLConstants.Tokens;

public class PhoenixUtils {
	
	public static final String QUOTE = "\"";
	
	public static String hbaseTableMappingDDL(PTable ptable) {
		
		StringBuffer sb = new StringBuffer();
		sb.append("CREATE TABLE IF NOT EXISTS").append(Tokens.SPACE).append(ptable.getTableName().getString());
		sb.append(Tokens.SPACE).append(Tokens.LPAREN);
		
		for (PColumn pColumn : ptable.getColumns()) {
			if(pColumn.getFamilyName() == null) {
				String pk = pColumn.getName().getString();
				sb.append(pk).append(Tokens.SPACE).append(pColumn.getDataType().getSqlTypeName()).append(Tokens.SPACE).append("PRIMARY KEY").append(Tokens.COMMA).append(Tokens.SPACE);
			} else {
				String family = pColumn.getFamilyName().getString();
				String qualifier = pColumn.getName().getString();
				sb.append(family).append(Tokens.DOT).append(qualifier).append(Tokens.SPACE).append(pColumn.getDataType().getSqlTypeName()).append(Tokens.COMMA).append(Tokens.SPACE);
			}
		}
		
//		String pks = "";
//		for(PColumn pkColumn: ptable.getPKColumns()) {
//			pks += pkColumn.getName().getString();
//			pks += Tokens.COMMA;
//			pks += Tokens.SPACE;
//		}
//		pks = pks.substring(0, pks.length() - 2);
//		sb.append("CONSTRAINT").append(Tokens.SPACE).append("PK_" + ptable.getTableName().getString()).append(Tokens.SPACE) ;
//		sb.append("PRIMARY KEY").append(Tokens.SPACE).append(Tokens.LPAREN).append(pks).append(Tokens.RPAREN);
		String ddl = sb.toString();
		ddl = ddl.substring(0, ddl.length() - 2) + Tokens.RPAREN ;
		return ddl;
	}
	
	public static boolean executeUpdate(Connection conn, String sql) throws SQLException {
		
		Statement stmt = null;
		
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			throw e;
		} finally {
			close(stmt);
		}
		
		return true ;
	}
	
	public static void close(Statement stmt) {

		if(null != stmt) {
			try {
				stmt.close();
				stmt = null;
			} catch (SQLException e) {
				
			}
		}
	}
	
	public static Connection getPhoenixConnection(String zkQuorum) throws Exception {
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		String connectionURL = "jdbc:phoenix:" + zkQuorum;
		Connection conn = DriverManager.getConnection(connectionURL);
		conn.setAutoCommit(true);
		return conn;
	}

	

}
