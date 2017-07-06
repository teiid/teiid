/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.adminshell;

import groovy.sql.TeiidSql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import java.util.Properties;

import org.teiid.jdbc.TeiidDriver;
import org.teiid.net.TeiidURL;
import org.teiid.script.io.ResultSetReader;

/**
 * Extensions of Groovy Sql support, which somewhat bridges the gap to our old adminshell sql logic
 */
public class GroovySqlExtensions {
	
	private static Help help = new Help(GroovySqlExtensions.class);
	
	@Help.Doc(text = "Get a Teiid connection with a URL")
	public static TeiidSql connect(
			@Help.Doc(text = "url") String url) throws SQLException {
		return connect(url, null, null);
	}

	@Help.Doc(text = "Get a Teiid connection")
	public static TeiidSql connect(
			@Help.Doc(text = "url") String url, 
			@Help.Doc(text = "user") String user, 
			@Help.Doc(text = "password") String password) throws SQLException {
		Properties info = new Properties();
		if (user != null) {
			info.setProperty(TeiidURL.CONNECTION.USER_NAME, user);
		}
		if (password != null) {
			info.setProperty(TeiidURL.CONNECTION.PASSWORD, password);
		}
		Connection c = TeiidDriver.getInstance().connect(url, info);
		if (c == null) {
			throw new SQLException("Invalid url " + url);
		}
		return new TeiidSql(c);
	}
	
	@SuppressWarnings("nls")
	@Help.Doc(text = "Get a SQL connection using the defaults from connection.properties")
	public static TeiidSql connect() throws SQLException {
		AdminShell.loadConnectionProperties();
		return connect(AdminShell.p.getProperty("jdbc.url", "jdbc:teiid:VDB@mm://localhost:31000"), 
				AdminShell.p.getProperty("jdbc.user", "admin"), AdminShell.p.getProperty("jdbc.password", "teiid"));
	}
	
	@Help.Doc(text = "Alternate row to String method that pretty prints SQL/XML",
		moreText = "e.g. sql.eachRow(\"select * from tables\", {row -> println rowToString(row) })")
	public static String rowToString(ResultSet results) throws SQLException {
		StringBuilder sb = new StringBuilder();
		//alternative toString - see GroovyResultSetExtension
		int columnCount = results.getMetaData().getColumnCount();
		for (int col = 1; col <= columnCount; col++) {
		    int type = results.getMetaData().getColumnType(col);
		    if (type == Types.BLOB) {
		    	Object anObj = results.getObject(col);
		        sb.append(anObj != null ? "BLOB" : "null"); //$NON-NLS-1$ //$NON-NLS-2$
		    }
		    else if (type == Types.SQLXML) {
		    	SQLXML xml = results.getSQLXML(col);
		    	sb.append(xml != null ? ResultSetReader.prettyPrint(xml) : "null"); //$NON-NLS-1$
		    }
		    else {
		    	String str = results.getString(col);
		        sb.append(str != null ? str : "null"); //$NON-NLS-1$
		    }
		    if (col != columnCount) {
		        sb.append("    "); //$NON-NLS-1$ 
		    }                    
		}
		return sb.toString();
	}
	
	@Help.Doc(text = "Pretty prints the ResultSetMetadata",
			moreText = {"e.g. sql.eachRow(\"select * from tables\", {rsmd -> println resultSetMetaDataToString(rsmd)},",
					    "                {row -> println rowToString(row) })"})
	public static String resultSetMetaDataToString(ResultSetMetaData rsmd) throws SQLException {
		return ResultSetReader.resultSetMetaDataToString(rsmd, "    "); //$NON-NLS-1$
	}
	
	@Help.Doc(text = "Show help for all SQL Extension methods")
	public static void sqlHelp() {
		help.help();
	}
	
	@Help.Doc(text = "Show help for the given SQL Extension method")
	public static void sqlHelp(
			@Help.Doc(text = "method name") String method) {
		help.help(method);
	}

}
