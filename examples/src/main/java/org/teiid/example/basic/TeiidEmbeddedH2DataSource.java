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

package org.teiid.example.basic;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.h2.tools.RunScript;
import org.h2.tools.Server;
import org.teiid.example.EmbeddedHelper;
import org.teiid.example.ExampleBase;
import org.teiid.example.util.FileUtils;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.jdbc.h2.H2ExecutionFactory;

import static org.teiid.example.util.JDBCUtils.executeQuery;


@SuppressWarnings("nls")
public class TeiidEmbeddedH2DataSource extends ExampleBase {
	
	private static Server h2Server = null;
	
	public static void main(String[] args) throws Exception {
		new TeiidEmbeddedH2DataSource().execute(FileUtils.readFileContent("rdbms-as-a-datasource", "h2-vdb.xml"));
	}

	private void stopH2Server() {
		h2Server.stop();
	}

	private void initSamplesData(DataSource ds) throws FileNotFoundException, SQLException {
		RunScript.execute(ds.getConnection(), new InputStreamReader(new FileInputStream(FileUtils.readFile("rdbms-as-a-datasource", "customer-schema-h2.sql"))));
	}


	private void startH2Server() throws SQLException {
		h2Server = Server.createTcpServer().start();	
	}

	@Override
	public void execute(String vdb) throws Exception {
		
		startH2Server();
		
		DataSource ds = EmbeddedHelper.newDataSource("org.h2.Driver", "jdbc:h2:mem://localhost/~/account", "sa", "sa");
		initSamplesData(ds);
		
		server = new EmbeddedServer();
		
		init("translator-h2", new H2ExecutionFactory());
		
		server.addConnectionFactory("java:/accounts-ds", ds);
		
		start(false);
		
		server.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
		
		conn = server.getDriver().connect("jdbc:teiid:H2VDB", null);
		
		executeQuery(conn, "SELECT * FROM CUSTOMERVIEW");
		
		executeQuery(conn, "SELECT SSN, FIRSTNAME, LASTNAME, ST_ADDRESS, CITY, STATE FROM Customer");
		
		tearDown();
		
		stopH2Server();
	}	

}
