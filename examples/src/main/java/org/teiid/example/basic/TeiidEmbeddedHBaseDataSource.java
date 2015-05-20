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

import static org.teiid.example.util.JDBCUtils.executeQuery;
import static org.teiid.example.util.JDBCUtils.executeUpdate;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.util.concurrent.ArrayBlockingQueue;

import javax.sql.DataSource;

import org.teiid.example.EmbeddedHelper;
import org.teiid.example.ExampleBase;
import org.teiid.example.util.FileUtils;
import org.teiid.example.util.JDBCUtils;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.hbase.HBaseExecutionFactory;

public class TeiidEmbeddedHBaseDataSource extends ExampleBase {
    
    public void execute(String vdb) throws Exception {
        execute(vdb, null);
    }
	
	@Override
	public void execute(String vdb, ArrayBlockingQueue<String> queue) throws Exception {
		
		DataSource ds = EmbeddedHelper.newDataSource(JDBC_DRIVER, JDBC_URL, JDBC_USER, JDBC_PASS);
		
		initSamplesData(ds);
		
		server = new EmbeddedServer();
		
		HBaseExecutionFactory factory = new HBaseExecutionFactory();
		factory.start();
		server.addTranslator("translator-hbase", factory); //$NON-NLS-1$
		
		server.addConnectionFactory("java:/hbaseDS", ds); //$NON-NLS-1$
		
		start(false);
    	
		server.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
		
		conn = server.getDriver().connect("jdbc:teiid:hbasevdb", null); //$NON-NLS-1$
		
		executeQuery(conn, "SELECT * FROM Customer", queue); //$NON-NLS-1$
		executeQuery(conn, "SELECT * FROM Customer ORDER BY name, city DESC", queue); //$NON-NLS-1$
		 
		tearDown();
		
		add(queue, "Exit"); //$NON-NLS-1$
	}
	


	public static void main(String[] args) throws Exception {
		new TeiidEmbeddedHBaseDataSource().execute(FileUtils.readFileContent("hbase-as-a-datasource", "hbase-vdb.xml")); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	private static void initSamplesData(DataSource ds) throws Exception {
		Connection conn = ds.getConnection();
		// init test data
		executeUpdate(conn, CUSTOMER);
		executeUpdate(conn, "UPSERT INTO \"Customer\" VALUES('101', 'Los Angeles, CA', 'John White', '$400.00', 'Chairs')"); //$NON-NLS-1$
		executeUpdate(conn, "UPSERT INTO \"Customer\" VALUES('102', 'Atlanta, GA', 'Jane Brown', '$200.00', 'Lamps')"); //$NON-NLS-1$
		executeUpdate(conn, "UPSERT INTO \"Customer\" VALUES('103', 'Pittsburgh, PA', 'Bill Green', '$500.00', 'Desk')"); //$NON-NLS-1$
		executeUpdate(conn, "UPSERT INTO \"Customer\" VALUES('104', 'St. Louis, MO', 'Jack Black', '$8000.00', 'Bed')"); //$NON-NLS-1$
		executeUpdate(conn, "UPSERT INTO \"Customer\" VALUES('105', 'Los Angeles, CA', 'John White', '$400.00', 'Chairs')"); //$NON-NLS-1$
		JDBCUtils.close(conn);
	}

	static final String CUSTOMER = "CREATE TABLE IF NOT EXISTS \"Customer\"(\"ROW_ID\" VARCHAR PRIMARY KEY, \"customer\".\"city\" VARCHAR, \"customer\".\"name\" VARCHAR, \"sales\".\"amount\" VARCHAR, \"sales\".\"product\" VARCHAR)";
	
	static final String JDBC_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    static final String JDBC_URL = "jdbc:phoenix:127.0.0.1:2181";
    static final String JDBC_USER = "";
    static final String JDBC_PASS = "";
	


}
