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

package org.teiid.example.federation;

import static org.teiid.example.util.JDBCUtils.executeQuery;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;

import javax.sql.DataSource;

import org.h2.tools.RunScript;
import org.h2.tools.Server;
import org.teiid.example.EmbeddedHelper;
import org.teiid.example.ExampleBase;
import org.teiid.example.util.FileUtils;
import org.teiid.resource.adapter.file.FileManagedConnectionFactory;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.file.FileExecutionFactory;
import org.teiid.translator.jdbc.h2.H2ExecutionFactory;


/**
 * This example is same as "Teiid Quick Start Example", you can find at http://jboss.org/teiid/quickstart
 * whereas the example shows how to use Dynamic VDB using a server based deployment, this below code shows to use same
 * example using Embedded Teiid. This uses a memory based H2 database and File source as sources and provides a 
 * view layer using DDL. 
 * 
 * Note that this example shows how to integrate the traditional sources like jdbc, file, web-service etc, however you are 
 * not limited to only those sources. As long as you can extended and provide implementations for
 *  - ConnectionfactoryProvider
 *  - Translator
 *  - Metadata Repository
 *  
 *  you can integrate any kind of sources together, or provide a JDBC interface on a source or expose with with known schema.
 */
public class TeiidEmbeddedPortfolio extends ExampleBase {
	
    public void execute(String vdb) throws Exception {
        execute(vdb, null);
    }
    
	@Override
	public void execute(String vdb, ArrayBlockingQueue<String> queue) throws Exception {

		startH2Server();
		
		DataSource ds = EmbeddedHelper.newDataSource("org.h2.Driver", "jdbc:h2:mem://localhost/~/account", "sa", "sa"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		initSamplesData(ds);
		
		server = new EmbeddedServer();
		
		H2ExecutionFactory executionFactory = new H2ExecutionFactory() ;
		executionFactory.setSupportsDirectQueryProcedure(true);
		executionFactory.start();
		server.addTranslator("translator-h2", executionFactory); //$NON-NLS-1$ 
		
		server.addConnectionFactory("java:/accounts-ds", ds); //$NON-NLS-1$ 
		
    	FileExecutionFactory fileExecutionFactory = new FileExecutionFactory();
    	fileExecutionFactory.start();
    	server.addTranslator("file", fileExecutionFactory); //$NON-NLS-1$ 
    	
    	FileManagedConnectionFactory managedconnectionFactory = new FileManagedConnectionFactory();
		managedconnectionFactory.setParentDirectory(FileUtils.readFilePath("embedded-portfolio", "data")); //$NON-NLS-1$  //$NON-NLS-2$ 
		server.addConnectionFactory("java:/marketdata-file", managedconnectionFactory.createConnectionFactory()); //$NON-NLS-1$ 
		
		start(false);
		
		server.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
		
		conn = server.getDriver().connect("jdbc:teiid:Portfolio", null); //$NON-NLS-1$ 
		
		executeQuery(conn, "SELECT * FROM Product", queue); //$NON-NLS-1$ 
		executeQuery(conn, "SELECT * FROM StockPrices", queue); //$NON-NLS-1$ 
		executeQuery(conn, "SELECT * FROM Stock", queue); //$NON-NLS-1$ 
		executeQuery(conn, "SELECT stock.* from (call MarketData.getTextFiles('*.txt')) f, TEXTTABLE(f.file COLUMNS symbol string, price bigdecimal HEADER) stock", queue); //$NON-NLS-1$ 
		executeQuery(conn, "select product.symbol, stock.price, company_name from product, (call MarketData.getTextFiles('*.txt')) f, TEXTTABLE(f.file COLUMNS symbol string, price bigdecimal HEADER) stock where product.symbol=stock.symbol", queue); //$NON-NLS-1$ 
		
		tearDown();
		
		stopH2Server();
		
		add(queue, "Exit"); //$NON-NLS-1$
	}	
	
	private static Server h2Server = null;
	
	public static void main(String[] args) throws Exception {
		new TeiidEmbeddedPortfolio().execute(FileUtils.readFileContent("embedded-portfolio", "portfolio-vdb.xml"));
	}

	private void stopH2Server() {
		h2Server.stop();
	}

	private void initSamplesData(DataSource ds) throws FileNotFoundException, SQLException {
		RunScript.execute(ds.getConnection(), new InputStreamReader(new FileInputStream(FileUtils.readFile("embedded-portfolio", "customer-schema.sql"))));
	}


	private void startH2Server() throws SQLException {
		h2Server = Server.createTcpServer().start();	
	}

	
}
