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

package org.teiid.example;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.StringTokenizer;

import javax.resource.ResourceException;
import javax.resource.cci.ConnectionFactory;
import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.resource.adapter.file.FileConnectionImpl;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.TranslatorException;
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
@SuppressWarnings("nls")
public class TeiidEmbeddedPortfolio {
	
	/**
	 * If you are trying to use per-built translators in Teiid framework, then the connection semantics for that
	 * source already defined in a interface. The below examples show how you use the connection interface that translator
	 * understands, in this case by using the default implementation with default values. If you are writing a custom translator
	 *  then can you define the connection interface and connection provider based on that.  
	 */
	@SuppressWarnings("serial")
	private static final class FileConnectionFactory extends
			BasicConnectionFactory<FileConnectionImpl> {
		@Override
		public FileConnectionImpl getConnection() throws ResourceException {
			return new FileConnectionImpl(".", null, false);
		}
	}
	
	/**
	 * VDB = Virtual Database, in Teiid a VDB contains one or more models which define the source characteristics, like
	 * connection to source, what translator need to be used, how to read/fetch metadata about the source. The 
	 */
	private static void buildDeployVDB(EmbeddedServer teiidServer) throws Exception {
		// model for the file source
		ModelMetaData fileModel = new ModelMetaData();
		fileModel.setName("MarketData");
		// ddl, native are two pre-built metadata repos types, you can build your own, 
		// then register it using "addMetadataRepository"
		fileModel.setSchemaSourceType("native");
		fileModel.addSourceMapping("text-connector", "file", "source-file");

		// model for the h2 database
		ModelMetaData jdbcModel = new ModelMetaData();
		jdbcModel.setName("Accounts");
		jdbcModel.setSchemaSourceType("native");
		jdbcModel.addSourceMapping("h2-connector", "h2", "source-jdbc");
		
		// creating a virtual (logical) view model
		ModelMetaData portfolioModel = new ModelMetaData();
		portfolioModel.setName("MyView");
		portfolioModel.setModelType(Type.VIRTUAL);
		portfolioModel.setSchemaSourceType("ddl");
		// For defining the view in DDL take a look at https://docs.jboss.org/author/display/TEIID/DDL+Metadata
		portfolioModel.setSchemaText("create view \"portfolio\" OPTIONS (UPDATABLE 'true') as select product.symbol as symbol, stock.price as price, company_name from product, (call MarketData.getTextFiles('data/marketdata-price.txt')) f, TEXTTABLE(f.file COLUMNS symbol string, price bigdecimal HEADER) stock where product.symbol=stock.symbol");

		// deploy the VDB to the embedded server
		teiidServer.deployVDB("example", fileModel, jdbcModel, portfolioModel);
	}	
	
	private static void execute(Connection connection, String sql, boolean closeConn) throws Exception {
		try {
			Statement statement = connection.createStatement();
			
			boolean hasResults = statement.execute(sql);
			if (hasResults) {
				ResultSet results = statement.getResultSet();
				ResultSetMetaData metadata = results.getMetaData();
				int columns = metadata.getColumnCount();
				System.out.println("Results");
				for (int row = 1; results.next(); row++) {
					System.out.print(row + ": ");
					for (int i = 0; i < columns; i++) {
						if (i > 0) {
							System.out.print(",");
						}
						System.out.print(results.getString(i+1));
					}
					System.out.println();
				}
				results.close();
			}
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null && closeConn) {
				connection.close();
			}
		}		
	}
	
	public static void main(String[] args) throws Exception {
		// setup accounts database (if you already have external database this is not needed)
		// for schema take look at "data/customer-schema.sql" file.
		final JdbcDataSource h2ds = new JdbcDataSource();
    	h2ds.setURL("jdbc:h2:accounts");

		EmbeddedServer.ConnectionFactoryProvider<DataSource> jdbcProvider = new EmbeddedServer.SimpleConnectionFactoryProvider<DataSource>(h2ds);
		Connection conn = jdbcProvider.getConnectionFactory().getConnection();
		String schema = ObjectConverterUtil.convertFileToString(new File("data/customer-schema.sql"));
		StringTokenizer st = new StringTokenizer(schema, ";");
		while (st.hasMoreTokens()) {
			String sql = st.nextToken();
			execute(conn, sql.trim(), false);
		}
		conn.close();
		
		
		// now start Teiid in embedded mode
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		ec.setUseDisk(true);
				
		EmbeddedServer teiidServer = new EmbeddedServer();
		teiidServer.start(ec);
		
		// configure the connection provider and translator for file based source. 
		// NOTE: every source that is being integrated, needs its connection provider and its translator 
		// check out https://docs.jboss.org/author/display/TEIID/Built-in+Translators prebuit translators
		final ConnectionFactory cf = new FileConnectionFactory();
		teiidServer.addConnectionFactoryProvider("source-file", new EmbeddedServer.SimpleConnectionFactoryProvider<ConnectionFactory>(cf));
		teiidServer.addTranslator(FileExecutionFactory.class);

		// configure the connection provider and translator for jdbc based source
		teiidServer.addConnectionFactoryProvider("source-jdbc", jdbcProvider);
		teiidServer.addTranslator(H2ExecutionFactory.class);
		
		buildDeployVDB(teiidServer);
		
		// Now query the VDB
		TeiidDriver td = teiidServer.getDriver();
		Connection c = td.connect("jdbc:teiid:example", null);
		execute(c, "select * from Product", false);
		execute(c, "select * from MyView.portfolio", true);
	}	

}
