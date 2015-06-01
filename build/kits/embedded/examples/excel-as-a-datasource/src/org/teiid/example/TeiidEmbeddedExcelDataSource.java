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

import static org.teiid.example.util.JDBCUtils.execute;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;

import org.teiid.resource.adapter.file.FileManagedConnectionFactory;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.excel.ExcelExecutionFactory;

@SuppressWarnings("nls")
public class TeiidEmbeddedExcelDataSource {

	public static void main(String[] args) throws Exception {
		
		EmbeddedServer server = new EmbeddedServer();
		
		ExcelExecutionFactory factory = new ExcelExecutionFactory();
		factory.start();
		factory.setSupportsDirectQueryProcedure(true);
		server.addTranslator("excel", factory);
		
		FileManagedConnectionFactory managedconnectionFactory = new FileManagedConnectionFactory();
		managedconnectionFactory.setParentDirectory("data");
		server.addConnectionFactory("java:/excel-file", managedconnectionFactory.createConnectionFactory());

		server.start(new EmbeddedConfiguration());
    	
		server.deployVDB(new FileInputStream(new File("excel-vdb.xml")));
		
		Connection c = server.getDriver().connect("jdbc:teiid:ExcelVDB", null);
		
		execute(c, "SELECT * FROM Sheet1", false);
		execute(c, "SELECT * FROM PersonalHoldings", true);
		
		server.stop();
	}

}
