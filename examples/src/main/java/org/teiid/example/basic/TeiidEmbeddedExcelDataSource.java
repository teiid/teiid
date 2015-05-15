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

import org.teiid.example.ExampleBase;
import org.teiid.example.util.FileUtils;
import org.teiid.example.util.JDBCUtils;
import org.teiid.resource.adapter.file.FileManagedConnectionFactory;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.excel.ExcelExecutionFactory;

import static org.teiid.example.util.JDBCUtils.executeQuery;

public class TeiidEmbeddedExcelDataSource extends ExampleBase {
	
	@Override
	public void execute(String vdb) throws Exception {
		
		server = new EmbeddedServer();
		
		ExcelExecutionFactory factory = new ExcelExecutionFactory();
		factory.start();
		factory.setSupportsDirectQueryProcedure(true);
		server.addTranslator("excel", factory);
		
		FileManagedConnectionFactory managedconnectionFactory = new FileManagedConnectionFactory();
		
		managedconnectionFactory.setParentDirectory(FileUtils.readFilePath("excel-as-a-datasource", "data"));
		server.addConnectionFactory("java:/excel-file", managedconnectionFactory.createConnectionFactory());
		
		start(false);
		
		server.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
		
		conn = server.getDriver().connect("jdbc:teiid:ExcelVDB", null);
		
		executeQuery(conn, "SELECT * FROM Sheet1");
		
		executeQuery(conn, "SELECT * FROM PersonalHoldings");
		
		JDBCUtils.close(conn);
		
		server.stop();
		
	}

	public static void main(String[] args) throws Exception {
		new TeiidEmbeddedExcelDataSource().execute(FileUtils.readFileContent("excel-as-a-datasource", "excel-vdb.xml"));
	}
}
