/*
* JBoss, Home of Professional Open Source.
* See the COPYRIGHT.txt file distributed with this work for information
* regarding copyright ownership. Some portions may be licensed
* to Red Hat, Inc. under one or more contributor license agreements.
*
* This library is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License as published by the Free Software Foundation; either
* version 2.1 of the License, or (at your option) any later version.
*
* This library is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this library; if not, write to the Free Software
* Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
* 02110-1301 USA.
*/
package org.teiid.example;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.Test;
import org.teiid.example.util.FileUtils;

public class TestFileUtils {

	@Test
	public void testReadClassPathFile() throws IOException{
		
		assertNotNull(FileUtils.readFileContent("rdbms-as-a-datasource", "readme.txt")); //$NON-NLS-1$ //$NON-NLS-2$
		assertNotNull(FileUtils.readFileContent("rdbms-as-a-datasource", "customer-schema-h2.sql")); //$NON-NLS-1$ //$NON-NLS-2$
		assertNotNull(FileUtils.readFileContent("rdbms-as-a-datasource", "h2-vdb.xml")); //$NON-NLS-1$ //$NON-NLS-2$
		
		assertNotNull(FileUtils.readFileContent("excel-as-a-datasource", "readme.txt")); //$NON-NLS-1$ //$NON-NLS-2$
		assertNotNull(FileUtils.readFileContent("excel-as-a-datasource", "otherholdings.xls")); //$NON-NLS-1$ //$NON-NLS-2$
		assertNotNull(FileUtils.readFileContent("excel-as-a-datasource", "excel-vdb.xml")); //$NON-NLS-1$ //$NON-NLS-2$

	}
	
	@Test
	public void testReadFilePath() {
		assertNotNull(FileUtils.readFilePath("excel-as-a-datasource", "data")); //$NON-NLS-1$ //$NON-NLS-2$
		assertNotNull(FileUtils.readFilePath("embedded-portfolio", "data")); //$NON-NLS-1$ //$NON-NLS-2$
		assertNotNull(FileUtils.readFilePath("embedded-portfolio", "data")); //$NON-NLS-1$ //$NON-NLS-2$
		
 	}

}
