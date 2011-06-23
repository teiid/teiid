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

package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Properties;

import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.VDBRepository;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.query.metadata.TransformationMetadata.Resource;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.teiid.TeiidExecutionFactory;


/**
 */
@SuppressWarnings("nls")
public class TestDymamicImportedMetaData {

	private MetadataFactory getMetadata(Properties importProperties, Connection conn)
			throws TranslatorException {
		MetadataFactory mf = createMetadataFactory("test", importProperties);
    	
    	TeiidExecutionFactory tef = new TeiidExecutionFactory();
    	tef.getMetadata(mf, conn);
    	return mf;
	}

	private MetadataFactory createMetadataFactory(String schema, Properties importProperties) {
		VDBRepository vdbRepository = new VDBRepository();
    	vdbRepository.setSystemStore(VDBMetadataFactory.getSystem());
    	return new MetadataFactory(schema, vdbRepository.getBuiltinDatatypes(), importProperties);
	}
	
    @Test public void testProcImport() throws Exception {
    	FakeServer server = new FakeServer();
    	server.deployVDB("vdb", UnitTestUtil.getTestDataPath() + "/TestCase3473/test.vdb");
    	Connection conn = server.createConnection("jdbc:teiid:vdb"); //$NON-NLS-1$
    	
    	Properties importProperties = new Properties();
    	importProperties.setProperty("importer.importProcedures", Boolean.TRUE.toString());
    	MetadataFactory mf = getMetadata(importProperties, conn);
    	Procedure p = mf.getMetadataStore().getSchemas().get("TEST").getProcedures().get("VDB.SYS.GETXMLSCHEMAS");
    	assertEquals(1, p.getResultSet().getColumns().size());
    }
    
    @Test public void testDuplicateException() throws Exception {
    	FakeServer server = new FakeServer();
    	MetadataFactory mf = createMetadataFactory("x", new Properties());
    	MetadataFactory mf1 = createMetadataFactory("y", new Properties());
    	
    	Table dup = mf.addTable("dup");
    	Table dup1 = mf1.addTable("dup");
    	
    	mf.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup);
    	mf1.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup1);
    	
    	MetadataStore ms = mf.getMetadataStore();
    	ms.addSchema(mf1.getMetadataStore().getSchemas().values().iterator().next());
    	
    	server.deployVDB("test", ms, new LinkedHashMap<String, Resource>());
    	Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$
    	
    	Properties importProperties = new Properties();
    	
    	mf = getMetadata(importProperties, conn);
    	Table t = mf.getMetadataStore().getSchemas().get("TEST").getTables().get("TEST.X.DUP");
    	assertEquals("\"test\".\"x\".\"dup\"", t.getNameInSource());

    	importProperties.setProperty("importer.useFullSchemaName", Boolean.FALSE.toString());
    	try {
    		getMetadata(importProperties, conn);
    		fail();
    	} catch (TranslatorException e) {
    		
    	}
    }
    
    @Test public void testUseCatalog() throws Exception {
    	FakeServer server = new FakeServer();
    	MetadataFactory mf = createMetadataFactory("x", new Properties());
    	
    	Table dup = mf.addTable("dup");
    	
    	mf.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup);
    	
    	MetadataStore ms = mf.getMetadataStore();
    	
    	server.deployVDB("test", ms, new LinkedHashMap<String, Resource>());
    	Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$
    	
    	Properties importProperties = new Properties();
    	importProperties.setProperty("importer.useCatalogName", Boolean.FALSE.toString());
    	mf = getMetadata(importProperties, conn);
    	Table t = mf.getMetadataStore().getSchemas().get("TEST").getTables().get("X.DUP");
    	assertEquals("\"x\".\"dup\"", t.getNameInSource());
    }

}
