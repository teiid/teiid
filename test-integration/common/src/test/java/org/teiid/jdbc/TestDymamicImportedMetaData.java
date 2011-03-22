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
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.VDBRepository;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.teiid.TeiidExecutionFactory;


/**
 */
@SuppressWarnings("nls")
public class TestDymamicImportedMetaData {

	Connection conn;
    
    ////////////////////Query Related Methods///////////////////////////

    @Before public void setUp() throws Exception {
    	FakeServer server = new FakeServer();
    	server.deployVDB("test", UnitTestUtil.getTestDataPath() + "/TestCase3473/test.vdb");
    	conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$
    }

	private MetadataFactory getMetadata(Properties importProperties)
			throws TranslatorException {
		VDBRepository vdbRepository = new VDBRepository();
    	vdbRepository.setSystemStore(VDBMetadataFactory.getSystem());
    	
    	TeiidExecutionFactory tef = new TeiidExecutionFactory();
    	MetadataFactory mf = new MetadataFactory("test", vdbRepository.getBuiltinDatatypes(), importProperties);
    	tef.getMetadata(mf, conn);
    	return mf;
	}
	
    @Test public void testProcImport() throws Exception {
    	Properties importProperties = new Properties();
    	importProperties.setProperty("importer.importProcedures", Boolean.TRUE.toString());
    	MetadataFactory mf = getMetadata(importProperties);
    	Procedure p = mf.getMetadataStore().getSchemas().get("TEST").getProcedures().get("GETXMLSCHEMAS");
    	assertEquals(1, p.getResultSet().getColumns().size());
    }
}
