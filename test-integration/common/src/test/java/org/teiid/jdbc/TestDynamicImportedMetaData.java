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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.VDBRepository;
import org.teiid.metadata.Column;
import org.teiid.metadata.DuplicateRecordException;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.NativeMetadataRepository;
import org.teiid.query.parser.QueryParser;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.oracle.OracleExecutionFactory;
import org.teiid.translator.jdbc.teiid.TeiidExecutionFactory;


/**
 */
@SuppressWarnings("nls")
public class TestDynamicImportedMetaData {

	private FakeServer server;
	
	@Before public void setup() {
		this.server = new FakeServer(true);
	}
	
	@After public void tearDown() {
		this.server.stop();
	}

	private MetadataFactory getMetadata(Properties importProperties, Connection conn)
			throws TranslatorException {
		MetadataFactory mf = createMetadataFactory("test", importProperties);
    	
    	TeiidExecutionFactory tef = new TeiidExecutionFactory();
    	tef.getMetadata(mf, conn);
    	return mf;
	}

	private MetadataFactory createMetadataFactory(String schema, Properties importProperties) {
		VDBRepository vdbRepository = new VDBRepository();
    	return new MetadataFactory("vdb", 1, schema, vdbRepository.getRuntimeTypeMap(), importProperties, null);
	}
	
	@Test public void testUniqueReferencedKey() throws Exception {
		server.deployVDB("vdb", UnitTestUtil.getTestDataPath() + "/keys.vdb");
    	Connection conn = server.createConnection("jdbc:teiid:vdb"); //$NON-NLS-1$
    	
    	Properties importProperties = new Properties();
    	importProperties.setProperty("importer.importKeys", "true");
    	importProperties.setProperty("importer.schemaPattern", "x");
    	MetadataFactory mf = getMetadata(importProperties, conn);
    	Table t = mf.asMetadataStore().getSchemas().get("TEST").getTables().get("VDB.X.A");
    	List<ForeignKey> fks = t.getForeignKeys();
    	assertEquals(1, fks.size());
    	assertNotNull(fks.get(0).getPrimaryKey());
	}
	
    @Test public void testProcImport() throws Exception {
    	server.deployVDB("vdb", UnitTestUtil.getTestDataPath() + "/TestCase3473/test.vdb");
    	Connection conn = server.createConnection("jdbc:teiid:vdb"); //$NON-NLS-1$
    	
    	Properties importProperties = new Properties();
    	importProperties.setProperty("importer.importProcedures", Boolean.TRUE.toString());
    	MetadataFactory mf = getMetadata(importProperties, conn);
    	Procedure p = mf.asMetadataStore().getSchemas().get("TEST").getProcedures().get("VDB.SYS.GETXMLSCHEMAS");
    	assertEquals(1, p.getResultSet().getColumns().size());
    }
    
    @Test public void testExcludes() throws Exception {
    	server.deployVDB("vdb", UnitTestUtil.getTestDataPath() + "/TestCase3473/test.vdb");
    	Connection conn = server.createConnection("jdbc:teiid:vdb"); //$NON-NLS-1$
    	
    	Properties importProperties = new Properties();
    	importProperties.setProperty("importer.importProcedures", Boolean.TRUE.toString());
    	importProperties.setProperty("importer.excludeTables", "VDB\\.SYS\\..*");
    	importProperties.setProperty("importer.excludeProcedures", "VDB\\..*");
    	MetadataFactory mf = getMetadata(importProperties, conn);
    	assertEquals(String.valueOf(mf.asMetadataStore().getSchemas().get("TEST").getTables()), 20, mf.asMetadataStore().getSchemas().get("TEST").getTables().size());
    	assertEquals(0, mf.asMetadataStore().getSchemas().get("TEST").getProcedures().size());
    }
        
    @Test public void testDuplicateException() throws Exception {
    	MetadataFactory mf = createMetadataFactory("x", new Properties());
    	MetadataFactory mf1 = createMetadataFactory("y", new Properties());
    	
    	Table dup = mf.addTable("dup");
    	Table dup1 = mf1.addTable("dup");
    	
    	mf.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup);
    	mf1.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup1);
    	
    	MetadataStore ms = mf.asMetadataStore();
    	ms.addSchema(mf1.asMetadataStore().getSchemas().values().iterator().next());
    	
    	server.deployVDB("test", ms);
    	Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$
    	
    	Properties importProperties = new Properties();
    	
    	mf = getMetadata(importProperties, conn);
    	Table t = mf.asMetadataStore().getSchemas().get("TEST").getTables().get("TEST.X.DUP");
    	assertEquals("\"test\".\"x\".\"dup\"", t.getNameInSource());

    	importProperties.setProperty("importer.useFullSchemaName", Boolean.FALSE.toString());
    	try {
    		getMetadata(importProperties, conn);
    		fail();
    	} catch (DuplicateRecordException e) {
    		
    	}
    }
    
    @Test public void testUseCatalog() throws Exception {
    	MetadataFactory mf = createMetadataFactory("x", new Properties());
    	
    	Table dup = mf.addTable("dup");
    	
    	mf.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup);
    	
    	MetadataStore ms = mf.asMetadataStore();
    	
    	server.deployVDB("test", ms);
    	Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$
    	
    	Properties importProperties = new Properties();
    	importProperties.setProperty("importer.useCatalogName", Boolean.FALSE.toString());
    	mf = getMetadata(importProperties, conn);
    	Table t = mf.asMetadataStore().getSchemas().get("TEST").getTables().get("X.DUP");
    	assertEquals("\"x\".\"dup\"", t.getNameInSource());
    }
    
    @Test public void testUseQualified() throws Exception {
    	MetadataFactory mf = createMetadataFactory("x", new Properties());
    	
    	Table dup = mf.addTable("dup");
    	
    	mf.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup);
    	
    	MetadataStore ms = mf.asMetadataStore();
    	
    	server.deployVDB("test", ms);
    	Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$
    	
    	//neither the name nor name in source should be qualified
    	Properties importProperties = new Properties();
    	importProperties.setProperty("importer.useQualifiedName", Boolean.FALSE.toString());
    	
    	mf = getMetadata(importProperties, conn);
    	Table t = mf.asMetadataStore().getSchemas().get("TEST").getTables().get("DUP");
    	assertEquals("\"dup\"", t.getNameInSource());
    }
    
    @Test
    public void testDDLMetadata() throws Exception {
    	String ddl = "CREATE FOREIGN PROCEDURE getTextFiles(IN pathAndPattern varchar) RETURNS (file clob, filpath string) OPTIONS(UUID 'uuid')";
    	MetadataFactory mf = createMetadataFactory("MarketData", new Properties());
    	QueryParser.getQueryParser().parseDDL(mf, ddl);
    	MetadataStore ms = mf.asMetadataStore();
    
    	String ddl2 = "CREATE VIEW stock (symbol string, price bigdecimal) OPTIONS (UUID 'uuid')" +
    			"AS select stock.* from (call MarketData.getTextFiles('*.txt')) f, " +
    			"TEXTTABLE(f.file COLUMNS symbol string, price bigdecimal HEADER) stock;";    	
    	MetadataFactory m2 = createMetadataFactory("portfolio", new Properties());

    	QueryParser.getQueryParser().parseDDL(m2, ddl2);
    	m2.getSchema().setPhysical(false);
    	m2.mergeInto(ms);
    	
    	server.deployVDB("test", ms);
    	Connection conn =  server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$
    	Properties props = new Properties();
    	props.setProperty("importer.importProcedures", Boolean.TRUE.toString());
    	MetadataStore store = getMetadata(props, conn).asMetadataStore();
    	
    	Procedure p = store.getSchema("test").getProcedure("test.MarketData.getTextFiles");
    	assertNotNull(p);
    	
    	ProcedureParameter pp = p.getParameters().get(0);
    	assertEquals("pathAndPattern", pp.getName());
    	assertEquals(ProcedureParameter.Type.In, pp.getType());
    	//assertEquals("string", pp.getDatatype().getName());
    	
    	Table t = store.getSchema("test").getTable("test.portfolio.stock");
    	assertNotNull(t);
		
    	List<Column> columns = t.getColumns();
    	assertEquals(2, columns.size());
    	assertEquals("symbol", columns.get(0).getName());
    	assertEquals("price", columns.get(1).getName());
    }    
    
    @Test public void testImportFunction() throws Exception {
    	MetadataFactory mf = createMetadataFactory("x", new Properties());
    	
    	Table dup = mf.addTable("dup");
    	
    	mf.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup);
    	
    	MetadataStore ms = mf.asMetadataStore();
    	
    	server.deployVDB("test", ms);
    	Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$
    	
    	Properties importProperties = new Properties();
    	importProperties.setProperty(NativeMetadataRepository.IMPORT_PUSHDOWN_FUNCTIONS, Boolean.TRUE.toString());
    	
    	mf = createMetadataFactory("test", importProperties);
    	NativeMetadataRepository nmr = new NativeMetadataRepository();
    	OracleExecutionFactory oef = new OracleExecutionFactory();
    	oef.start();
    	DataSource ds = Mockito.mock(DataSource.class);
    	    	
    	Mockito.stub(ds.getConnection()).toReturn(conn);
    	
    	nmr.loadMetadata(mf, oef, ds);
    	
    	Map<String, FunctionMethod> functions = mf.asMetadataStore().getSchemas().get("TEST").getFunctions();
    	
    	assertEquals(14, functions.size());
    }
}
