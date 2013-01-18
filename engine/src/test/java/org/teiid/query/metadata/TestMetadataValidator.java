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
package org.teiid.query.metadata;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Table;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.parser.TestDDLParser;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;

@SuppressWarnings("nls")
public class TestMetadataValidator {
	public static final SystemFunctionManager SFM = new SystemFunctionManager();
	private VDBMetaData vdb = new VDBMetaData();
	private MetadataStore store = new MetadataStore();
	
	@Before
	public void setUp() {
		vdb.setName("myVDB"); 
		vdb.setVersion(1);
	}

	private void buildTransformationMetadata() {
		TransformationMetadata metadata =  new TransformationMetadata(this.vdb, new CompositeMetadataStore(this.store), null, SFM.getSystemFunctions(), null);
		this.vdb.addAttchment(QueryMetadataInterface.class, metadata);
		this.vdb.addAttchment(TransformationMetadata.class, metadata);
	}

	public static ModelMetaData buildModel(String modelName, boolean physical, VDBMetaData vdb, MetadataStore store, String ddl) throws Exception {
		ModelMetaData model = new ModelMetaData();
		model.setName(modelName); 	
		model.setModelType(physical?Model.Type.PHYSICAL:Model.Type.VIRTUAL);
		vdb.addModel(model);
		
		DDLMetadataRepository repo = new DDLMetadataRepository();
		MetadataFactory mf = new MetadataFactory("myVDB",1, modelName, TestDDLParser.getDataTypes(), new Properties(), ddl);
		mf.setParser(QueryParser.getQueryParser());
		mf.setBuiltinDataTypes(SystemMetadata.getInstance().getSystemStore().getDatatypes());
		mf.getSchema().setPhysical(physical);
		repo.loadMetadata(mf, null, null);
		mf.mergeInto(store);
		model.addAttchment(MetadataFactory.class, mf);
		return model;
	}
	
	@Test
	public void testSourceModelArtifacts() throws Exception {
		String ddl = "create foreign table g1(e1 integer, e2 varchar(12)); create view g2(e1 integer, e2 varchar(12)) AS select * from foo;";
		buildModel("pm1", true, this.vdb, this.store, ddl);
		ValidatorReport report = new ValidatorReport();
		new MetadataValidator.SourceModelArtifacts().execute(vdb, store, report, new MetadataValidator());
		assertFalse(printError(report), report.hasItems());
	}

	private String printError(ValidatorReport report) {
		StringBuilder sb = new StringBuilder();
		for (ValidatorFailure v:report.getItems()) {
			if (v.getStatus() == ValidatorFailure.Status.ERROR) {
				sb.append(v);
				sb.append("\n");
			}
		}
		return sb.toString();
	}
	
	@Test
	public void testViewModelArtifacts() throws Exception {
		String ddl = "create foreign table g1(e1 integer, e2 varchar(12)); create view g2(e1 integer, e2 varchar(12)) AS select * from foo;";
		buildModel("vm1", false, this.vdb, this.store, ddl);
		ValidatorReport report = new ValidatorReport();
		new MetadataValidator.SourceModelArtifacts().execute(vdb, store, report, new MetadataValidator());
		assertTrue(printError(report), report.hasItems());
	}
	
	@Test
	public void testModelArtifactsSucess() throws Exception {
		buildModel("vm1", false, this.vdb, this.store, "create view g2(e1 integer, e2 varchar(12)) AS select * from foo;");
		buildModel("pm1", true, this.vdb, this.store, "create foreign table g1(e1 integer, e2 varchar(12));");
		ValidatorReport report = new ValidatorReport();
		new MetadataValidator.SourceModelArtifacts().execute(vdb, store, report, new MetadataValidator());
		assertFalse(printError(report), report.hasItems());	
	}
	
	@Test
	public void testMinimalDataNoColumns() throws Exception {
		buildModel("pm1", true, this.vdb, this.store, "create foreign table g1;");
		ValidatorReport report = new ValidatorReport();
		new MetadataValidator.MinimalMetadata().execute(vdb, store, report, new MetadataValidator());
		assertTrue(printError(report), report.hasItems());		
	}
	
	@Test
	public void testResolveMetadata() throws Exception {
		String ddl = "create view g1 (e1 integer, e2 varchar(12)) AS select * from pm1.g1; " +
				"create view g2 AS select * from pm1.g1; " +
				"create trigger on g1 INSTEAD OF UPDATE AS FOR EACH ROW BEGIN ATOMIC END; " +
				"create virtual procedure proc1(IN e1 varchar) RETURNS (e1 integer, e2 varchar(12)) AS select * from foo; ";
		buildModel("pm1", true, this.vdb, this.store, "create foreign table g1(e1 integer, e2 varchar(12));");
		buildModel("vm1", false, this.vdb, this.store, ddl);
		buildTransformationMetadata();
		ValidatorReport report = new ValidatorReport();
		new MetadataValidator.ResolveQueryPlans().execute(vdb, store, report, new MetadataValidator());
		assertTrue(printError(report), report.hasItems());			
	}
	
	@Test public void testProcWithMultipleReturn() throws Exception {
		String ddl = "create foreign procedure x (out param1 string result, out param2 string result); ";
		buildModel("pm1", true, this.vdb, this.store,ddl);
		buildTransformationMetadata();
		ValidatorReport report = new MetadataValidator().validate(vdb, store);
		assertTrue(printError(report), report.hasItems());			
	}
	
	@Test public void testProcWithDuplicateParam() throws Exception {
		String ddl = "create foreign procedure x (out param1 string, out param1 string); ";
		buildModel("pm1", true, this.vdb, this.store,ddl);
		buildTransformationMetadata();
		ValidatorReport report = new MetadataValidator().validate(vdb, store);
		assertTrue(printError(report), report.hasItems());			
	}
	
	@Test public void testProcMetadata() throws Exception {
		String ddl = "create virtual procedure proc1(IN e1 varchar) RETURNS (e1 integer, e2 varchar(12)) AS begin create local temporary table x (e1 integer, e2 varchar); select * from x; end;" +
		"create virtual procedure proc2(IN e1 varchar) RETURNS (e1 integer, e2 varchar(12)) AS select x.* from (exec proc1('a')) as X; ";
		buildModel("vm1", false, this.vdb, this.store, ddl);
		buildTransformationMetadata();
		ValidatorReport report = new ValidatorReport();
		new MetadataValidator.ResolveQueryPlans().execute(vdb, store, report, new MetadataValidator());
		assertFalse(printError(report), report.hasItems());			
	}
	
	@Test public void testResolveTempMetadata() throws Exception {
		String ddl = "create virtual procedure proc1() RETURNS (e1 integer, e2 varchar(12)) AS begin create local temporary table x (e1 integer, e2 varchar); select * from x; end;" +
		"create view z (e1 integer, e2 varchar(12)) AS select x.* from (exec proc1()) as X, (exec proc1()) as Y; ";
		buildModel("vm1", false, this.vdb, this.store, ddl);
		buildTransformationMetadata();
		ValidatorReport report = new ValidatorReport();
		new MetadataValidator.ResolveQueryPlans().execute(vdb, store, report, new MetadataValidator());
		assertFalse(printError(report), report.hasItems());			
	}
	
	@Test
	public void testResolveMetadataError() throws Exception {
		buildModel("vm1", false, this.vdb, this.store, "create view g1 (e1 integer, e2 varchar(12)) AS select * from pm1.g1; create view g2 AS select * from pm1.g1;");
		buildTransformationMetadata();
		ValidatorReport report = new ValidatorReport();
		new MetadataValidator.ResolveQueryPlans().execute(vdb, store, report, new MetadataValidator());
		assertTrue(printError(report), report.hasItems());		
	}
	
	@Test
	public void testCrossReferenceFK() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(g1e1 integer, g1e2 varchar, PRIMARY KEY(g1e1, g1e2));";
		String ddl2 = "CREATE FOREIGN TABLE G2( g2e1 integer, g2e2 varchar, PRIMARY KEY(g2e1, g2e2), FOREIGN KEY (g2e1, g2e2) REFERENCES pm1.G1(g1e1, g1e2))";		
		
		buildModel("pm1", true, this.vdb, this.store, ddl);
		buildModel("pm2", true, this.vdb, this.store, ddl2);
		
		buildTransformationMetadata();
		
		ValidatorReport report = new ValidatorReport();
		report = new MetadataValidator().validate(this.vdb, this.store);
		assertFalse(printError(report), report.hasItems());
		
		assertNotNull(this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getPrimaryKey());
		assertEquals(2, this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getPrimaryKey().getColumns().size());
		assertEquals("g1e1", this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getPrimaryKey().getColumns().get(0).getName());
	}
	
	@Test
	public void testCrossReferenceFKFromUniqueKey() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(g1e1 integer, g1e2 varchar, UNIQUE(g1e2));";
		String ddl2 = "CREATE FOREIGN TABLE G2(g2e1 integer, g2e2 varchar, FOREIGN KEY (g2e2) REFERENCES pm1.G1(g1e2))";		
		
		buildModel("pm1", true, this.vdb, this.store, ddl);
		buildModel("pm2", true, this.vdb, this.store, ddl2);
		
		buildTransformationMetadata();
		
		ValidatorReport report = new ValidatorReport();
		report = new MetadataValidator().validate(this.vdb, this.store);
		assertFalse(printError(report), report.hasItems());
		
		assertNotNull(this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getPrimaryKey());
		assertEquals(1, this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getPrimaryKey().getColumns().size());
		assertEquals("g1e2", this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getPrimaryKey().getColumns().get(0).getName());
	}	

	@Test
	public void testCrossReferenceResoveOptionalFK() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(g1e1 integer, g1e2 varchar, PRIMARY KEY(g1e1, g1e2));";
		String ddl2 = "CREATE FOREIGN TABLE G2( g2e1 integer, g2e2 varchar, FOREIGN KEY (g2e1, g2e2) REFERENCES pm1.G1)";		
		
		buildModel("pm1", true, this.vdb, this.store, ddl);
		buildModel("pm2", true, this.vdb, this.store, ddl2);
		
		buildTransformationMetadata();
		
		ValidatorReport report = new ValidatorReport();
		report = new MetadataValidator().validate(this.vdb, this.store);
		assertFalse(printError(report), report.hasItems());
		
		assertNotNull(this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getPrimaryKey());
		assertEquals(2, this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getPrimaryKey().getColumns().size());
		assertEquals("g1e1", this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getPrimaryKey().getColumns().get(0).getName());
	}
	
	@Test
	public void testCrossReferenceFKNoPKonRefTable() throws Exception {
		// note here the unique here does not matter for non-existent reference columns, only primary key counted.
		String ddl = "CREATE FOREIGN TABLE G1(g1e1 integer, g1e2 varchar, UNIQUE(g1e1, g1e2));";
		String ddl2 = "CREATE FOREIGN TABLE G2( g2e1 integer, g2e2 varchar, PRIMARY KEY(g2e1, g2e2), FOREIGN KEY (g2e1, g2e2) REFERENCES pm1.G1)";		
		
		buildModel("pm1", true, this.vdb, this.store, ddl);
		buildModel("pm2", true, this.vdb, this.store, ddl2);
		
		buildTransformationMetadata();
		
		ValidatorReport report = new ValidatorReport();
		report = new MetadataValidator().validate(this.vdb, this.store);
		assertTrue(printError(report), report.hasItems());
	}	
	
	
	@Test
	public void testInternalMaterializationValidate() throws Exception {
		// note here the unique here does not matter for non-existent reference columns, only primary key counted.
		String ddl = "CREATE FOREIGN TABLE G1(e1 integer, e2 varchar);";
		String ddl2 = "CREATE VIEW G2 OPTIONS (MATERIALIZED 'YES') AS SELECT * FROM pm1.G1";		
		
		buildModel("pm1", true, this.vdb, this.store, ddl);
		buildModel("vm1", false, this.vdb, this.store, ddl2);
		
		buildTransformationMetadata();
		
		ValidatorReport report = new ValidatorReport();
		report = new MetadataValidator().validate(this.vdb, this.store);
		assertFalse(printError(report), report.hasItems());
	}	
	
	@Test
	public void testExternalMaterializationValidate() throws Exception {
		// note here the unique here does not matter for non-existent reference columns, only primary key counted.
		String ddl = "CREATE FOREIGN TABLE G1(e1 integer, e2 varchar);";
		String ddl2 = "CREATE VIEW G2 OPTIONS (MATERIALIZED 'true', MATERIALIZED_TABLE 'pm1.G1') AS SELECT * FROM pm1.G1";		
		
		buildModel("pm1", true, this.vdb, this.store, ddl);
		buildModel("vm1", false, this.vdb, this.store, ddl2);
		
		buildTransformationMetadata();
		
		ValidatorReport report = new ValidatorReport();
		report = new MetadataValidator().validate(this.vdb, this.store);
		assertFalse(printError(report), report.hasItems());
		assertNotNull("pm1.G1", store.getSchema("vm1").getTable("G2").getMaterializedTable());
		assertEquals("G1", store.getSchema("vm1").getTable("G2").getMaterializedTable().getName());
	}	
	
	
	@Test
	public void testSkipDocumentModel() throws Exception {
		ModelMetaData model = new ModelMetaData();
		model.setName("xmlstuff"); 	
		model.setModelType(Model.Type.VIRTUAL);
		vdb.addModel(model);
		
		MetadataFactory mf = new MetadataFactory("myVDB",1, "xmlstuff", TestDDLParser.getDataTypes(), new Properties(), null);
		mf.getSchema().setPhysical(false);
		
		Table t = mf.addTable("xmldoctable");
		t.setTableType(Table.Type.Document);
		mf.addColumn("c1", "string", t);
		t.setSelectTransformation("some dummy stuff, should not be validated");
		t.setVirtual(true);
		
		Table t2 = mf.addTable("xmldoctable2");
		t2.setTableType(Table.Type.XmlMappingClass);
		mf.addColumn("c1", "string", t2);
		t2.setSelectTransformation("some dummy stuff, should not be validated");	
		t2.setVirtual(true);
		mf.mergeInto(store);	
		
		buildTransformationMetadata();
		
		ValidatorReport report = new MetadataValidator().validate(this.vdb, this.store);
		assertFalse(printError(report), report.hasItems());
	}	
	
	@Test public void testInvalidVarArgs() throws Exception {
		// note here the unique here does not matter for non-existent reference columns, only primary key counted.
		String ddl = "CREATE FOREIGN FUNCTION f1(VARIADIC e1 integer, e2 varchar) RETURNS varchar;";
		helpTest(ddl, true);
	}
	
	@Test public void testFBIResolveError() throws Exception {
		String ddl = "CREATE view G1(e1 integer, e2 varchar, CONSTRAINT fbi INDEX (UPPER(e3))) options (materialized true) as select 1, 'a'";
		helpTest(ddl, true);
	}
	
	@Test public void testFBISubquery() throws Exception {
		String ddl = "CREATE view G1(e1 integer, e2 varchar, CONSTRAINT fbi INDEX ((select 1))) options (materialized true) as select 1, 'a'";
		helpTest(ddl, true);
	}
	
    @Test public void testResultSet() throws Exception {
    	String ddl = "create virtual procedure vproc (x integer) returns table (y integer) as begin if (x = 1) select 1; else select 1, 2; end;";
    	helpTest(ddl, true);
    }
    
    @Test public void testReturnResolving() throws Exception {
    	String ddl = "create procedure proc (x integer) returns string as return x;\n";
		helpTest(ddl, false);
    }
    
    @Test public void testReturnResolving1() throws Exception {
    	String ddl = "create procedure proc (x integer) as return x;\n";
		helpTest(ddl, true);
    }
    
    @Test public void testViewKeys() throws Exception {
    	buildModel("phy1", true, this.vdb, this.store, "CREATE FOREIGN TABLE t1 ( col1 string, col2 integer ) options (updatable true)");
    	buildModel("phy2", true, this.vdb, this.store, "CREATE FOREIGN TABLE t2 ( col1 string, col2 integer ) options (updatable true)");
    	buildModel("view1", false, this.vdb, this.store, "CREATE view vw_t1 ( col1 string, col2 integer primary key, foreign key (col2) references vw_t2 (col2) ) options (updatable true) as select * from t1;" +
				"CREATE view vw_t2 ( col1 string, col2 integer primary key, foreign key (col2) references vw_t1 (col2) ) options (updatable true) as select * from t2;" +
				"CREATE VIEW v1 ( col1 string, col2 integer ) OPTIONS (updatable 'true') AS select vw_t1.col1, vw_t1.col2 FROM vw_t1, vw_t2 where vw_t1.col2 = vw_t2.col2");
    	
		buildTransformationMetadata();
		
		ValidatorReport report = new MetadataValidator().validate(this.vdb, this.store);
		assertFalse(printError(report), report.hasItems());
    }
    
    @Test public void testResolvingOrder() throws Exception {
    	buildModel("phy1", true, this.vdb, this.store, "CREATE FOREIGN TABLE t1 ( col1 string, col2 integer ) options (updatable true); CREATE view a as select * from t1;");
    	
		buildTransformationMetadata();
		
		ValidatorReport report = new MetadataValidator().validate(this.vdb, this.store);
		assertFalse(printError(report), report.hasItems());
    }

	private ValidatorReport helpTest(String ddl, boolean expectErrors) throws Exception {
		buildModel("pm1", true, this.vdb, this.store, ddl);
		
		buildTransformationMetadata();
		
		ValidatorReport report = new ValidatorReport();
		report = new MetadataValidator().validate(this.vdb, this.store);
		if (expectErrors) {
			assertTrue(printError(report), report.hasItems());
		} else {
			assertFalse(printError(report), report.hasItems());
		}
		return report;
	}

    
}
