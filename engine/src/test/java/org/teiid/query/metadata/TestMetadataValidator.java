/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
	public static final SystemFunctionManager SFM = SystemMetadata.getInstance().getSystemFunctionManager();
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
		mf.getSchema().setPhysical(physical);
		repo.loadMetadata(mf, null, null, ddl);
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
	
    @Test
    public void testInvalidView() throws Exception {
        String ddl = "create view g1 (e1 integer, e2 varchar(12)) AS select 'a';";
        buildModel("vm1", false, this.vdb, this.store, ddl);
        buildTransformationMetadata();
        ValidatorReport report = new ValidatorReport();
        new MetadataValidator.ResolveQueryPlans().execute(vdb, store, report, new MetadataValidator());
        assertTrue(printError(report), report.hasItems());          
    }
	
	@Test
	public void testCreateTrigger() throws Exception {
		String ddl = "create view g1 options (updatable true) AS select * from pm1.g1; " +
				"create trigger on g1 INSTEAD OF UPDATE AS FOR EACH ROW BEGIN ATOMIC END; ";
		buildModel("pm1", true, this.vdb, this.store, "create foreign table g1(e1 integer, e2 varchar(12));");
		buildModel("vm1", false, this.vdb, this.store, ddl);
		buildTransformationMetadata();
		ValidatorReport report = new MetadataValidator().validate(vdb, store);
		assertFalse(printError(report), report.hasItems());			
	}
	
	@Test
	public void testCreateTriggerFails() throws Exception {
		String ddl = "create view g1 options (updatable true) AS select * from pm1.g1; " +
				"create trigger on g1 instead of update as for each row begin if (\"new\" is distinct from pm1.g1) select 1; END; ";
		buildModel("pm1", true, this.vdb, this.store, "create foreign table g1(e1 integer, e2 varchar(12));");
		buildModel("vm1", false, this.vdb, this.store, ddl);
		buildTransformationMetadata();
		ValidatorReport report = new MetadataValidator().validate(vdb, store);
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
	
	@Test public void testProcMetadataValidationError() throws Exception {
		String ddl = "create virtual procedure proc1(IN e1 varchar) RETURNS (e1 integer, e2 varchar(12)) AS begin create local temporary table x (e1 integer, e2 varchar not null); insert into x (e1) values (1); select * from x; end;";
		buildModel("vm1", false, this.vdb, this.store, ddl);
		buildTransformationMetadata();
		ValidatorReport report = new ValidatorReport();
		new MetadataValidator.ResolveQueryPlans().execute(vdb, store, report, new MetadataValidator());
		assertEquals("TEIID31080 vm1.proc1 validation error: Element x.e2 of x is neither nullable nor has a default value. A value must be specified in the insert.", report.getItems().iterator().next().toString());			
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
		
		assertNotNull(this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getReferenceKey());
		assertEquals(2, this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getReferenceKey().getColumns().size());
		assertEquals("g1e1", this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getReferenceKey().getColumns().get(0).getName());
	}
	
	@Test
	public void testEmptyKey() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(g1e1 integer, g1e2 varchar, PRIMARY KEY(g1e1, g1e2));";
		
		buildModel("pm1", true, this.vdb, this.store, ddl);
		
		buildTransformationMetadata();
		
		this.store.getSchema("pm1").getTable("G1").getPrimaryKey().getColumns().clear();
		
		ValidatorReport report = new ValidatorReport();
		report = new MetadataValidator().validate(this.vdb, this.store);
		assertTrue(printError(report), report.hasItems());
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
		
		assertNotNull(this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getReferenceKey());
		assertEquals(1, this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getReferenceKey().getColumns().size());
		assertEquals("g1e2", this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getReferenceKey().getColumns().get(0).getName());
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
		
		assertNotNull(this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getReferenceKey());
		assertEquals(2, this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getReferenceKey().getColumns().size());
		assertEquals("g1e1", this.store.getSchema("pm2").getTable("G2").getForeignKeys().get(0).getReferenceKey().getColumns().get(0).getName());
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
		String ddl = "CREATE FOREIGN TABLE G1(e1 integer, e2 varchar);"
				+ "create foreign table status (VDBNAME STRING, VDBVERSION STRING, "
				+ " SCHEMANAME STRING, NAME STRING, TARGETSCHEMANAME STRING, TARGETNAME STRING, "
				+ " VALID BOOLEAN, LOADSTATE STRING, CARDINALITY LONG, UPDATED TIMESTAMP, LOADNUMBER LONG, NODENAME STRING, STALECOUNT LONG)";
		String ddl2 = "CREATE VIEW G2 OPTIONS (MATERIALIZED 'true', MATERIALIZED_TABLE 'pm1.G1', \"teiid_rel:MATVIEW_STATUS_TABLE\" 'pm1.status', \"teiid_rel:MATVIEW_LOAD_SCRIPT\" 'begin end') AS SELECT * FROM pm1.G1";		
		
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
	public void testExternalMaterializationValidateColumns() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(e2 varchar);"
				+ "create foreign table status (VDBNAME STRING, VDBVERSION STRING, "
				+ " SCHEMANAME STRING, NAME STRING, TARGETSCHEMANAME STRING, TARGETNAME STRING, "
				+ " VALID BOOLEAN, LOADSTATE STRING, CARDINALITY LONG, UPDATED TIMESTAMP, LOADNUMBER LONG, NODENAME STRING, STALECOUNT LONG)";
		String ddl2 = "CREATE VIEW G2 (e1 integer, e2 varchar) OPTIONS (MATERIALIZED 'true', MATERIALIZED_TABLE 'pm1.G1', \"teiid_rel:MATVIEW_STATUS_TABLE\" 'pm1.status', \"teiid_rel:MATVIEW_LOAD_SCRIPT\" 'begin end') AS SELECT 1, 'a' FROM pm1.G1";		
		
		buildModel("pm1", true, this.vdb, this.store, ddl);
		buildModel("vm1", false, this.vdb, this.store, ddl2);
		
		buildTransformationMetadata();
		
		ValidatorReport report = new ValidatorReport();
		report = new MetadataValidator().validate(this.vdb, this.store);
		assertTrue(printError(report), report.hasItems());
	}
	
	@Test
	public void testExternalMaterializationValidateColumnTypes() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(e1 integer, e2 integer);"
				+ "create foreign table status (VDBNAME STRING, VDBVERSION STRING, "
				+ " SCHEMANAME STRING, NAME STRING, TARGETSCHEMANAME STRING, TARGETNAME STRING, "
				+ " VALID BOOLEAN, LOADSTATE STRING, CARDINALITY LONG, UPDATED TIMESTAMP, LOADNUMBER LONG, NODENAME STRING, STALECOUNT STRING)";
		String ddl2 = "CREATE VIEW G2 (e1 integer, e2 varchar) OPTIONS (MATERIALIZED 'true', MATERIALIZED_TABLE 'pm1.G1', \"teiid_rel:MATVIEW_STATUS_TABLE\" 'pm1.status', \"teiid_rel:MATVIEW_LOAD_SCRIPT\" 'begin end') AS SELECT 1, 'a' FROM pm1.G1";		
		
		buildModel("pm1", true, this.vdb, this.store, ddl);
		buildModel("vm1", false, this.vdb, this.store, ddl2);
		
		buildTransformationMetadata();
		
		ValidatorReport report = new ValidatorReport();
		report = new MetadataValidator().validate(this.vdb, this.store);
		assertTrue(printError(report), report.hasItems());
	}
	
	@Test
	public void testExternalMaterializationValidateMissingStatus() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(e1 integer, e2 varchar); CREATE FOREIGN TABLE status(e1 integer, e2 varchar);";
		String ddl2 = "CREATE VIEW G2 OPTIONS (MATERIALIZED 'true', MATERIALIZED_TABLE 'pm1.G1', \"teiid_rel:MATVIEW_STATUS_TABLE\" 'x') AS SELECT * FROM pm1.G1";		
		
		buildModel("pm1", true, this.vdb, this.store, ddl);
		buildModel("vm1", false, this.vdb, this.store, ddl2);
		
		buildTransformationMetadata();
		
		ValidatorReport report = new ValidatorReport();
		report = new MetadataValidator().validate(this.vdb, this.store);
		assertTrue(printError(report), report.hasItems());
	}
	
	@Test
	public void testExternalMaterializationValidateMissingColumns() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(e1 integer, e2 varchar); CREATE FOREIGN TABLE status(e1 integer, e2 varchar);";
		String ddl2 = "CREATE VIEW G2 OPTIONS (MATERIALIZED 'true', MATERIALIZED_TABLE 'pm1.G1', \"teiid_rel:MATVIEW_STATUS_TABLE\" 'status') AS SELECT * FROM pm1.G1";		
		
		buildModel("pm1", true, this.vdb, this.store, ddl);
		buildModel("vm1", false, this.vdb, this.store, ddl2);
		
		buildTransformationMetadata();
		
		ValidatorReport report = new ValidatorReport();
		report = new MetadataValidator().validate(this.vdb, this.store);
		assertTrue(printError(report), report.hasItems());
	}
	
	@Test
    public void testExternalMaterializationValidateLoadScripts() throws Exception {
	    String ddl = "CREATE FOREIGN TABLE G1(e1 integer, e2 varchar); CREATE FOREIGN TABLE status(e1 integer, e2 varchar);";
        String ddl2 = "CREATE VIEW G2 OPTIONS (MATERIALIZED 'true', MATERIALIZED_TABLE 'pm1.G1', \"teiid_rel:MATVIEW_STATUS_TABLE\" 'status' , \"teiid_rel:MATVIEW_BEFORE_LOAD_SCRIPT\" '----') AS SELECT * FROM pm1.G1";     
        
        
        buildModel("pm1", true, this.vdb, this.store, ddl);
        buildModel("vm1", false, this.vdb, this.store, ddl2);
        
        buildTransformationMetadata();
        
        ValidatorReport report = new ValidatorReport();
        report = new MetadataValidator().validate(this.vdb, this.store);
        assertTrue(printError(report), report.hasItems());
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
	
	@Test public void testVirtualFunction() throws Exception {
		String ddl = "CREATE VIRTUAL FUNCTION f1(VARIADIC e1 integer) RETURNS integer as return array_length(e1);";
		helpTest(ddl, false);
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
    
    @Test public void testConstraintNames() throws Exception {
    	buildModel("phy1", true, this.vdb, this.store, "CREATE FOREIGN TABLE t1 ( col1 string, col2 integer, constraint x primary key (col1), constraint x unique (col2) )");
    	
		buildTransformationMetadata();
		
		ValidatorReport report = new MetadataValidator().validate(this.vdb, this.store);
		assertTrue(printError(report), report.hasItems());
    }
    
    @Test public void testResolvingOrder() throws Exception {
    	buildModel("phy1", true, this.vdb, this.store, "CREATE FOREIGN TABLE t1 ( col1 string, col2 integer ) options (updatable true); CREATE view a as select * from t1;");
    	
		buildTransformationMetadata();
		
		ValidatorReport report = new MetadataValidator().validate(this.vdb, this.store);
		assertFalse(printError(report), report.hasItems());
    }
    
    @Test public void testFunctionProcedureValidation() throws Exception {
    	buildModel("phy1", true, this.vdb, this.store, "CREATE VIRTUAL FUNCTION f1(VARIADIC x integer) RETURNS integer as return (select e1 from g1 where e2 = array_length(x));; create foreign table g1 (e1 string, e2 integer);");
    	
		buildTransformationMetadata();
		
		ValidatorReport report = new MetadataValidator().validate(this.vdb, this.store);
		assertTrue(printError(report), report.hasItems());
    }
    
    @Test public void testAfterTrigger() throws Exception {
        String ddl = "CREATE FOREIGN TABLE T ( e1 integer, e2 varchar);" +
                "CREATE TRIGGER tr ON T AFTER UPDATE AS " +
                "FOR EACH ROW \n" +
                "BEGIN ATOMIC \n" +
                "if (\"new\" is not distinct from \"old\") raise sqlexception 'error';\n" +
                "END;";
        buildModel("phy1", true, this.vdb, this.store, ddl);
        
        buildTransformationMetadata();
        
        ValidatorReport report = new MetadataValidator().validate(this.vdb, this.store);
        assertFalse(printError(report), report.hasItems());
    }
    
    @Test public void testAfterTriggerFails() throws Exception {
        String ddl = "CREATE FOREIGN TABLE T ( e1 integer, e2 varchar);" +
                "CREATE TRIGGER tr ON T AFTER INSERT AS " +
                "FOR EACH ROW \n" +
                "BEGIN ATOMIC \n" +
                " raise sqlexception old.e1;\n" +
                "END;";
        buildModel("phy1", true, this.vdb, this.store, ddl);
        
        buildTransformationMetadata();
        
        ValidatorReport report = new MetadataValidator().validate(this.vdb, this.store);
        //old is not resolvable with insert
        assertTrue(printError(report), report.hasItems());
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
