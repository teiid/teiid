package org.teiid.query.parser;
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
import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.*;
import org.teiid.metadata.Column.SearchType;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.validator.ValidatorReport;

//import static org.junit.Assert.*;

@SuppressWarnings("nls")
public class TestDDLParser {
	private QueryParser parser = new QueryParser();
	
	@Test
	public void testForeignTable() throws Exception {
		
		String ddl = "CREATE FOREIGN TABLE G1(\n" +
						"e1 integer primary key,\n" +
						"e2 varchar(10) unique,\n" +
						"e3 date not null unique,\n" +
						"e4 decimal(12,3) options (searchable 'unsearchable'),\n" +
						"e5 integer auto_increment INDEX OPTIONS (UUID 'uuid', NAMEINSOURCE 'nis', SELECTABLE 'NO'),\n" +
						"e6 varchar index default 'hello')\n" +
						"OPTIONS (CARDINALITY 12, UUID 'uuid2',  UPDATABLE 'true', FOO 'BAR', ANNOTATION 'Test Table')";
				
		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		Table table = tableMap.get("G1");
		assertTrue(table.isPhysical());
		assertFalse(table.isVirtual());
		assertFalse(table.isSystem());
		assertFalse(table.isMaterialized());
		assertFalse(table.isDeletePlanEnabled());
		assertEquals("uuid2", table.getUUID());
		assertEquals(12, table.getCardinality());
		assertTrue(table.supportsUpdate());
		assertEquals("BAR", table.getProperties().get("FOO"));
		assertEquals("Test Table", table.getAnnotation());
		
		
		assertEquals(6, table.getColumns().size());
		
		List<Column> columns = table.getColumns();
		Column e1 = columns.get(0);
		Column e2 = columns.get(1);
		Column e3 = columns.get(2);
		Column e4 = columns.get(3);
		Column e5 = columns.get(4);
		Column e6 = columns.get(5);
		
		assertEquals("e1", e1.getName());
		assertEquals("int", e1.getDatatype().getName());
		assertEquals("primary key not same", e1, table.getPrimaryKey().getColumns().get(0));
		
		assertEquals("e2", e2.getName());
		assertEquals("string", e2.getDatatype().getName());
		assertEquals("unique", e2, table.getUniqueKeys().get(0).getColumns().get(0));
		assertEquals(NullType.Nullable, e2.getNullType());
		assertEquals(10, e2.getLength());
		assertEquals(0, e2.getPrecision());
		
		assertEquals("e3", e3.getName());
		assertEquals("date", e3.getDatatype().getName());
		assertEquals("unique", e3, table.getUniqueKeys().get(1).getColumns().get(0));
		assertEquals(NullType.No_Nulls, e3.getNullType());		
		
		assertEquals("e4", e4.getName());
		assertEquals("bigdecimal", e4.getDatatype().getName());
		assertEquals(false, e4.isAutoIncremented());
		assertEquals(12, e4.getPrecision());
		assertEquals(3, e4.getScale());
		assertEquals(SearchType.Unsearchable, e4.getSearchType());
		
		assertEquals("e5", e5.getName());
		assertEquals("int", e5.getDatatype().getName());
		assertEquals(true, e5.isAutoIncremented());
		assertEquals("uuid", e5.getUUID());
		assertEquals("nis", e5.getNameInSource());
		assertEquals(false, e5.isSelectable());
		assertEquals("index", e5, table.getIndexes().get(0).getColumns().get(0));
		
		assertEquals("e6", e6.getName());
		assertEquals("string", e6.getDatatype().getName());
		assertEquals("index", e6, table.getIndexes().get(1).getColumns().get(0));
		assertEquals("hello", e6.getDefaultValue());
	}
	
	@Test(expected=MetadataException.class)
	public void testDuplicatePrimarykey() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer primary key, e2 varchar primary key)";
		MetadataStore mds = new MetadataStore();
		MetadataFactory mf = new MetadataFactory(null, 1, "model", getDataTypes(), new Properties(), null); 
		parser.parseDDL(mf, ddl);
		mf.mergeInto(mds);
	}
	
	@Test
	public void testUDT() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar OPTIONS (UDT 'NMTOKENS(12,13,14)'))";

		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		Table table = tableMap.get("G1");
		
		assertEquals("NMTOKENS", table.getColumns().get(1).getDatatype().getName());
		assertEquals(12, table.getColumns().get(1).getLength());
		assertEquals(13, table.getColumns().get(1).getPrecision());
		assertEquals(14, table.getColumns().get(1).getScale());		
	}	
	
	@Test public void testFBI() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(e1 integer, e2 varchar, CONSTRAINT fbi INDEX (UPPER(e2)))";

		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		
		Table table = tableMap.get("G1");
		
		assertEquals(1, table.getFunctionBasedIndexes().size());
	}
	
	@Test
	public void testMultiKeyPK() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date, PRIMARY KEY (e1, e2))";

		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		Table table = tableMap.get("G1");
		
		assertEquals(table.getColumns().subList(0, 2), table.getPrimaryKey().getColumns());
	}
	
	@Test
	public void testOptionsKey() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date, UNIQUE (e1) OPTIONS (CUSTOM_PROP 'VALUE'))";
		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		Table table = tableMap.get("G1");
		KeyRecord record = table.getAllKeys().iterator().next();
		assertEquals("VALUE", record.getProperty("CUSTOM_PROP", false));
	}
	
	@Test
	public void testConstraints() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date, " +
				" PRIMARY KEY (e1, e2), INDEX(e2, e3), ACCESSPATTERN(e1), UNIQUE(e1)," +
				" ACCESSPATTERN(e2, e3))";

		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		Table table = tableMap.get("G1");
		
		assertEquals(table.getColumns().subList(0, 2), table.getPrimaryKey().getColumns());
		assertEquals(table.getColumns().subList(1, 3), table.getIndexes().get(0).getColumns());
		assertEquals(table.getColumns().subList(0, 1), table.getUniqueKeys().get(0).getColumns());
		assertEquals(2, table.getAccessPatterns().size());
		assertEquals(table.getColumns().subList(0, 1), table.getAccessPatterns().get(0).getColumns());
		assertEquals(table.getColumns().subList(1, 3), table.getAccessPatterns().get(1).getColumns());
	}	
	
	@Test
	public void testConstraints2() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date, " +
				"ACCESSPATTERN(e1), UNIQUE(e1), ACCESSPATTERN(e2, e3))";

		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		Table table = tableMap.get("G1");
		
		assertEquals(table.getColumns().subList(0, 1), table.getUniqueKeys().get(0).getColumns());
		assertEquals(2, table.getAccessPatterns().size());
		assertEquals(table.getColumns().subList(0, 1), table.getAccessPatterns().get(0).getColumns());
		assertEquals(table.getColumns().subList(1, 3), table.getAccessPatterns().get(1).getColumns());
	}	
	
	@Test(expected=MetadataException.class)
	public void testWrongPrimarykey() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, PRIMARY KEY (e3))";

		MetadataStore mds = new MetadataStore();
		MetadataFactory mf = new MetadataFactory(null, 1, "model", getDataTypes(), new Properties(), null); 
		parser.parseDDL(mf, ddl);
		mf.mergeInto(mds);
	}	

	@Test
	public void testFK() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(g1e1 integer, g1e2 varchar, PRIMARY KEY(g1e1, g1e2));\n" +
				"CREATE FOREIGN TABLE G2( g2e1 integer, g2e2 varchar, " +
				"FOREIGN KEY (g2e1, g2e2) REFERENCES G1 (g1e1, g1e2))";
		
		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		assertEquals(2, tableMap.size());
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		assertTrue("Table not found", tableMap.containsKey("G2"));
		
		Table table = tableMap.get("G2");
		ForeignKey fk = table.getForeignKeys().get(0);
		assertEquals(fk.getColumns(), table.getColumns());
		assertEquals("G1", fk.getReferenceTableName());
	}	
	
	@Test
	public void testOptionalFK() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(g1e1 integer, g1e2 varchar, PRIMARY KEY(g1e1, g1e2));\n" +
				"CREATE FOREIGN TABLE G2( g2e1 integer, g2e2 varchar, PRIMARY KEY(g2e1, g2e2)," +
				"FOREIGN KEY (g2e1, g2e2) REFERENCES G1)";
		
		MetadataFactory s = helpParse(ddl, "model");
		Map<String, Table> tableMap = s.getSchema().getTables();	
		assertEquals(2, tableMap.size());
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		assertTrue("Table not found", tableMap.containsKey("G2"));
		
		Table table = tableMap.get("G2");
		ForeignKey fk = table.getForeignKeys().get(0);
		assertEquals(fk.getColumns(), table.getColumns());
		assertEquals("G1", fk.getReferenceTableName());
		
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("myVDB"); //$NON-NLS-1$
		ModelMetaData modelOne = new ModelMetaData();
		modelOne.setName("model"); //$NON-NLS-1$
		vdb.addModel(modelOne);

		ValidatorReport report = new MetadataValidator().validate(vdb, s.asMetadataStore());
		
		assertFalse(report.hasItems());
		
		assertEquals(fk.getPrimaryKey().getColumns(), tableMap.get("G1").getColumns());
	}	
	
	@Test
	public void testOptionalFKFail() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(g1e1 integer, g1e2 varchar);\n" +
				"CREATE FOREIGN TABLE G2( g2e1 integer, g2e2 varchar, PRIMARY KEY(g2e1, g2e2)," +
				"FOREIGN KEY (g2e1, g2e2) REFERENCES G1)";
		
		MetadataFactory s = helpParse(ddl, "model");
		Map<String, Table> tableMap = s.getSchema().getTables();	
		assertEquals(2, tableMap.size());
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		assertTrue("Table not found", tableMap.containsKey("G2"));
		
		Table table = tableMap.get("G2");
		ForeignKey fk = table.getForeignKeys().get(0);
		assertEquals(fk.getColumns(), table.getColumns());
		assertEquals("G1", fk.getReferenceTableName());
		
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("myVDB"); //$NON-NLS-1$
		ModelMetaData modelOne = new ModelMetaData();
		modelOne.setName("model"); //$NON-NLS-1$
		vdb.addModel(modelOne);

		ValidatorReport report = new MetadataValidator().validate(vdb, s.asMetadataStore());
		
		assertTrue(report.hasItems());
	}	
	
	@Test
	public void testFKAccrossSchemas() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(g1e1 integer, g1e2 varchar, PRIMARY KEY(g1e1, g1e2));\n";
		
		String ddl2 = "CREATE FOREIGN TABLE G2( g2e1 integer, g2e2 varchar, PRIMARY KEY(g2e1, g2e2)," +
				"FOREIGN KEY (g2e1, g2e2) REFERENCES model.G1)";		
		
		MetadataFactory f1 = helpParse(ddl, "model");
		MetadataFactory f2 = helpParse(ddl2, "model2");

		
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("myVDB"); //$NON-NLS-1$
		ModelMetaData modelOne = new ModelMetaData();
		modelOne.setName("model"); //$NON-NLS-1$
		vdb.addModel(modelOne);
		
		ModelMetaData modelTwo = new ModelMetaData();
		modelTwo.setName("model2"); //$NON-NLS-1$
		vdb.addModel(modelTwo);		
		
		MetadataStore s = f1.asMetadataStore();
		f2.mergeInto(s);

		ValidatorReport report = new MetadataValidator().validate(vdb, s);
		
		assertFalse(report.hasItems());
		
		Table table = s.getSchema("model2").getTable("G2");
		ForeignKey fk = table.getForeignKeys().get(0);
		assertEquals(fk.getColumns(), table.getColumns());
		assertEquals("model.G1", fk.getReferenceTableName());
		
		assertEquals(fk.getPrimaryKey().getColumns(), s.getSchema("model").getTable("G1").getColumns());
	}	
	
	@Test(expected=MetadataException.class)
	public void testTableWithPlan() throws Exception {
		String ddl = "CREATE foreign table G1 as select 1";
		MetadataStore mds = new MetadataStore();
		MetadataFactory mf = new MetadataFactory(null, 1, "model", getDataTypes(), new Properties(), null); 
		parser.parseDDL(mf,ddl);
		mf.mergeInto(mds);
	}	
	
	@Test
	public void testViewWithoutColumns() throws Exception {
		MetadataStore mds = new MetadataStore();			
		MetadataFactory mf = new MetadataFactory(null, 1, "VM1", getDataTypes(), new Properties(), null); 
		parser.parseDDL(mf,"CREATE VIEW V1 AS SELECT * FROM PM1.G1");			
		mf.mergeInto(mds);
	}	
	
	@Test
	public void testMultipleCommands() throws Exception {
		String ddl = "CREATE VIEW V1 AS SELECT * FROM PM1.G1 " +
				"CREATE PROCEDURE FOO(P1 integer) RETURNS (e1 integer, e2 varchar) AS SELECT * FROM PM1.G1;";
		
		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();
		Table table = tableMap.get("V1");
		assertNotNull(table);
		assertEquals("SELECT * FROM PM1.G1", table.getSelectTransformation());
		
		Map<String, Procedure> procedureMap = s.getProcedures();
		Procedure p = procedureMap.get("FOO");
		assertNotNull(p);
		assertEquals("SELECT * FROM PM1.G1;", p.getQueryPlan());
		
	}		
	
	@Test
	public void testMultipleCommands2() throws Exception {
		String ddl = "             CREATE VIRTUAL PROCEDURE getTweets(query varchar) RETURNS (created_on varchar(25), from_user varchar(25), to_user varchar(25), \n" + 
				"                 profile_image_url varchar(25), source varchar(25), text varchar(140)) AS \n" + 
				"                select tweet.* from \n" + 
				"	                (call twitter.invokeHTTP(action => 'GET', endpoint =>querystring('',query as \"q\"))) w, \n" + 
				"	                XMLTABLE('results' passing JSONTOXML('myxml', w.result) columns \n" + 
				"	                created_on string PATH 'created_at', \n" + 
				"	                from_user string PATH 'from_user',\n" + 
				"	                to_user string PATH 'to_user',	\n" + 
				"	                profile_image_url string PATH 'profile_image_url',	\n" + 
				"	                source string PATH 'source',	\n" + 
				"	                text string PATH 'text') tweet;" + 
				"                CREATE VIEW Tweet AS select * FROM twitterview.getTweets;";
		
		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();
		Table table = tableMap.get("Tweet");
		assertNotNull(table);
		
		Map<String, Procedure> procedureMap = s.getProcedures();
		Procedure p = procedureMap.get("getTweets");
		assertNotNull(p);
		
	}	
	
	@Test
	public void testView() throws Exception {
		String ddl = "CREATE View G1( e1 integer, e2 varchar) OPTIONS (CARDINALITY 12) AS select e1, e2 from foo.bar";

		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		
		Table table = tableMap.get("G1");
		
		assertEquals("SELECT e1, e2 FROM foo.bar", table.getSelectTransformation());
		assertEquals(12, table.getCardinality());
	}	
	
	@Test
	public void testPushdownFunctionNoArgs() throws Exception {
		String ddl = "CREATE FOREIGN FUNCTION SourceFunc() RETURNS integer OPTIONS (UUID 'hello world')";

		Schema s = helpParse(ddl, "model").getSchema();
		
		FunctionMethod fm = s.getFunction("hello world");
		assertNotNull(fm);
		assertEquals("integer", fm.getOutputParameter().getType());
		assertEquals(FunctionMethod.PushDown.MUST_PUSHDOWN, fm.getPushdown());
	}	
	
	@Test(expected=DuplicateRecordException.class)
	public void testDuplicateFunctions() throws Exception {
		String ddl = "CREATE FUNCTION SourceFunc() RETURNS integer; CREATE FUNCTION SourceFunc() RETURNS string";
		helpParse(ddl, "model");
	}
	
	@Test(expected=DuplicateRecordException.class)
	public void testDuplicateFunctions1() throws Exception {
		String ddl = "CREATE FUNCTION SourceFunc() RETURNS string OPTIONS (UUID 'a'); CREATE FUNCTION SourceFunc1() RETURNS string OPTIONS (UUID 'a')";
		helpParse(ddl, "model");
	}

	@Test()
	public void testDuplicateFunctions2() throws Exception {
		String ddl = "CREATE FUNCTION SourceFunc() RETURNS string; CREATE FUNCTION SourceFunc(param string) RETURNS string";
		helpParse(ddl, "model");
	}

	@Test
	public void testUDF() throws Exception {
		String ddl = "CREATE VIRTUAL FUNCTION SourceFunc(flag boolean, msg varchar) RETURNS varchar " +
				"OPTIONS(CATEGORY 'misc', DETERMINISM 'DETERMINISTIC', " +
				"\"NULL-ON-NULL\" 'true', JAVA_CLASS 'foo', JAVA_METHOD 'bar', RANDOM 'any', UUID 'x')";

		Schema s = helpParse(ddl, "model").getSchema();
		
		FunctionMethod fm = s.getFunction("x");
		assertNotNull(fm);
		assertEquals("string", fm.getOutputParameter().getType());
		assertEquals(FunctionMethod.PushDown.CAN_PUSHDOWN, fm.getPushdown());
		assertEquals(2, fm.getInputParameterCount());
		assertEquals("flag", fm.getInputParameters().get(0).getName());
		assertEquals("boolean", fm.getInputParameters().get(0).getType());
		assertEquals("msg", fm.getInputParameters().get(1).getName());
		assertEquals("string", fm.getInputParameters().get(1).getType());
		assertFalse( fm.getInputParameters().get(1).isVarArg());
		
		assertEquals(FunctionMethod.Determinism.DETERMINISTIC, fm.getDeterminism());
		assertEquals("misc", fm.getCategory());
		assertEquals(true, fm.isNullOnNull());
		assertEquals("foo", fm.getInvocationClass());
		assertEquals("bar", fm.getInvocationMethod());
		assertEquals("any", fm.getProperties().get("RANDOM"));
	}
	
	@Test
	public void testUDAggregate() throws Exception {
		String ddl = "CREATE VIRTUAL FUNCTION SourceFunc(flag boolean, msg varchar) RETURNS varchar " +
				"OPTIONS(CATEGORY 'misc', AGGREGATE 'true', \"allows-distinct\" 'true', UUID 'y')";

		Schema s = helpParse(ddl, "model").getSchema();
		
		FunctionMethod fm = s.getFunction("y");
		assertNotNull(fm);
		assertEquals("string", fm.getOutputParameter().getType());
		assertEquals(FunctionMethod.PushDown.CAN_PUSHDOWN, fm.getPushdown());
		assertEquals(2, fm.getInputParameterCount());
		assertEquals("flag", fm.getInputParameters().get(0).getName());
		assertEquals("boolean", fm.getInputParameters().get(0).getType());
		assertEquals("msg", fm.getInputParameters().get(1).getName());
		assertEquals("string", fm.getInputParameters().get(1).getType());
		assertFalse( fm.getInputParameters().get(1).isVarArg());
		assertNotNull(fm.getAggregateAttributes());
		assertTrue(fm.getAggregateAttributes().allowsDistinct());
		assertEquals(FunctionMethod.Determinism.DETERMINISTIC, fm.getDeterminism());
		assertEquals("misc", fm.getCategory());
		assertFalse(fm.isNullOnNull());
	}
	
	@Test
	public void testVarArgs() throws Exception {
		String ddl = "CREATE FUNCTION SourceFunc(flag boolean) RETURNS varchar options (varargs 'true', UUID 'z')";

		Schema s = helpParse(ddl, "model").getSchema();
		
		FunctionMethod fm = s.getFunction("z");	
		assertTrue( fm.getInputParameters().get(0).isVarArg());
	}
	
	@Test public void testMixedCaseTypes() throws Exception {
		String ddl = "CREATE FUNCTION SourceFunc(flag Boolean) RETURNS varchaR options (UUID 'z')";

		Schema s = helpParse(ddl, "model").getSchema();
		
		FunctionMethod fm = s.getFunction("z");	
		assertEquals("boolean", fm.getInputParameters().get(0).getType());
	}
	
	@Test(expected=MetadataException.class) public void testInvalidFunctionBody() throws Exception {
		String ddl = "CREATE FUNCTION SourceFunc(flag boolean) RETURNS varchar AS SELECT 'a';";

		Schema s = helpParse(ddl, "model").getSchema();
		
		FunctionMethod fm = s.getFunction("z");	
		assertTrue( fm.getInputParameters().get(0).isVarArg());
	}
	
	@Test(expected=MetadataException.class) public void testInvalidProcedureBody() throws Exception {
		String ddl = "CREATE FOREIGN PROCEDURE SourceFunc(flag boolean) RETURNS varchar AS SELECT 'a';";

		Schema s = helpParse(ddl, "model").getSchema();
		
		FunctionMethod fm = s.getFunction("z");	
		assertTrue( fm.getInputParameters().get(0).isVarArg());
	}
	
	@Test
	public void testVirtualProcedure() throws Exception {
		String ddl = "CREATE VIRTUAL PROCEDURE myProc(OUT p1 boolean, p2 varchar, INOUT p3 decimal) " +
				"RETURNS (r1 varchar, r2 decimal) " +
				"OPTIONS(RANDOM 'any', UUID 'uuid', NAMEINSOURCE 'nis', ANNOTATION 'desc', UPDATECOUNT '2') " +
				"AS /*+ cache */ BEGIN select * from foo; END";

		Schema s = helpParse(ddl, "model").getSchema();
		
		Procedure proc = s.getProcedure("myProc");
		assertNotNull(proc);
		
		assertTrue(proc.isVirtual());
		assertFalse(proc.isFunction());
		
		assertEquals(3, proc.getParameters().size());
		assertEquals("p1", proc.getParameters().get(0).getName());
		assertEquals("boolean", proc.getParameters().get(0).getDatatype().getName());
		assertEquals(ProcedureParameter.Type.Out, proc.getParameters().get(0).getType());
		
		assertEquals("p2", proc.getParameters().get(1).getName());
		assertEquals("string", proc.getParameters().get(1).getDatatype().getName());
		assertEquals(ProcedureParameter.Type.In, proc.getParameters().get(1).getType());
		
		assertEquals("p3", proc.getParameters().get(2).getName());
		assertEquals("bigdecimal", proc.getParameters().get(2).getDatatype().getName());
		assertEquals(ProcedureParameter.Type.InOut, proc.getParameters().get(2).getType());		
		
		ColumnSet<Procedure> ret = proc.getResultSet();
		assertNotNull(ret);
		assertEquals(2, ret.getColumns().size());
		assertEquals("r1", ret.getColumns().get(0).getName());
		assertEquals("string", ret.getColumns().get(0).getDatatype().getName());	
		assertEquals("r2", ret.getColumns().get(1).getName());
		assertEquals("bigdecimal", ret.getColumns().get(1).getDatatype().getName());		
		
		assertEquals("uuid", proc.getUUID());
		assertEquals("nis", proc.getNameInSource());
		assertEquals("desc", proc.getAnnotation());
		assertEquals(2, proc.getUpdateCount());
		assertEquals("any", proc.getProperties().get("RANDOM"));	
		
		assertEquals("/*+ cache */ BEGIN\nSELECT * FROM foo;\nEND", proc.getQueryPlan());	
		
	}	
	
	@Test
	public void testInsteadOfTrigger() throws Exception {
		String ddl = 	"CREATE VIEW G1( e1 integer, e2 varchar) AS select * from foo;" +
						"CREATE TRIGGER ON G1 INSTEAD OF INSERT AS " +
						"FOR EACH ROW \n" +
						"BEGIN ATOMIC \n" +
						"insert into g1 (e1, e2) values (1, 'trig');\n" +
						"END;" +
						"CREATE View G2( e1 integer, e2 varchar) AS select * from foo;";

		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		assertTrue("Table not found", tableMap.containsKey("G2"));
		assertEquals("FOR EACH ROW\nBEGIN ATOMIC\nINSERT INTO g1 (e1, e2) VALUES (1, 'trig');\nEND", s.getTable("G1").getInsertPlan());
	}	
	
	@Test
	public void testSourceProcedure() throws Exception {
		String ddl = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean, p2 varchar, INOUT p3 decimal) " +
				"RETURNS (r1 varchar, r2 decimal)" +
				"OPTIONS(RANDOM 'any', UUID 'uuid', NAMEINSOURCE 'nis', ANNOTATION 'desc', UPDATECOUNT '2');";
		
		Schema s = helpParse(ddl, "model").getSchema();
		
		Procedure proc = s.getProcedure("myProc");
		assertNotNull(proc);
		
		assertFalse(proc.isVirtual());
		assertFalse(proc.isFunction());
		
		assertEquals(3, proc.getParameters().size());
		assertEquals("p1", proc.getParameters().get(0).getName());
		assertEquals("boolean", proc.getParameters().get(0).getDatatype().getName());
		assertEquals(ProcedureParameter.Type.Out, proc.getParameters().get(0).getType());
		
		assertEquals("p2", proc.getParameters().get(1).getName());
		assertEquals("string", proc.getParameters().get(1).getDatatype().getName());
		assertEquals(ProcedureParameter.Type.In, proc.getParameters().get(1).getType());
		
		assertEquals("p3", proc.getParameters().get(2).getName());
		assertEquals("bigdecimal", proc.getParameters().get(2).getDatatype().getName());
		assertEquals(ProcedureParameter.Type.InOut, proc.getParameters().get(2).getType());		
		
		ColumnSet<Procedure> ret = proc.getResultSet();
		assertNotNull(ret);
		assertEquals(2, ret.getColumns().size());
		assertEquals("r1", ret.getColumns().get(0).getName());
		assertEquals("string", ret.getColumns().get(0).getDatatype().getName());	
		assertEquals("r2", ret.getColumns().get(1).getName());
		assertEquals("bigdecimal", ret.getColumns().get(1).getDatatype().getName());		
		
		assertEquals("uuid", proc.getUUID());
		assertEquals("nis", proc.getNameInSource());
		assertEquals("desc", proc.getAnnotation());
		assertEquals(2, proc.getUpdateCount());
		assertEquals("any", proc.getProperties().get("RANDOM"));		
	}
	
	@Test
	public void testNamespace() throws Exception {
		String ddl = 	"set namespace 'http://teiid.org' AS teiid";

		MetadataStore mds = new MetadataStore();
		MetadataFactory mf = new MetadataFactory(null, 1, "model", getDataTypes(), new Properties(), null); 
		parser.parseDDL(mf, ddl);
		mf.mergeInto(mds);
		
		assertTrue(mf.getNamespaces().keySet().contains("teiid"));
		assertEquals("http://teiid.org", mf.getNamespaces().get("teiid"));
	}		

	public static MetadataFactory helpParse(String ddl, String model) {
		MetadataFactory mf = new MetadataFactory(null, 1, model, getDataTypes(), new Properties(), null); 
		QueryParser.getQueryParser().parseDDL(mf, ddl);
		return mf;
	}
	
	public static Map<String, Datatype> getDataTypes() {
		return SystemMetadata.getInstance().getRuntimeTypeMap();
	}
	
	@Test public void testKeyResolve() {
		MetadataFactory mf = new MetadataFactory(null, 1, "foo", getDataTypes(), new Properties(), null);
		mf.addNamespace("x", "http://x");
		assertEquals("{http://x}z", SQLParserUtil.resolvePropertyKey(mf, "x:z"));
		assertEquals("y:z", SQLParserUtil.resolvePropertyKey(mf, "y:z"));
	}
	
	@Test public void testCreateError() {
		try {
			helpParse("CREATE foreign FUNCTION convert(msg integer, type varchar) RETURNS varchar", "x");
			fail();
		} catch (org.teiid.metadata.ParseException e) {
			assertEquals("TEIID30386 org.teiid.api.exception.query.QueryParserException: TEIID31100 Parsing error: Encountered \"CREATE foreign FUNCTION [*]convert[*](msg\" at line 1, column 25.\nWas expecting: id", e.getMessage());
		}
	}
	
	
	@Test
	public void testAlterTableAddOptions() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);" +
				"ALTER FOREIGN TABLE G1 OPTIONS(ADD CARDINALITY 12);" +
				"ALTER FOREIGN TABLE G1 OPTIONS(ADD FOO 'BAR');";

		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		Table table = tableMap.get("G1");
		assertEquals(12, table.getCardinality());
		assertEquals("BAR", table.getProperty("FOO", false));
	}	
	
	@Test
	public void testAlterTableModifyOptions() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date) OPTIONS(CARDINALITY 12, FOO 'BAR');" +
				"ALTER FOREIGN TABLE G1 OPTIONS(SET CARDINALITY 24);" +
				"ALTER FOREIGN TABLE G1 OPTIONS(SET FOO 'BARBAR');";

		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		Table table = tableMap.get("G1");
		assertEquals(24, table.getCardinality());
		assertEquals("BARBAR", table.getProperty("FOO", false));
	}	
	
	@Test
	public void testAlterTableDropOptions() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date) OPTIONS(CARDINALITY 12, FOO 'BAR');" +
				"ALTER FOREIGN TABLE G1 OPTIONS(DROP CARDINALITY);" +
				"ALTER FOREIGN TABLE G1 OPTIONS(DROP FOO);";

		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		Table table = tableMap.get("G1");
		assertEquals(-1, table.getCardinality());
		assertNull(table.getProperty("FOO", false));
	}	
	
	@Test
	public void testAlterTableAddColumnOptions() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);" +
				"ALTER FOREIGN TABLE G1 OPTIONS(ADD CARDINALITY 12);" +
				"ALTER FOREIGN TABLE G1 ALTER COLUMN e1 OPTIONS(ADD NULL_VALUE_COUNT 12);" +
				"ALTER FOREIGN TABLE G1 ALTER COLUMN e1 OPTIONS(ADD FOO 'BAR');";

		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		Table table = tableMap.get("G1");
		assertEquals(12, table.getCardinality());
		Column c = table.getColumnByName("e1");
		assertNotNull(c);
		
		assertEquals("BAR", c.getProperty("FOO", false));
		assertEquals(12, c.getNullValues());
	}	
	
	@Test
	public void testAlterTableRemoveColumnOptions() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer OPTIONS (NULL_VALUE_COUNT 12, FOO 'BAR'), e2 varchar, e3 date);" +
				"ALTER FOREIGN TABLE G1 ALTER COLUMN e1 OPTIONS(DROP NULL_VALUE_COUNT);" +
				"ALTER FOREIGN TABLE G1 ALTER COLUMN e1 OPTIONS(DROP FOO);" +
				"ALTER FOREIGN TABLE G1 ALTER COLUMN e1 OPTIONS( ADD x 'y');" ;

		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		Table table = tableMap.get("G1");
		Column c = table.getColumnByName("e1");
		assertNotNull(c);
		
		assertNull(c.getProperty("FOO", false));
		assertEquals(-1, c.getNullValues());
		assertEquals("y", c.getProperty("x", false));
	}	
	
	@Test
	public void testAlterProcedureOptions() throws Exception {
		String ddl = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean, p2 varchar, INOUT p3 decimal) " +
				"RETURNS (r1 varchar, r2 decimal)" +
				"OPTIONS(RANDOM 'any', UUID 'uuid', NAMEINSOURCE 'nis', ANNOTATION 'desc', UPDATECOUNT '2');" +
				"ALTER FOREIGN PROCEDURE myProc OPTIONS(SET NAMEINSOURCE 'x')" +
				"ALTER FOREIGN PROCEDURE myProc ALTER PARAMETER p2 OPTIONS (ADD x 'y');" +
				"ALTER FOREIGN PROCEDURE myProc OPTIONS(DROP UPDATECOUNT);";
		
		Schema s = helpParse(ddl, "model").getSchema();
		Procedure proc = s.getProcedure("myProc");
		assertNotNull(proc);
	
		assertEquals("x", proc.getNameInSource());
		assertEquals("p2", proc.getParameters().get(1).getName());
		assertEquals("y", proc.getParameters().get(1).getProperty("x", false));
		assertEquals(1, proc.getUpdateCount());
	}		
}
