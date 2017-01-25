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

import java.io.StringReader;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.*;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.metadata.Grant.Permission;
import org.teiid.metadata.Grant.Permission.Privilege;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.metadata.DatabaseStore;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.validator.ValidatorReport;

//import static org.junit.Assert.*;

@SuppressWarnings("nls")
public class TestDDLParser {
	@Test
	public void testForeignTable() throws Exception {
		
		String ddl = "CREATE FOREIGN TABLE G1(\n" +
						"e1 integer primary key,\n" +
						"e2 varchar(10) unique,\n" +
						"e3 date not null unique,\n" +
						"e4 decimal(12,3) default 12.2 options (searchable 'unsearchable'),\n" +
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
		assertEquals("12.2", e4.getDefaultValue());
		
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
    public void testZeroPrecision() throws Exception {
        String ddl = "CREATE FOREIGN TABLE G1(e4 decimal(0))";
                
        helpParse(ddl, "model").getSchema();
    }
	
    @Test
    public void testDefaultPrecision() throws Exception {
        String ddl = "CREATE FOREIGN TABLE G1(e4 decimal)";
                
        Schema s = helpParse(ddl, "model").getSchema();
        Map<String, Table> tableMap = s.getTables();
        
        assertTrue("Table not found", tableMap.containsKey("G1"));
        Table table = tableMap.get("G1");
        Column e4 = table.getColumns().get(0);
        
        assertEquals("e4", e4.getName());
        assertEquals("bigdecimal", e4.getDatatype().getName());
        assertEquals(false, e4.isAutoIncremented());
        assertEquals(Short.MAX_VALUE, e4.getPrecision());
        assertEquals(Short.MAX_VALUE/2, e4.getScale());
        assertEquals(SearchType.Searchable, e4.getSearchType());
    }
	
	@Test(expected=MetadataException.class)
	public void testDuplicatePrimarykey() throws Exception {
		QueryParser parser = new QueryParser();
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer primary key, e2 varchar primary key)";
		MetadataStore mds = new MetadataStore();
		MetadataFactory mf = new MetadataFactory("x", 1, "model", getDataTypes(), new Properties(), null); 
		parser.parseDDL(mf, ddl);
		mf.mergeInto(mds);
	}
	
	@Test public void testAutoIncrementPrimarykey() throws Exception {
		QueryParser parser = new QueryParser();
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer auto_increment primary key, e2 varchar)";
		MetadataStore mds = new MetadataStore();
		MetadataFactory mf = new MetadataFactory("x", 1, "model", getDataTypes(), new Properties(), null); 
		parser.parseDDL(mf, ddl);
		mf.mergeInto(mds);
	}
	
	@Test
	public void testUDT() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar OPTIONS (UDT 'NMTOKENS(12,13,11)'))";

		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		Table table = tableMap.get("G1");
		
		assertEquals("NMTOKENS", table.getColumns().get(1).getDatatype().getName());
		assertEquals(12, table.getColumns().get(1).getLength());
		assertEquals(13, table.getColumns().get(1).getPrecision());
		assertEquals(11, table.getColumns().get(1).getScale());		
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
		QueryParser parser = new QueryParser();
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, PRIMARY KEY (e3))";

		MetadataStore mds = new MetadataStore();
		MetadataFactory mf = new MetadataFactory("x", 1, "model", getDataTypes(), new Properties(), null); 
		parser.parseDDL(mf, ddl);
		mf.mergeInto(mds);
	}	

	@Test
	public void testFK() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(g1e1 integer, g1e2 varchar, PRIMARY KEY(g1e1, g1e2));\n" +
				"CREATE FOREIGN TABLE G2( g2e1 integer, g2e2 varchar, " +
				"FOREIGN KEY (g2e1, g2e2) REFERENCES G1 (g1e1, g1e2) options (\"teiid_rel:allow-join\" true))";
		
		Schema s = helpParse(ddl, "model").getSchema();
		Map<String, Table> tableMap = s.getTables();	
		assertEquals(2, tableMap.size());
		
		assertTrue("Table not found", tableMap.containsKey("G1"));
		assertTrue("Table not found", tableMap.containsKey("G2"));
		
		Table table = tableMap.get("G2");
		ForeignKey fk = table.getForeignKeys().get(0);
		assertEquals(Boolean.TRUE.toString(), fk.getProperty(ForeignKey.ALLOW_JOIN, false));
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
		assertEquals("G1", fk.getReferenceTableName());
		
		assertEquals(fk.getPrimaryKey().getColumns(), s.getSchema("model").getTable("G1").getColumns());
	}	
	
	@Test(expected=MetadataException.class)
	public void testTableWithPlan() throws Exception {
		QueryParser parser = new QueryParser();
		String ddl = "CREATE foreign table G1 as select 1";
		MetadataStore mds = new MetadataStore();
		MetadataFactory mf = new MetadataFactory("x", 1, "model", getDataTypes(), new Properties(), null); 
		parser.parseDDL(mf,ddl);
		mf.mergeInto(mds);
	}	
	
	@Test
	public void testViewWithoutColumns() throws Exception {
		QueryParser parser = new QueryParser();
		MetadataStore mds = new MetadataStore();			
		MetadataFactory mf = new MetadataFactory("x", 1, "VM1", getDataTypes(), new Properties(), null); 
		parser.parseDDL(mf,"CREATE VIEW V1 AS SELECT * FROM PM1.G1");			
		mf.mergeInto(mds);
	}	
	
	@Test
	public void testMultipleCommands() throws Exception {
		String ddl = "CREATE VIEW V1 AS SELECT * FROM PM1.G1; " +
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
		String ddl = "CREATE FOREIGN FUNCTION SourceFunc(flag boolean) RETURNS varchar AS SELECT 'a';";

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
	public void testInsteadOfTriggerIsDistinct() throws Exception {
		String ddl = 	"CREATE VIEW G1( e1 integer, e2 varchar) AS select * from foo;" +
						"CREATE TRIGGER ON G1 INSTEAD OF UPDATE AS " +
						"FOR EACH ROW \n" +
						"BEGIN ATOMIC \n" +
						"if (\"new\" is not distinct from \"old\") raise sqlexception 'error';\n" +
						"END;";

		Schema s = helpParse(ddl, "model").getSchema();
		
		assertEquals("FOR EACH ROW\nBEGIN ATOMIC\nIF(\"new\" IS NOT DISTINCT FROM \"old\")\nBEGIN\nRAISE SQLEXCEPTION 'error';\nEND\nEND", s.getTable("G1").getUpdatePlan());
	}
	
	@Test(expected=MetadataException.class)
	public void testInsteadOfTriggerNoView() throws Exception {
		String ddl = 	"CREATE TRIGGER ON G1 INSTEAD OF INSERT AS " +
						"FOR EACH ROW \n" +
						"BEGIN ATOMIC \n" +
						"insert into g1 (e1, e2) values (1, 'trig');\n" +
						"END;" +
						"CREATE View G2( e1 integer, e2 varchar) AS select * from foo;";

		helpParse(ddl, "model");
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
		QueryParser parser = new QueryParser();
		String ddl = 	"set namespace 'http://teiid.org' AS teiid";

		MetadataStore mds = new MetadataStore();
		MetadataFactory mf = new MetadataFactory("x", 1, "model", getDataTypes(), new Properties(), null); 
		parser.parseDDL(mf, ddl);
		mf.mergeInto(mds);
		
		assertTrue(mf.getNamespaces().keySet().contains("teiid"));
		assertEquals("http://teiid.org", mf.getNamespaces().get("teiid"));
	}
	
    @Test
    public void testReservedNamespace1() throws Exception {
    	QueryParser parser = new QueryParser();
        String ddl = "set namespace 'http://www.teiid.org/translator/salesforce/2012' AS teiid_sf";
        MetadataStore mds = new MetadataStore();
        MetadataFactory mf = new MetadataFactory("x", 1, "model", getDataTypes(), new Properties(), null); 
        parser.parseDDL(mf, ddl);
        mf.mergeInto(mds);
        
        assertTrue(mf.getNamespaces().keySet().contains("teiid_sf"));
        assertEquals("http://www.teiid.org/translator/salesforce/2012", mf.getNamespaces().get("teiid_sf"));
    }   
    
    @Test(expected=MetadataException.class)
    public void testReservedNamespaceURIWrong() throws Exception {
    	QueryParser parser = new QueryParser();
        String ddl = "set namespace 'http://www.teiid.org/translator/salesforce/2013' AS teiid_sf";
        MetadataStore mds = new MetadataStore();
        MetadataFactory mf = new MetadataFactory("x", 1, "model", getDataTypes(), new Properties(), null); 
        parser.parseDDL(mf, ddl);
        mf.mergeInto(mds);
        
        assertTrue(mf.getNamespaces().keySet().contains("teiid_sf"));
        assertEquals("http://www.teiid.org/translator/salesforce/2012", mf.getNamespaces().get("teiid_sf"));
    } 
    
    @Test(expected=MetadataException.class)
    public void testReservedNamespacePrefixMismatch() throws Exception {
    	QueryParser parser = new QueryParser();
        String ddl = "set namespace 'http://www.teiid.org/translator/salesforce/2012' AS teiid_foo";
        MetadataStore mds = new MetadataStore();
        MetadataFactory mf = new MetadataFactory("x", 1, "model", getDataTypes(), new Properties(), null); 
        parser.parseDDL(mf, ddl);
        mf.mergeInto(mds);
        
        assertTrue(mf.getNamespaces().keySet().contains("teiid_foo"));
        assertEquals("http://www.teiid.org/translator/salesforce/2012", mf.getNamespaces().get("teiid_foo"));
    }     
	
    @Test
    public void testReservedURIDifferentNS() throws Exception {
    	QueryParser parser = new QueryParser();
        String ddl = "set namespace 'http://www.teiid.org/translator/salesforce/2012' AS ns";
        MetadataStore mds = new MetadataStore();
        MetadataFactory mf = new MetadataFactory("x", 1, "model", getDataTypes(), new Properties(), null); 
        parser.parseDDL(mf, ddl);
        mf.mergeInto(mds);
        
        assertTrue(mf.getNamespaces().keySet().contains("ns"));
        assertEquals("http://www.teiid.org/translator/salesforce/2012", mf.getNamespaces().get("ns"));
    }    

	public static MetadataFactory helpParse(String ddl, String model) {
		MetadataFactory mf = new MetadataFactory("x", 1, model, getDataTypes(), new Properties(), null); 
		QueryParser.getQueryParser().parseDDL(mf, ddl);
		return mf;
	}
	
    public static Database helpParse(String ddl) {
        DatabaseStore store = new DatabaseStore() {
            @Override
            public Map<String, Datatype> getRuntimeTypes() {
                return getDataTypes();
            }
            @Override
            public Map<String, Datatype> getBuiltinDataTypes() {
                return getDataTypes();
            }
			@Override
			public SystemFunctionManager getSystemFunctionManager() {
				return new SystemFunctionManager();
			}
        }; 
        store.startEditing(true);
        QueryParser.getQueryParser().parseDDL(store, new StringReader(ddl));
        store.stopEditing();
        if (store.getDatabases().isEmpty()) {
            return null;
        }
        return store.getDatabases().get(0);
    }	
	
	public static Map<String, Datatype> getDataTypes() {
		return SystemMetadata.getInstance().getRuntimeTypeMap();
	}
	
	@Test public void testKeyResolve() {
		MetadataFactory mf = new MetadataFactory("x", 1, "foo", getDataTypes(), new Properties(), null);
		mf.addNamespace("x", "http://x");
		assertEquals("{http://x}z", MetadataFactory.resolvePropertyKey(mf, "x:z"));
		assertEquals("y:z", MetadataFactory.resolvePropertyKey(mf, "y:z"));
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
				"ALTER FOREIGN PROCEDURE myProc OPTIONS(SET NAMEINSOURCE 'x');" +
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
	
	@Test
	public void testTypeLength() throws Exception {
		String ddl = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean, p2 varchar, INOUT p3 decimal) " +
				"RETURNS (r1 varchar(10), r2 decimal(11,2), r3 object(1), r4 clob(10000))" +
				"OPTIONS(RANDOM 'any', UUID 'uuid', NAMEINSOURCE 'nis', ANNOTATION 'desc', UPDATECOUNT '2');";
		
		Schema s = helpParse(ddl, "model").getSchema();
		Procedure proc = s.getProcedure("myProc");
		assertNotNull(proc);
	
		List<Column> cols = proc.getResultSet().getColumns();
		assertEquals(10, cols.get(0).getLength());
		assertEquals(11, cols.get(1).getPrecision());
		assertEquals(1, cols.get(2).getLength());
		assertEquals(10000, cols.get(3).getLength());
	}
	
	@Test
	public void testGlobalTemp() throws Exception {
		String ddl = "CREATE GLOBAL TEMPORARY TABLE T (col string);";
		
		Schema s = helpParse(ddl, "model").getSchema();
		Table t = s.getTable("T");
		assertEquals(1, t.getColumns().size());
	}
	
	@Test public void testArrayType() throws Exception {
		String ddl = "CREATE VIEW V (col string[]) as select ('a','b');";
		
		Schema s = helpParse(ddl, "model").getSchema();
		Table t = s.getTable("V");
		assertEquals(1, t.getColumns().size());
		assertEquals("string[]", t.getColumns().get(0).getRuntimeType());
		assertEquals(String[].class, t.getColumns().get(0).getJavaType());
	}

   @Test(expected=MetadataException.class) public void testAfterTriggerNameRequired() throws Exception {
        String ddl = "CREATE FOREIGN TABLE T ( e1 integer, e2 varchar);" +
                "CREATE TRIGGER ON T AFTER UPDATE AS " +
                "FOR EACH ROW \n" +
                "BEGIN ATOMIC \n" +
                "if (\"new\" is not distinct from \"old\") raise sqlexception 'error';\n" +
                "END;";

        
        helpParse(ddl, "model").getSchema();
    }
	        
	@Test public void testAfterTrigger() throws Exception {
        String ddl = "CREATE FOREIGN TABLE T ( e1 integer, e2 varchar);" +
                "CREATE TRIGGER tr ON T AFTER UPDATE AS " +
                "FOR EACH ROW \n" +
                "BEGIN ATOMIC \n" +
                "if (\"new\" is not distinct from \"old\") raise sqlexception 'error';\n" +
                "END;";

        
        Schema s = helpParse(ddl, "model").getSchema();
        Table t = s.getTable("T");
        assertEquals(1, t.getTriggers().size());
        Trigger tr = t.getTriggers().values().iterator().next();
        assertEquals("tr", tr.getName());
        assertEquals(TriggerEvent.UPDATE, tr.getEvent());
        assertNotNull(tr.getPlan());
	}
	
    @Test 
    public void testCreateDatabase() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2' OPTIONS (k1 'v1', k2 'v2')";
        
        Database db = helpParse(ddl);
        
        assertEquals("FOO", db.getName());
        assertEquals("2", db.getVersion());
        assertEquals("v1", db.getProperty("k1", false));
        assertEquals("v2", db.getProperty("k2", false));
    }	
    
    @Test 
    public void testAlterDatabase() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2' OPTIONS (k1 'v1', k2 'v2');"
                + "USE DATABASE FOO version '2';"
                 + "ALTER DATABASE FOO OPTIONS (ADD k3 'v3', SET k1 'v4', DROP k2);";
        
        Database db = helpParse(ddl);
        
        assertEquals("FOO", db.getName());
        assertEquals("2", db.getVersion());
        assertEquals("v4", db.getProperty("k1", false));
        assertNull(db.getProperty("k2", false));
        assertEquals("v3", db.getProperty("k3", false));
    }
    
    @Test 
    public void testAlterServer() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2';"
                + "USE DATABASE FOO version '2';"
                + "CREATE FOREIGN DATA WRAPPER orcl;"
                + "CREATE SERVER x TYPE 'oracle' VERSION '2.0' FOREIGN DATA WRAPPER orcl OPTIONS (k1 'v1');"
                + "ALTER SERVER x OPTIONS(SET k1 'v2')";
        		
        Database db = helpParse(ddl);
        
        assertEquals("FOO", db.getName());
        assertEquals("2", db.getVersion());
        Server s = db.getServer("x");
        assertNotNull(s);
        assertEquals("v2", s.getProperty("k1", false));
    }    
    
    @Test 
    public void testCreateDatabaseNoVersion() throws Exception {
        String ddl = "CREATE DATABASE FOO OPTIONS (k1 'v1', k2 'v2')";
        
        Database db = helpParse(ddl);
        
        assertEquals("FOO", db.getName());
        assertEquals("1", db.getVersion());
        assertEquals("v1", db.getProperty("k1", false));
        assertEquals("v2", db.getProperty("k2", false));
    }
    
    @Test 
    public void testCreateDropDatabase() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "DROP DATABASE FOO;";
        
        Database db = helpParse(ddl);
        assertNull(db);
    }    
    
    @Test 
    public void testCreateSchema() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER orcl;"
                + "CREATE SERVER x TYPE 'oracle' VERSION '2.0' FOREIGN DATA WRAPPER orcl OPTIONS (k1 'v1');"
                + "CREATE SCHEMA S1 SERVER x OPTIONS (x 'y');";
        
        Database db = helpParse(ddl);
        
        // schema test
        Schema schema = db.getSchema("S1");
        assertNotNull(schema);
        assertEquals("y", schema.getProperty("x", false));
        
        //server test
        assertNotNull(db.getServer("x"));
        assertNotNull(schema.getServer("x"));
        assertEquals("v1", db.getServer("x").getProperty("k1", false));
        
        // data wrapper test
        assertNotNull(db.getDataWrapper("orcl"));
        assertEquals("orcl", db.getServer("x").getDataWrapper());
    }
    
    @Test 
    public void testRole() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE ROLE superuser WITH JAAS ROLE x,y;";
        
        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);
        
        assertEquals("[x, y]", role.getJassRoles().toString());
    } 
    
    @Test 
    public void testRoleAnyAuth() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE ROLE superuser WITH JAAS ROLE x,y WITH ANY AUTHENTICATED;";
        
        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);
        
        assertEquals("[x, y]", role.getJassRoles().toString());
        assertTrue(role.isAnyAuthenticated());
    }  

    @Test 
    public void testDropRole() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE ROLE superuser WITH JAAS ROLE x,y WITH ANY AUTHENTICATED;"
                + "DROP ROLE superuser";
        
        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNull(role);
        
    }      
    
    @Test 
    public void testGrant() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"  
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "CREATE ROLE superuser WITH JAAS ROLE x,y WITH ANY AUTHENTICATED;"
                + "GRANT SELECT,INSERT,DELETE ON TABLE G1 TO superuser;"
                + "GRANT UPDATE ON TABLE test.G1 TO superuser;";
        
        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);

        Collection<Grant> grants = db.getGrants();
        assertEquals(1, grants.size());
        Grant g = grants.iterator().next();
        assertEquals(1, g.getPermissions().size());
        Permission p = g.getPermissions().iterator().next();
        assertTrue(p.hasPrivilege(Privilege.SELECT));
        assertTrue(p.hasPrivilege(Privilege.INSERT));
        assertTrue(p.hasPrivilege(Privilege.DELETE));
        assertTrue(p.hasPrivilege(Privilege.UPDATE));
        assertFalse(p.hasPrivilege(Privilege.DROP));
    }
    
    @Test 
    public void testGrantAll() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"  
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "CREATE ROLE superuser WITH JAAS ROLE x,y WITH ANY AUTHENTICATED;"
                + "GRANT ALL PRIVILEGES ON TABLE test.G1 TO superuser;";
        
        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);

        Collection<Grant> grants = db.getGrants();
        assertEquals(1, grants.size());
        Grant g = grants.iterator().next();
        assertEquals(1, g.getPermissions().size());
        Permission p = g.getPermissions().iterator().next();
        assertTrue(p.hasPrivilege(Privilege.ALL_PRIVILEGES));
    }  
    
    @Test 
    public void testGrantWithCondition() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"  
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "CREATE ROLE superuser WITH JAAS ROLE x,y WITH ANY AUTHENTICATED;"
                + "GRANT ALL PRIVILEGES ON TABLE test.G1 CONDITION CONSTRAINT 'foo=bar' TO superuser;";
        
        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);

        Collection<Grant> grants = db.getGrants();
        assertEquals(1, grants.size());
        Grant g = grants.iterator().next();
        assertEquals(1, g.getPermissions().size());
        Permission p = g.getPermissions().iterator().next();
        assertTrue(p.hasPrivilege(Privilege.ALL_PRIVILEGES));
        assertEquals("foo=bar", p.getCondition());
        assertTrue(p.isConditionAConstraint());
    }     
    
    @Test 
    public void testRevokeGrant() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"  
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "CREATE ROLE superuser WITH JAAS ROLE x,y WITH ANY AUTHENTICATED;"
                + "GRANT SELECT,INSERT,DELETE ON TABLE G1 TO superuser;"
                + "GRANT UPDATE ON TABLE test.G1 TO superuser;"
                + "REVOKE GRANT SELECT ON TABLE test.G1 FROM superuser;";
        
        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);

        Collection<Grant> grants = db.getGrants();
        assertEquals(1, grants.size());
        Grant g = grants.iterator().next();
        assertEquals(1, g.getPermissions().size());
        Permission p = g.getPermissions().iterator().next();
        assertFalse(p.hasPrivilege(Privilege.SELECT));
        assertTrue(p.hasPrivilege(Privilege.INSERT));
        assertTrue(p.hasPrivilege(Privilege.DELETE));
        assertTrue(p.hasPrivilege(Privilege.UPDATE));
        assertFalse(p.hasPrivilege(Privilege.DROP));
    }  
    
    @Test 
    public void testRevokeALl() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"  
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "CREATE ROLE superuser WITH JAAS ROLE x,y WITH ANY AUTHENTICATED;"
                + "GRANT SELECT,INSERT,DELETE ON TABLE G1 TO superuser;"
                + "GRANT UPDATE ON TABLE test.G1 TO superuser;"
                + "REVOKE GRANT ALL PRIVILEGES ON TABLE test.G1 FROM superuser;";
        
        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);

        Collection<Grant> grants = db.getGrants();
        assertEquals(0, grants.size());
    }    

    @Test 
    public void testPhysicalTable() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"  
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);";
        
        Database db = helpParse(ddl);
        Schema s = db.getSchema("test");
        Table t = s.getTable("G1");
        assertNotNull(t);  
        assertTrue(t.getColumns().size() == 3);
        assertNotNull(t.getColumnByName("e1"));
        assertNotNull(t.getColumnByName("e2"));
        assertNotNull(t.getColumnByName("e3"));
    }    
    
    @Test 
    public void testAlterView() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE VIRTUAL SCHEMA test;"
                + "SET SCHEMA test;"
                + "CREATE VIRTUAL VIEW G1( e1 integer, e2 varchar, e3 date) AS SELECT 1, '2', curdate();"
                + "ALTER VIEW G1 AS /*+ foo */ SELECT 1, 'foo', curdate()";
        
        Database db = helpParse(ddl);
        Schema s = db.getSchema("test");
        Table t = s.getTable("G1");
        assertEquals("SELECT 1, 'foo', curdate()", t.getSelectTransformation());
    } 
    
    @Test 
    public void testPhysicalTableAlterAddColumn() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"  
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "ALTER TABLE G1 ADD COLUMN e4 integer PRIMARY KEY OPTIONS(x 10)";
        
        Database db = helpParse(ddl);
        Schema s = db.getSchema("test");
        Table t = s.getTable("G1");
        assertNotNull(t);  
        assertTrue(t.getColumns().size() == 4);
        assertNotNull(t.getColumnByName("e1"));
        assertNotNull(t.getColumnByName("e2"));
        assertNotNull(t.getColumnByName("e3"));
        assertNotNull(t.getColumnByName("e4"));
        
        Column c = t.getColumnByName("e4");
        assertEquals("integer", c.getRuntimeType());
        assertNotNull(t.getPrimaryKey().getColumnByName("e4"));
        assertEquals("10", c.getProperty("x", false));
    }
    
    @Test 
    public void testPhysicalTableAlterDropColumn() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"  
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "ALTER TABLE G1 DROP COLUMN e1";
        
        Database db = helpParse(ddl);
        Schema s = db.getSchema("test");
        Table t = s.getTable("G1");
        assertNotNull(t);  
        assertTrue(t.getColumns().size() == 2);
        assertNull(t.getColumnByName("e1"));
        assertNotNull(t.getColumnByName("e2"));
        assertNotNull(t.getColumnByName("e3"));
    }     
    
    @Test 
    public void testDropPhysicalTable() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"  
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "DROP FOREIGN TABLE G1";
        
        Database db = helpParse(ddl);
        Schema s = db.getSchema("test");
        Table t = s.getTable("G1");
        assertNull(t);  
    }       
    
    @Test 
    public void testAlterViewAddColumn2() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE VIRTUAL SCHEMA test;"
                + "SET SCHEMA test;"
                + "CREATE VIRTUAL VIEW G1 AS SELECT 1 as e1, '2' as e2, curdate() as e3;"
                + "ALTER VIEW G1 AS SELECT 1 as e1, '2' as e2, curdate() as e3, 'foo' as e4;";
        
        Database db = helpParse(ddl);
        Schema s = db.getSchema("test");
        Table t = s.getTable("G1");
        assertEquals("SELECT 1 AS e1, '2' AS e2, curdate() AS e3, 'foo' AS e4", t.getSelectTransformation());
    }    
    
    @Test 
    public void testDropView() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE VIRTUAL SCHEMA test;"
                + "SET SCHEMA test;"
                + "CREATE VIRTUAL VIEW G1 AS SELECT 1 as e1, '2' as e2, curdate() as e3;"
                + "DROP VIEW G1";
        
        Database db = helpParse(ddl);
        Schema s = db.getSchema("test");
        Table t = s.getTable("G1");
        assertNull(t);
    }     
    
	@Test 
	public void testFunction() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"  
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN FUNCTION SourceFunc(flag Boolean) RETURNS varchar";

        FunctionMethod method = null;
		Database db = helpParse(ddl);
		Schema s = db.getSchema("test");
		for (FunctionMethod fm : s.getFunctions().values()) {
			if (fm.getName().equalsIgnoreCase("SourceFunc")) {
				method = fm;
				break;
			}
		}
		assertNotNull(method);
		assertEquals("boolean", method.getInputParameters().get(0).getType());
	}
	
	@Test 
	public void testDropFunction() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"  
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FUNCTION SourceFunc(flag Boolean) RETURNS varchaR options (UUID 'z');"
                + "DROP FUNCTION SourceFunc;";

		FunctionMethod method = null;
		Database db = helpParse(ddl);
		Schema s = db.getSchema("test");
		for (FunctionMethod fm : s.getFunctions().values()) {
			if (fm.getName().equalsIgnoreCase("SourceFunc")) {
				method = fm;
				break;
			}
		}
		assertNull(method);	
	}	
	
    @Test 
    public void testDropSchema() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE VIRTUAL SCHEMA test;"
                + "SET SCHEMA test;"
                + "CREATE VIRTUAL VIEW G1 AS SELECT 1 as e1, '2' as e2, curdate() as e3;"
                + "DROP VIRTUAL SCHEMA test";
        
        Database db = helpParse(ddl);
        Schema s = db.getSchema("test");
        assertNull(s);
    } 	
    
    @Test(expected=MetadataException.class) 
    public void testDropDataWrapper() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2';"
                + "CREATE FOREIGN DATA WRAPPER orcl;"
                + "CREATE SERVER x TYPE 'oracle' VERSION '2.0' FOREIGN DATA WRAPPER orcl OPTIONS (k1 'v1');"
                + "DROP FOREIGN DATA WRAPPER orcl;";
        		
        Database db = helpParse(ddl);
        assertNull(db.getDataWrapper("orcl"));
    } 
    
    @Test 
    public void testDropDataWrappers() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2';"
                + "USE DATABASE FOO version '2';"
                + "CREATE FOREIGN DATA WRAPPER orcl;"
                + "CREATE SERVER x TYPE 'oracle' VERSION '2.0' FOREIGN DATA WRAPPER orcl OPTIONS (k1 'v1');"
                + "DROP SERVER x;"
                + "DROP FOREIGN DATA WRAPPER orcl;";
        		
        Database db = helpParse(ddl);
        assertNull(db.getDataWrapper("orcl"));
    }    
    
    @Test 
    public void testCreateProcedure() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"  
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN PROCEDURE procG1(P1 integer) RETURNS (e1 integer, e2 varchar)";
        
        Database db = helpParse(ddl);
        Schema s = db.getSchema("test");
        Procedure p = s.getProcedure("procG1");
        assertNotNull(p);  
        assertEquals(1, p.getParameters().size());
        assertNotNull(p.getParameterByName("P1"));
        assertEquals(2, p.getResultSet().getColumns().size());
        assertEquals("e1", p.getResultSet().getColumns().get(0).getName());
    }         

    @Test 
    public void testDropProcedure() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"  
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN PROCEDURE procG1(P1 integer) RETURNS (e1 integer, e2 varchar);"
                + "DROP FOREIGN PROCEDURE procG1";
        
        Database db = helpParse(ddl);
        Schema s = db.getSchema("test");
        Procedure p = s.getProcedure("procG1");
        assertNull(p);  
    }
}
