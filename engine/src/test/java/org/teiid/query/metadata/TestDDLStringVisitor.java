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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.DataWrapper;
import org.teiid.metadata.Database;
import org.teiid.metadata.Database.ResourceType;
import org.teiid.metadata.Grant;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Role;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Server;
import org.teiid.metadata.Table;
import org.teiid.query.parser.SQLParserUtil;
import org.teiid.query.parser.TestDDLParser;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestDDLStringVisitor {

	@Test
	public void testForeignTable() throws Exception {
		
		String ddl = "SET NAMESPACE 'http://www.teiid.org/ext/relational/2012' AS teiid_rel;\n\nCREATE FOREIGN TABLE G1 (\n" + 
				"	e1 integer,\n" + 
				"	e2 string(10),\n" + 
				"	e3 date NOT NULL DEFAULT current_date() OPTIONS (\"teiid_rel:default_handling\" 'expression'),\n" + 
				"	e4 bigdecimal(12,3),\n" + 
				"	e5 integer AUTO_INCREMENT OPTIONS (UUID 'uuid', NAMEINSOURCE 'nis', SELECTABLE FALSE),\n" + 
				"	e6 string DEFAULT 'hello',\n" +
				"	PRIMARY KEY(e1),\n" +
				"	UNIQUE(e2),\n" +
				"	UNIQUE(e3),\n" +
				"	INDEX(e5),\n" +
				"	INDEX(e6)\n" +
				") OPTIONS (ANNOTATION 'Test Table', CARDINALITY '12', FOO 'BAR', UPDATABLE 'true', UUID 'uuid2');";
		
		MetadataFactory mf = new MetadataFactory("test", 1, "model", TestDDLParser.getDataTypes(), new Properties(), null);
		
		Table table = mf.addTable("G1");
		table.setVirtual(false);
		
		mf.addColumn("e1","integer", table);
		Column e2 = mf.addColumn("e2","varchar", table);
		e2.setLength(10);
		
		Column e3 = mf.addColumn("e3","date", table);
		e3.setNullType(NullType.No_Nulls);
		SQLParserUtil.setDefault(e3, new Function("current_date", new Expression[0]));
		
		Column e4 = mf.addColumn("e4","decimal", table);
		e4.setPrecision(12);
		e4.setScale(3);
		
		Column e5 = mf.addColumn("e5","integer", table);
		e5.setAutoIncremented(true);
		e5.setUUID("uuid");
		e5.setNameInSource("nis");
		e5.setSelectable(false);
		
		Column e6 = mf.addColumn("e6","varchar", table);
		e6.setDefaultValue("hello");
		
		mf.addPrimaryKey("PK", Arrays.asList("e1"), table);
		mf.addIndex("UNIQUE0", false, Arrays.asList("e2"), table);
		mf.addIndex("UNIQUE1", false, Arrays.asList("e3"), table);
		mf.addIndex("INDEX0", true, Arrays.asList("e5"), table);
		mf.addIndex("INDEX1", true, Arrays.asList("e6"), table);
		
		Map<String, String> options = new HashMap<String, String>();
		options.put("CARDINALITY", "12");
		options.put("UUID", "uuid2");
		options.put("UPDATABLE", "true");
		options.put("FOO", "BAR");
		options.put("ANNOTATION", "Test Table");
		table.setProperties(options);
		
		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
		assertEquals(ddl, metadataDDL);
	}
	
	@Test
	public void testMultiKeyPK() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date, PRIMARY KEY (e1, e2))";
		String expected = "CREATE FOREIGN TABLE G1 (\n" + 
				"	e1 integer,\n" + 
				"	e2 string,\n" + 
				"	e3 date,\n" + 
				"	PRIMARY KEY(e1, e2)\n" + 
				");";
		helpTest(ddl, expected);
	}	
	
	@Test
	public void testConstraints2() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date, " +
				"ACCESSPATTERN(e1), UNIQUE(e1) options (x true), ACCESSPATTERN(e2, e3))";
		String expected = "CREATE FOREIGN TABLE G1 (\n" + 
				"	e1 integer,\n" + 
				"	e2 string,\n" + 
				"	e3 date,\n" + 
				"	ACCESSPATTERN(e1),\n	ACCESSPATTERN(e2, e3),\n	UNIQUE(e1) OPTIONS (x 'true')\n" + 
				");";
		helpTest(ddl, expected);
	}		
	
	@Test
	public void testFK() throws Exception {
		String ddl = "CREATE FOREIGN TABLE G1(\"g1-e1\" integer, g1e2 varchar, PRIMARY KEY(\"g1-e1\", g1e2));\n" +
				"CREATE FOREIGN TABLE G2( g2e1 integer, g2e2 varchar, " +
				"FOREIGN KEY (g2e1, g2e2) REFERENCES G1 (\"g1-e1\", g1e2))";
		
		String expected = "CREATE FOREIGN TABLE G1 (\n" + 
				"	\"g1-e1\" integer,\n" + 
				"	g1e2 string,\n" + 
				"	PRIMARY KEY(\"g1-e1\", g1e2)\n" + 
				");\n" + 
				"\n" + 
				"CREATE FOREIGN TABLE G2 (\n" + 
				"	g2e1 integer,\n" + 
				"	g2e2 string,\n" + 
				"	FOREIGN KEY(g2e1, g2e2) REFERENCES G1 (\"g1-e1\", g1e2)\n" + 
				");";
		
		TransformationMetadata vdb = RealMetadataFactory.fromDDL(ddl, "x", "y");
		Schema s = vdb.getModelID("y");
		String metadataDDL = DDLStringVisitor.getDDLString(s, null, null);
		assertEquals(expected, metadataDDL);
	}	
	
	@Test
	public void testOptionalFK() throws Exception {
		String ddl = "CREATE FOREIGN TABLE \"G1+\"(g1e1 integer, g1e2 varchar, PRIMARY KEY(g1e1, g1e2));\n" +
				"CREATE FOREIGN TABLE G2( g2e1 integer, g2e2 varchar, PRIMARY KEY(g2e1, g2e2)," +
				"FOREIGN KEY (g2e1, g2e2) REFERENCES G1)";
		String expected = "CREATE FOREIGN TABLE \"G1+\" (\n" + 
				"	g1e1 integer,\n" + 
				"	g1e2 string,\n" + 
				"	PRIMARY KEY(g1e1, g1e2)\n" + 
				");\n" + 
				"\n" + 
				"CREATE FOREIGN TABLE G2 (\n" + 
				"	g2e1 integer,\n" + 
				"	g2e2 string,\n" + 
				"	PRIMARY KEY(g2e1, g2e2),\n	FOREIGN KEY(g2e1, g2e2) REFERENCES G1 \n" + 
				");";
		helpTest(ddl, expected);
	}	

	@Test
	public void testFKWithOptions() throws Exception {
		String ddl = "CREATE FOREIGN TABLE \"G1+\"(g1e1 integer, g1e2 varchar, PRIMARY KEY(g1e1, g1e2));\n" +
				"CREATE FOREIGN TABLE G2( g2e1 integer, g2e2 varchar, PRIMARY KEY(g2e1, g2e2)," +
				"FOREIGN KEY (g2e1, g2e2) REFERENCES G1 OPTIONS (NAMEINSOURCE 'g1Relationship'))  ";
		String expected = "CREATE FOREIGN TABLE \"G1+\" (\n" + 
				"	g1e1 integer,\n" + 
				"	g1e2 string,\n" + 
				"	PRIMARY KEY(g1e1, g1e2)\n" + 
				");\n" + 
				"\n" + 
				"CREATE FOREIGN TABLE G2 (\n" + 
				"	g2e1 integer,\n" + 
				"	g2e2 string,\n" + 
				"	PRIMARY KEY(g2e1, g2e2),\n	FOREIGN KEY(g2e1, g2e2) REFERENCES G1  OPTIONS (NAMEINSOURCE 'g1Relationship')\n" + 
				");";
		helpTest(ddl, expected);
	}		
	
	@Test
	public void testMultipleCommands() throws Exception {
		String ddl = "CREATE VIEW V1 AS SELECT * FROM PM1.G1; " +
				"CREATE PROCEDURE FOO(P1 integer) RETURNS (e1 integer, e2 varchar) AS SELECT * FROM PM1.G1;";
		String expected = "CREATE VIEW V1\n" + 
				"AS\n" + 
				"SELECT * FROM PM1.G1;\n" + 
				"\n" + 
				"CREATE VIRTUAL PROCEDURE FOO(IN P1 integer) RETURNS TABLE (e1 integer, e2 string)\n" + 
				"AS\n" + 
				"SELECT * FROM PM1.G1;;";
		helpTest(ddl, expected);
		
	}	
	
	@Test 
	public void testView() throws Exception {
		String ddl = "CREATE View G1( e1 integer, e2 varchar) OPTIONS (CARDINALITY 1234567890123) AS select e1, e2 from foo.bar";
		String expected = "CREATE VIEW G1 (\n" + 
				"	e1 integer,\n" + 
				"	e2 string\n" + 
				") OPTIONS (CARDINALITY 1234567954432)\n" + 
				"AS\n" + 
				"SELECT e1, e2 FROM foo.bar;";
		helpTest(ddl, expected);
	}
	
	@Test 
    public void testViewWithUntyped() throws Exception {
        String ddl = "CREATE View G1( e2, e3) OPTIONS (CARDINALITY 1234567890123) AS select e1, e2 from foo.bar";
        String expected = "CREATE VIEW G1 (\n" + 
                "\te2,\n" + 
                "\te3\n" + 
                ") OPTIONS (CARDINALITY 1234567954432)\n" + 
                "AS\n" + 
                "SELECT e1, e2 FROM foo.bar;";
        helpTest(ddl, expected);
    }
	
	@Test
	public void testInsteadOfTrigger() throws Exception {
		String ddl = 	"CREATE VIEW G1( e1 integer, e2 varchar) AS select * from foo;" +
						"CREATE TRIGGER ON G1 INSTEAD OF INSERT AS " +
						"FOR EACH ROW \n" +
						"BEGIN ATOMIC \n" +
						"insert into g1 (e1, e2) values (1, 'trig');\n" +
						"END;";

		String expected = "CREATE VIEW G1 (\n" + 
				"	e1 integer,\n" + 
				"	e2 string\n" + 
				")\n" + 
				"AS\n" + 
				"SELECT * FROM foo;\n" + 
				"\n" + 
				"CREATE TRIGGER ON G1 INSTEAD OF INSERT AS\n" + 
				"FOR EACH ROW\n" + 
				"BEGIN ATOMIC\n" + 
				"INSERT INTO g1 (e1, e2) VALUES (1, 'trig');\n" + 
				"END;";
		helpTest(ddl, expected);
	}	
	
	@Test
	public void testSourceProcedure() throws Exception {
		String ddl = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean options(foo 'bar'), p2 varchar, INOUT p3 decimal) " +
				"RETURNS options(x 'y') (r1 varchar, r2 decimal)" +
				"OPTIONS(RANDOM 'any', UUID 'uuid', NAMEINSOURCE 'nis', ANNOTATION 'desc', UPDATECOUNT '2');";
		
		String expected = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean OPTIONS (foo 'bar'), IN p2 string, INOUT p3 bigdecimal) RETURNS OPTIONS (x 'y') TABLE (r1 string, r2 bigdecimal)\n" + 
				"OPTIONS (UUID 'uuid', ANNOTATION 'desc', NAMEINSOURCE 'nis', UPDATECOUNT 2, RANDOM 'any');";
		helpTest(ddl, expected);		
	}
	
	@Test
	public void testSourceFunction() throws Exception {
		String ddl = "CREATE FOREIGN Function myProc(p1 boolean options(foo 'bar'), p2 varchar) " +
				"RETURNS options(x 'y') varchar " +
				"OPTIONS(RANDOM 'any');";
		
		String expected = "CREATE FOREIGN FUNCTION myProc(p1 boolean, p2 string) RETURNS OPTIONS (x 'y') string\n"
				+ "OPTIONS (RANDOM 'any');";
		helpTest(ddl, expected);		
	}
	
	@Test
	public void testPushdownFunctionNoArgs() throws Exception {
		String ddl = "CREATE FOREIGN FUNCTION SourceFunc() RETURNS integer OPTIONS (UUID 'hello world')";
		String expected = "CREATE FOREIGN FUNCTION SourceFunc() RETURNS integer\n" + 
				"OPTIONS (UUID 'hello world');";
		helpTest(ddl, expected);
	}	
	
	@Test
	public void testNonPushdownFunction() throws Exception {
		String ddl = "CREATE FUNCTION SourceFunc(p1 integer, p2 varchar) RETURNS integer OPTIONS (JAVA_CLASS 'foo', JAVA_MEHTOD 'bar')";
		String expected = "CREATE VIRTUAL FUNCTION SourceFunc(p1 integer, p2 string) RETURNS integer\n" + 
				"OPTIONS (JAVA_CLASS 'foo', JAVA_MEHTOD 'bar');";
		helpTest(ddl, expected);
	}
	
	@Test
	public void testNonPushdownFunction1() throws Exception {
		String ddl = "CREATE VIRTUAL FUNCTION SourceFunc(p1 integer, p2 string) RETURNS integer\n" + 
				"as return repeat(p2, p1);";
		String expected = "CREATE VIRTUAL FUNCTION SourceFunc(OUT \"return\" integer RESULT, IN p1 integer, IN p2 string)\nAS\nRETURN repeat(p2, p1);;";
		helpTest(ddl, expected);
	}

	private void helpTest(String ddl, String expected) {
		Schema s = TestDDLParser.helpParse(ddl, "model").getSchema();
		String metadataDDL = DDLStringVisitor.getDDLString(s, null, null);
		assertEquals(expected, metadataDDL);
	}	
	
	@Test
	public void testSourceProcedureVariadic() throws Exception {
		String ddl = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean, VARIADIC p3 decimal) " +
				"RETURNS (r1 varchar, r2 decimal);";
		
		String expected = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean, VARIADIC p3 bigdecimal) RETURNS TABLE (r1 string, r2 bigdecimal);";
		helpTest(ddl, expected);		
	}	
	
	@Test public void testViewFBI() throws Exception {
		String ddl = "CREATE View G1( \"a e1\" integer, \"a e2\" varchar, INDEX (\"a e1\", upper(\"a e2\"))) AS select e1, e2 from foo.bar";
		String expected = "CREATE VIEW G1 (\n	\"a e1\" integer,\n	\"a e2\" string,\n	INDEX(\"a e1\", upper(\"a e2\"))\n)\nAS\nSELECT e1, e2 FROM foo.bar;";
		helpTest(ddl, expected);
	}
	
	@Test public void testNamespaces() throws Exception {
		String ddl = "set namespace 'some long thing' as x; CREATE View G1(a integer, b varchar) options (\"teiid_rel:x\" false, \"x:z\" 'stringval') AS select e1, e2 from foo.bar";
		String expected = "SET NAMESPACE 'http://www.teiid.org/ext/relational/2012' AS teiid_rel;\nSET NAMESPACE 'some long thing' AS n1;\n\nCREATE VIEW G1 (\n	a integer,\n	b string\n) OPTIONS (\"teiid_rel:x\" 'false', \"n1:z\" 'stringval')\nAS\nSELECT e1, e2 FROM foo.bar;";
		helpTest(ddl, expected);
	}
	
    @Test 
    public void testNamespaces3() throws Exception {
    	String ddl = "set namespace 'some long thing' as x; CREATE View G1( e1 integer, e2 varchar) OPTIONS (\"x:z\" 'stringval') AS select e1, e2 from foo.bar";
		Schema s = TestDDLParser.helpParse(ddl, "model").getSchema();

		Database db = new Database("foo", "2");
		db.addSchema(s);
        
		String expected = "\n/*\n" + 
				"###########################################\n" + 
				"# START DATABASE foo\n" + 
				"###########################################\n" + 
				"*/\n" + 
				"CREATE DATABASE foo VERSION '2';\n" + 
				"USE DATABASE foo VERSION '2';\n" + 
				"SET NAMESPACE 'some long thing' AS n0;\n" + 
				"\n" + 
				"\n" + 
				"--############ Schemas ############\n" + 
				"CREATE SCHEMA model;\n" + 
				"\n" + 
				"\n" + 
				"--############ Schema:model ############\n" + 
				"SET SCHEMA model;\n" + 
				"\n" + 
				"CREATE VIEW G1 (\n" + 
				"	e1 integer,\n" + 
				"	e2 string\n" + 
				") OPTIONS (\"n0:z\" 'stringval')\n" + 
				"AS\n" + 
				"SELECT e1, e2 FROM foo.bar;\n" + 
				"/*\n" + 
				"###########################################\n" + 
				"# END DATABASE foo\n" + 
				"###########################################\n" + 
				"*/\n\n";		
		String metadataDDL = DDLStringVisitor.getDDLString(db);
		assertEquals(expected, metadataDDL);

    }	
	
	@Test public void testNamespaces1() throws Exception {
        String ddl = "CREATE View G1(a integer, b varchar) options (\"teiid_rel:x\" false, \"teiid_sf:z\" 'stringval') AS select e1, e2 from foo.bar";
        String expected = "SET NAMESPACE 'http://www.teiid.org/ext/relational/2012' AS teiid_rel;\n" + 
                "SET NAMESPACE 'http://www.teiid.org/translator/salesforce/2012' AS teiid_sf;\n" + 
                "\n" + 
                "CREATE VIEW G1 (\n" + 
                "\ta integer,\n" + 
                "\tb string\n" + 
                ") OPTIONS (\"teiid_rel:x\" 'false', \"teiid_sf:z\" 'stringval')\n" + 
                "AS\n" + 
                "SELECT e1, e2 FROM foo.bar;";
        helpTest(ddl, expected);
    }
	
	@Test public void testGlobalTemporaryTable() throws Exception {
		String ddl = "create global temporary table myTemp (x string, y serial, primary key (x))";
		String expected = "CREATE GLOBAL TEMPORARY TABLE myTemp (\n	x string,\n	y SERIAL,\n	PRIMARY KEY(x)\n);";
		helpTest(ddl, expected);
	}
	
	@Test public void testArrayTypes() throws Exception {
		String ddl = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean, p2 varchar, INOUT p3 decimal) " +
				"RETURNS (r1 varchar(100)[], r2 decimal[][])";
		
		String expected = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean, IN p2 string, INOUT p3 bigdecimal) RETURNS TABLE (r1 string(100)[], r2 bigdecimal[][]);";
		helpTest(ddl, expected);		
	}
	
	@Test public void testGeometry() throws Exception {
		String ddl = "CREATE foreign table G1( e1 geometry)";
		String expected = "CREATE FOREIGN TABLE G1 (\n" + 
				"	e1 geometry\n" + 
				");";
		helpTest(ddl, expected);
	}
	
	@Test public void testGeography() throws Exception {
        String ddl = "CREATE foreign table G1( e1 geography)";
        String expected = "CREATE FOREIGN TABLE G1 (\n" + 
                "\te1 geography\n" + 
                ");";
        helpTest(ddl, expected);
    }
	
	public static String[] someMethod(int param) {return null;}
	
	@Test public void testFunction() throws Exception {
		Schema s = new Schema();
		s.addFunction(MetadataFactory.createFunctionFromMethod("x", TestDDLStringVisitor.class.getMethod("someMethod", int.class)));
		String metadataDDL = DDLStringVisitor.getDDLString(s, null, null);
		assertEquals("CREATE VIRTUAL FUNCTION x(param1 integer) RETURNS string[]"
				+ "\nOPTIONS (NAMEINSOURCE 'x', JAVA_CLASS 'org.teiid.query.metadata.TestDDLStringVisitor', JAVA_METHOD 'someMethod');", metadataDDL);
	}
	
	@Test public void testAfterTrigger() throws Exception {
	    String ddl = "CREATE FOREIGN TABLE T ( e1 integer, e2 varchar);" +
                "CREATE TRIGGER \"table\" ON T AFTER UPDATE AS " +
                "FOR EACH ROW \n" +
                "BEGIN ATOMIC \n" +
                "if (\"new\" is not distinct from \"old\") raise sqlexception 'error';\n" +
                "END;"+
                "CREATE TRIGGER \"table1\" ON T AFTER UPDATE AS " +
                "FOR EACH ROW \n" +
                "BEGIN ATOMIC \n" +
                "if (\"new\" is not distinct from \"old\") raise sqlexception 'error';\n" +
                "END;";
	    
	    String expected = "CREATE FOREIGN TABLE T (\n\te1 integer,\n\te2 string\n);\n"
	            + "\nCREATE TRIGGER \"table\" ON T INSTEAD OF UPDATE AS\nFOR EACH ROW\nBEGIN ATOMIC\nIF(\"new\" IS NOT DISTINCT FROM \"old\")\nBEGIN\nRAISE SQLEXCEPTION 'error';\nEND\nEND;\n"
	            + "\nCREATE TRIGGER table1 ON T INSTEAD OF UPDATE AS\nFOR EACH ROW\nBEGIN ATOMIC\nIF(\"new\" IS NOT DISTINCT FROM \"old\")\nBEGIN\nRAISE SQLEXCEPTION 'error';\nEND\nEND;";
	    helpTest(ddl, expected);
	}
    @Test 
    public void testDatabase() throws Exception {
        Database db = new Database("foo", "2");
        String metadataDDL = DDLStringVisitor.getDDLString(db);
        String expected = "\n"
        		+ "/*\n" + 
                "###########################################\n" + 
                "# START DATABASE foo\n" + 
                "###########################################\n"
                + "*/\n" + 
                "CREATE DATABASE foo VERSION '2';\n" +
                "USE DATABASE foo VERSION '2';\n" +
                "\n" +
                "/*\n" +
                "###########################################\n" + 
                "# END DATABASE foo\n" + 
                "###########################################\n"
                + "*/\n" + 
                "\n";
        assertEquals(expected, metadataDDL);
    }
    
    @Test 
    public void testDatabaseWithDomains() throws Exception {
        String expected = "\n"
                + "/*\n" + 
                "###########################################\n" + 
                "# START DATABASE foo\n" + 
                "###########################################\n"
                + "*/\n" + 
                "CREATE DATABASE foo VERSION '2';\n" +
                "USE DATABASE foo VERSION '2';\n" +
                "\n" +
                "--############ Domains ############\n" +
                "CREATE DOMAIN x AS string(1000) NOT NULL;\n\n" + 
                "CREATE DOMAIN y AS integer NOT NULL;\n\n" +
                "CREATE DOMAIN z AS bigdecimal(10,2) NOT NULL;\n\n" +
                "\n--############ Schemas ############\n" +
                "CREATE VIRTUAL SCHEMA SchemaA;\n\n" +
                "\n--############ Schema:SchemaA ############\n" +
                "SET SCHEMA SchemaA;\n" +
                "\n" + 
                "CREATE VIEW G1 (\n" + 
                "\te1 x,\n" + 
                "\te2 y\n" + 
                ")\nAS\nSELECT 'a', 1;\n" +
                "/*\n"+
                "###########################################\n" + 
                "# END DATABASE foo\n" + 
                "###########################################\n"
                + "*/\n" + 
                "\n";
        
        Database db = TestDDLParser.helpParse(expected);
        
        String metadataDDL = DDLStringVisitor.getDDLString(db);
        
        assertEquals(expected, metadataDDL);
    }
    
    @Test 
    public void testSchema() throws Exception {
        Database db = new Database("foo", "2");
        
        DataWrapper dw = new DataWrapper("orcle");
        db.addDataWrapper(dw);
        
        Server s = new Server("testing");
        s.setDataWrapper(dw.getName());
        s.setJndiName("java://test-server");
        s.setType("orcl");
        db.addServer(s);
        
        String table = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar)";
        Table t = TestDDLParser.helpParse(table, "SchemaA").getSchema().getTable("G1");
        Schema schema = new Schema();
        schema.setName("SchemaA");
        schema.addTable(t);
        schema.addServer(s);
        db.addSchema(schema);
        
        String metadataDDL = DDLStringVisitor.getDDLString(db);
        String expected = "\n" + 
        		"/*\n" +
                "###########################################\n" + 
                "# START DATABASE foo\n" + 
                "###########################################\n" + 
                "*/\n" +
                "CREATE DATABASE foo VERSION '2';\n" + 
                "USE DATABASE foo VERSION '2';" +
                "\n" + 
                "\n--############ Translators ############\n" +
                "CREATE FOREIGN DATA WRAPPER orcle;\n" + 
                "\n" +
                "\n--############ Servers ############\n" +
                "CREATE SERVER testing TYPE 'orcl' FOREIGN DATA WRAPPER orcle OPTIONS (\"jndi-name\" 'java://test-server');\n" + 
                "\n" + 
                "\n--############ Schemas ############\n" +
                "CREATE SCHEMA SchemaA SERVER testing;\n\n" +
                "\n--############ Schema:SchemaA ############\n" +
                "SET SCHEMA SchemaA;\n" +
                "\n" + 
                "CREATE FOREIGN TABLE G1 (\n" + 
                "\te1 integer,\n" + 
                "\te2 string\n" + 
                ");\n" + 
                "/*\n" +
                "###########################################\n" + 
                "# END DATABASE foo\n" + 
                "###########################################\n" + 
                "*/\n" +
                "\n"; 
        assertEquals(expected, metadataDDL);
    }
    
    @Test 
    public void testGrants() throws Exception {
        Database db = new Database("foo", "2");
        
        Role role = new Role("admin");
        role.setAnyAuthenticated(true);        
        
        Role role1 = new Role("uber");
        
        Grant.Permission permission = new Grant.Permission();
        permission.setAllowAlter(true);
        permission.setAllowSelect(true);
        permission.setResourceName("schema.tableName");
        permission.setResourceType(ResourceType.TABLE);
        
        Grant.Permission permission2 = new Grant.Permission();
        permission2.setAllowDelete(true);
        permission2.setResourceName("schema.tableName");
        permission2.setResourceType(ResourceType.TABLE);        
        
        Grant.Permission permission3 = new Grant.Permission();
        permission3.setAllowAllPrivileges(true);
        permission3.setAllowTemporyTables(true);
        
        Grant.Permission permission4 = new Grant.Permission();
        permission4.setAllowTemporyTables(true);
        
        Grant g = new Grant();
        g.setRole(role.getName());
        g.addPermission(permission);
        g.addPermission(permission4);

        Grant g2 = new Grant();
        g2.setRole(role.getName());
        g2.addPermission(permission2);
        
        Grant g3 = new Grant();
        g3.setRole("uber");
        g3.addPermission(permission3);
        
        db.addRole(role);
        db.addRole(role1);
        db.addGrant(g);
        db.addGrant(g2);
        db.addGrant(g3);
        
        
        String expected = "\n" + 
        		"/*\n" +
                "###########################################\n" + 
                "# START DATABASE foo\n" + 
                "###########################################\n" + 
                "*/\n" +
                "CREATE DATABASE foo VERSION '2';\n" + 
                "USE DATABASE foo VERSION '2';\n" +
                "\n" + 
                "--############ Roles ############\n"+
                "CREATE ROLE admin WITH ANY AUTHENTICATED;\n\n" + 
                "CREATE ROLE uber;\n\n\n" +
                "--############ Grants ############\n"+
                "GRANT SELECT,DELETE,ALTER ON TABLE \"schema.tableName\" TO admin;\n" + 
                "GRANT TEMPORARY TABLE TO admin;\n\n" +
                "GRANT ALL PRIVILEGES TO uber;\n" +
                "GRANT TEMPORARY TABLE TO uber;\n" +
                "\n" + 
                "\n" + 
                "/*\n" +
                "###########################################\n" + 
                "# END DATABASE foo\n" + 
                "###########################################\n" + 
                "*/\n" +
                "\n";
        
        String metadataDDL = DDLStringVisitor.getDDLString(db);
        assertEquals(expected, metadataDDL);        
    }    
}
