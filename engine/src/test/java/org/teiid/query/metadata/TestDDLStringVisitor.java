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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.query.parser.TestDDLParser;

@SuppressWarnings("nls")
public class TestDDLStringVisitor {

	@Test
	public void testForeignTable() throws Exception {
		
		String ddl = "CREATE FOREIGN TABLE G1 (\n" + 
				"	e1 integer,\n" + 
				"	e2 string(10),\n" + 
				"	e3 date NOT NULL,\n" + 
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
				"FOREIGN KEY (g2e1, g2e2) REFERENCES G1 (g1e1, g1e2))";
		
		String expected = "CREATE FOREIGN TABLE G1 (\n" + 
				"	\"g1-e1\" integer,\n" + 
				"	g1e2 string,\n" + 
				"	PRIMARY KEY(\"g1-e1\", g1e2)\n" + 
				");\n" + 
				"\n" + 
				"CREATE FOREIGN TABLE G2 (\n" + 
				"	g2e1 integer,\n" + 
				"	g2e2 string,\n" + 
				"	FOREIGN KEY(g2e1, g2e2) REFERENCES G1 (g1e1, g1e2)\n" + 
				");";
		
		helpTest(ddl, expected);
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
	public void testMultipleCommands() throws Exception {
		String ddl = "CREATE VIEW V1 AS SELECT * FROM PM1.G1 " +
				"CREATE PROCEDURE FOO(P1 integer) RETURNS (e1 integer, e2 varchar) AS SELECT * FROM PM1.G1;";
		String expected = "CREATE VIEW V1\n" + 
				"AS\n" + 
				"SELECT * FROM PM1.G1;\n" + 
				"\n" + 
				"CREATE VIRTUAL PROCEDURE FOO(IN P1 integer) RETURNS TABLE (e1 integer, e2 string)\n" + 
				"AS\n" + 
				"SELECT * FROM PM1.G1;";
		helpTest(ddl, expected);
		
	}	
	
	@Test 
	public void testView() throws Exception {
		String ddl = "CREATE View G1( e1 integer, e2 varchar) OPTIONS (CARDINALITY 12) AS select e1, e2 from foo.bar";
		String expected = "CREATE VIEW G1 (\n" + 
				"	e1 integer,\n" + 
				"	e2 string\n" + 
				") OPTIONS (CARDINALITY 12)\n" + 
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
		String ddl = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean, p2 varchar, INOUT p3 decimal) " +
				"RETURNS (r1 varchar, r2 decimal)" +
				"OPTIONS(RANDOM 'any', UUID 'uuid', NAMEINSOURCE 'nis', ANNOTATION 'desc', UPDATECOUNT '2');";
		
		String expected = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean, IN p2 string, INOUT p3 bigdecimal) RETURNS TABLE (r1 string, r2 bigdecimal)\n" + 
				"OPTIONS (UUID 'uuid', ANNOTATION 'desc', NAMEINSOURCE 'nis', UPDATECOUNT 2, RANDOM 'any')";
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

	private void helpTest(String ddl, String expected) {
		Schema s = TestDDLParser.helpParse(ddl, "model").getSchema();
		String metadataDDL = DDLStringVisitor.getDDLString(s, null, null);
		assertEquals(expected, metadataDDL);
	}	
	
	@Test
	public void testSourceProcedureVariadic() throws Exception {
		String ddl = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean, VARIADIC p3 decimal) " +
				"RETURNS (r1 varchar, r2 decimal);";
		
		String expected = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean, VARIADIC p3 bigdecimal) RETURNS TABLE (r1 string, r2 bigdecimal)";
		helpTest(ddl, expected);		
	}	
	
	@Test public void testViewFBI() throws Exception {
		String ddl = "CREATE View G1( \"a e1\" integer, \"a e2\" varchar, INDEX (\"a e1\", upper(\"a e2\"))) AS select e1, e2 from foo.bar";
		String expected = "CREATE VIEW G1 (\n	\"a e1\" integer,\n	\"a e2\" string,\n	INDEX(\"a e1\", upper(\"a e2\"))\n)\nAS\nSELECT e1, e2 FROM foo.bar;";
		helpTest(ddl, expected);
	}
	
	@Test public void testNamespaces() throws Exception {
		String ddl = "set namespace 'some long thing' as x; CREATE View G1(a integer, b varchar) options (\"teiid_rel:x\" false, \"x:z\" 'stringval') AS select e1, e2 from foo.bar";
		String expected = "SET NAMESPACE 'some long thing' AS n0;\n\nCREATE VIEW G1 (\n	a integer,\n	b string\n) OPTIONS (\"teiid_rel:x\" 'false', \"n0:z\" 'stringval')\nAS\nSELECT e1, e2 FROM foo.bar;";
		helpTest(ddl, expected);
	}
}
