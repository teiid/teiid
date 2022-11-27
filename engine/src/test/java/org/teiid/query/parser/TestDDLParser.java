package org.teiid.query.parser;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.Database;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Permission;
import org.teiid.metadata.Permission.Privilege;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Role;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Server;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.metadata.Trigger;
import org.teiid.query.metadata.BasicQueryMetadata;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.DatabaseStore;
import org.teiid.query.metadata.DatabaseStore.Mode;
import org.teiid.query.metadata.DatabaseUtil;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;

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
        assertEquals("integer", e1.getDatatype().getName());
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
        assertEquals("integer", e5.getDatatype().getName());
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

    @Test(expected=MetadataException.class)
    public void testVirtualTable() throws Exception {
        String ddl = "CREATE VIEW G1(e4 string)";

        helpParse(ddl, "model").getSchema();
    }

    @Test(expected=MetadataException.class)
    public void testTableDefinition() throws Exception {
        String ddl = "CREATE FOREIGN TABLE G1(e4 string); alter view g1 as select 'a';";

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
    public void testRenameTable() throws Exception {
        QueryParser parser = new QueryParser();
        String ddl = "CREATE FOREIGN TABLE G1( e1 integer auto_increment primary key, e2 varchar);"
                + "ALTER TABLE G1 RENAME TO G2";
        MetadataStore mds = new MetadataStore();
        MetadataFactory mf = new MetadataFactory("x", 1, "model", getDataTypes(), new Properties(), null);
        parser.parseDDL(mf, ddl);
        mf.mergeInto(mds);
    }

    @Test
    public void testRenameTableQualified() throws Exception {
        QueryParser parser = new QueryParser();
        String ddl = "CREATE FOREIGN TABLE G1( e1 integer auto_increment primary key, e2 varchar);"
                + "ALTER TABLE model.G1 RENAME TO G2";
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
    public void testAlterAddPK() throws Exception {
        String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date); ALTER TABLE G1 ADD PRIMARY KEY (e1, e2);";

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
        vdb.addAttachment(QueryMetadataInterface.class, new BasicQueryMetadata());

        ValidatorReport report = new MetadataValidator().validate(vdb, s.asMetadataStore());

        assertFalse(report.hasItems());

        assertEquals(fk.getReferenceKey().getColumns(), tableMap.get("G1").getColumns());
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
        vdb.addAttachment(QueryMetadataInterface.class, new BasicQueryMetadata());

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
        vdb.addAttachment(QueryMetadataInterface.class, new BasicQueryMetadata());

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

        assertEquals(fk.getReferenceKey().getColumns(), s.getSchema("model").getTable("G1").getColumns());
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
    public void testViewWithoutColumnTypes() throws Exception {
        QueryParser parser = new QueryParser();
        MetadataStore mds = new MetadataStore();
        MetadataFactory mf = new MetadataFactory("x", 1, "VM1", getDataTypes(), new Properties(), null);
        parser.parseDDL(mf,"CREATE VIEW V1 (e11, e12, e13, e14) AS SELECT * FROM PM1.G1");
        mf.mergeInto(mds);
    }

    @Test
    public void testRenameView() throws Exception {
        QueryParser parser = new QueryParser();
        MetadataStore mds = new MetadataStore();
        MetadataFactory mf = new MetadataFactory("x", 1, "VM1", getDataTypes(), new Properties(), null);
        parser.parseDDL(mf,"CREATE VIEW V1 AS SELECT * FROM PM1.G1;ALTER VIEW V1 RENAME TO V2");
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
        assertEquals("integer", fm.getOutputParameter().getRuntimeType());
        assertEquals(FunctionMethod.PushDown.MUST_PUSHDOWN, fm.getPushdown());
    }

    @Test(expected=org.teiid.metadata.ParseException.class)
    public void testDuplicateFunctions() throws Exception {
        String ddl = "CREATE FUNCTION SourceFunc() RETURNS integer; CREATE FUNCTION SourceFunc() RETURNS string";
        helpParse(ddl, "model");
    }

    @Test(expected=org.teiid.metadata.ParseException.class)
    public void testDuplicateFunctions1() throws Exception {
        String ddl = "CREATE FUNCTION SourceFunc() RETURNS string OPTIONS (UUID 'a'); CREATE FUNCTION SourceFunc1() RETURNS string OPTIONS (UUID 'a')";
        helpParse(ddl, "model");
    }

    @Test()
    public void testDuplicateFunctions2() throws Exception {
        String ddl = "CREATE FUNCTION SourceFunc() RETURNS string; CREATE FUNCTION SourceFunc(param string) RETURNS string";
        helpParse(ddl, "model");
    }

    @Test(expected=org.teiid.metadata.ParseException.class)
    public void testFunctionDefault() throws Exception {
        String ddl = "CREATE FUNCTION SourceFunc(param string default 'a') RETURNS string";
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
        assertEquals("string", fm.getOutputParameter().getRuntimeType());
        assertEquals(FunctionMethod.PushDown.CAN_PUSHDOWN, fm.getPushdown());
        assertEquals(2, fm.getInputParameterCount());
        assertEquals("flag", fm.getInputParameters().get(0).getName());
        assertEquals("boolean", fm.getInputParameters().get(0).getRuntimeType());
        assertEquals("msg", fm.getInputParameters().get(1).getName());
        assertEquals("string", fm.getInputParameters().get(1).getRuntimeType());
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
        assertEquals("string", fm.getOutputParameter().getRuntimeType());
        assertEquals(FunctionMethod.PushDown.CAN_PUSHDOWN, fm.getPushdown());
        assertEquals(2, fm.getInputParameterCount());
        assertEquals("flag", fm.getInputParameters().get(0).getName());
        assertEquals("boolean", fm.getInputParameters().get(0).getRuntimeType());
        assertEquals("msg", fm.getInputParameters().get(1).getName());
        assertEquals("string", fm.getInputParameters().get(1).getRuntimeType());
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
        assertEquals("boolean", fm.getInputParameters().get(0).getRuntimeType());
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
        String ddl =     "CREATE VIEW G1( e1 integer, e2 varchar) AS select * from foo;" +
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
        String ddl =     "CREATE VIEW G1( e1 integer, e2 varchar) AS select * from foo;" +
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
        String ddl =     "CREATE TRIGGER ON G1 INSTEAD OF INSERT AS " +
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

    @Test(expected=org.teiid.metadata.ParseException.class)
    public void testNamespace() throws Exception {
        QueryParser parser = new QueryParser();
        String ddl =     "set namespace 'http://teiid.org' AS xyz";
        MetadataFactory mf = new MetadataFactory("x", 1, "model", getDataTypes(), new Properties(), null);
        parser.parseDDL(mf, ddl);
    }

    @Test
    public void testReservedNamespace1() throws Exception {
        QueryParser parser = new QueryParser();
        String ddl = "set namespace 'http://www.teiid.org/translator/salesforce/2012' AS teiid_sf";
        MetadataFactory mf = new MetadataFactory("x", 1, "model", getDataTypes(), new Properties(), null);
        parser.parseDDL(mf, ddl);
    }

    @Test(expected=MetadataException.class)
    public void testReservedNamespaceURIWrong() throws Exception {
        QueryParser parser = new QueryParser();
        String ddl = "set namespace 'http://www.teiid.org/translator/salesforce/2013' AS teiid_sf";
        MetadataFactory mf = new MetadataFactory("x", 1, "model", getDataTypes(), new Properties(), null);
        parser.parseDDL(mf, ddl);
    }

    public static MetadataFactory helpParse(String ddl, String model) {
        MetadataFactory mf = new MetadataFactory("x", 1, model, getDataTypes(), new Properties(), null);
        QueryParser.getQueryParser().parseDDL(mf, ddl);
        return mf;
    }

    public static Database helpParse(String ddl) {
        return helpParse(ddl, Mode.ANY);
    }

    public static Database helpParse(String ddl, DatabaseStore.Mode mode) {
        final Map<String, Datatype> dataTypes = getDataTypes();
        DatabaseStore store = new DatabaseStore() {
            @Override
            public Map<String, Datatype> getRuntimeTypes() {
                return dataTypes;
            }
            @Override
            protected TransformationMetadata getTransformationMetadata() {
                Database database = getCurrentDatabase();

                CompositeMetadataStore store = new CompositeMetadataStore(database.getMetadataStore());
                //grants are already stored on the VDBMetaData
                store.getRoles().clear();
                return new TransformationMetadata(DatabaseUtil.convert(database), store, null,
                        null, null).getDesignTimeMetadata();
            }
        };
        store.startEditing(true);
        store.setMode(mode);
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
    public void testAlterTableAlterColumnType() throws Exception {
        String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);" +
                "ALTER FOREIGN TABLE G1 ALTER COLUMN e1 TYPE varchar(12);" +
                "ALTER FOREIGN TABLE G1 ALTER COLUMN e2 TYPE serial;" +
                "ALTER FOREIGN TABLE G1 ALTER COLUMN e3 TYPE integer not null auto_increment;";

        Schema s = helpParse(ddl, "model").getSchema();
        Map<String, Table> tableMap = s.getTables();

        assertTrue("Table not found", tableMap.containsKey("G1"));
        Table table = tableMap.get("G1");

        Column e1 = table.getColumnByName("e1");
        assertNotNull(e1);
        assertEquals("string", e1.getRuntimeType());
        assertEquals(NullType.Nullable, e1.getNullType());

        Column e2 = table.getColumnByName("e2");
        assertNotNull(e2);
        assertEquals("integer", e2.getRuntimeType());
        assertTrue(e2.isAutoIncremented());
        assertEquals(NullType.No_Nulls, e2.getNullType());

        Column e3 = table.getColumnByName("e3");
        assertNotNull(e3);
        assertEquals("integer", e3.getRuntimeType());
        assertTrue(e3.isAutoIncremented());
        assertEquals(NullType.No_Nulls, e3.getNullType());
    }

    @Test
    public void testAlterTableRenameColumn() throws Exception {
        String ddl = "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);" +
                "ALTER FOREIGN TABLE G1 RENAME COLUMN e3 TO e3renamed;";

        Schema s = helpParse(ddl, "model").getSchema();
        Map<String, Table> tableMap = s.getTables();

        assertTrue("Table not found", tableMap.containsKey("G1"));
        Table table = tableMap.get("G1");

        Column e3 = table.getColumnByName("e3renamed");
        assertNotNull(e3);
        assertEquals("date", e3.getRuntimeType());
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
    public void testAlterProcedureColumnType() throws Exception {
        String ddl = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean, p2 varchar, INOUT p3 decimal) " +
                "RETURNS (r1 varchar, r2 decimal)" +
                "OPTIONS(RANDOM 'any', UUID 'uuid', NAMEINSOURCE 'nis', ANNOTATION 'desc', UPDATECOUNT '2');" +
                "ALTER FOREIGN PROCEDURE myProc ALTER PARAMETER p2 TYPE integer;";

        Schema s = helpParse(ddl, "model").getSchema();
        Procedure proc = s.getProcedure("myProc");
        assertNotNull(proc);

        assertEquals("p2", proc.getParameters().get(1).getName());
        assertEquals("integer", proc.getParameters().get(1).getRuntimeType());
    }

    @Test
    public void testAlterProcedureColumnRename() throws Exception {
        String ddl = "CREATE FOREIGN PROCEDURE myProc(OUT p1 boolean, p2 varchar, INOUT p3 decimal) " +
                "RETURNS (r1 varchar, r2 decimal)" +
                "OPTIONS(RANDOM 'any', UUID 'uuid', NAMEINSOURCE 'nis', ANNOTATION 'desc', UPDATECOUNT '2');" +
                "ALTER FOREIGN PROCEDURE myProc RENAME PARAMETER p2 TO p2renamed;";

        Schema s = helpParse(ddl, "model").getSchema();
        Procedure proc = s.getProcedure("myProc");
        assertNotNull(proc);

        assertEquals("p2renamed", proc.getParameters().get(1).getName());
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

    @Test(expected=org.teiid.metadata.ParseException.class)
    public void testCreateDataWrapperWithOptionsNoType() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2';"
                + "USE DATABASE FOO version '2';"
                + "CREATE FOREIGN DATA WRAPPER orcl options (x 'y');";
        helpParse(ddl);
    }

    @Test
    public void testCreateServerNoType() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2';"
                + "USE DATABASE FOO version '2';"
                + "CREATE FOREIGN DATA WRAPPER orcl;"
                + "CREATE SERVER x FOREIGN DATA WRAPPER orcl;";

        Database db = helpParse(ddl);

        assertEquals("FOO", db.getName());
        assertEquals("2", db.getVersion());
        Server s = db.getServer("x");
        assertNotNull(s);
        assertNull(s.getType());
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

    @Ignore
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
                + "CREATE SCHEMA S1 SERVER x OPTIONS (x 'y', visible false, annotation 'foo');";

        Database db = helpParse(ddl);

        // schema test
        Schema schema = db.getSchema("S1");
        assertNotNull(schema);
        assertEquals("y", schema.getProperty("x", false));
        assertFalse(schema.isVisible());
        assertEquals("foo", schema.getAnnotation());

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

        assertEquals("[x, y]", role.getMappedRoles().toString());
    }

    @Test
    public void testRoleAnyAuth() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE ROLE superuser WITH ANY AUTHENTICATED;";

        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);

        assertEquals("[]", role.getMappedRoles().toString());
        assertTrue(role.isAnyAuthenticated());
    }

    @Test
    public void testDropRole() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE ROLE superuser WITH ANY AUTHENTICATED;"
                + "DROP ROLE superuser";

        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNull(role);

    }

    @Test
    public void testPolicy() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"
                + "CREATE SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "CREATE ROLE superuser WITH FOREIGN ROLE x,y;"
                + "CREATE POLICY p ON test.G1 TO superuser USING (e1 =1 and e2 =2);"
                + "CREATE POLICY p1 ON test.G1 FOR SELECT TO superuser USING (true);"
                + "CREATE POLICY p3 ON test.G1 FOR SELECT TO superuser USING (e1 =1);";

        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);

        Collection<Role> roles = db.getRoles();
        assertEquals(1, roles.size());
        Role r = roles.iterator().next();
        assertEquals(0, r.getGrants().size());
        assertEquals(1, r.getPolicies().size());
        assertEquals("(e1 = 1) AND (e2 = 2)", r.getPolicies().values().iterator().next().get("p").getCondition());

        assertEquals("\n" +
                "/*\n" +
                "###########################################\n" +
                "# START DATABASE FOO\n" +
                "###########################################\n" +
                "*/\n" +
                "CREATE DATABASE FOO VERSION '1';\n" +
                "USE DATABASE FOO VERSION '1';\n" +
                "\n" +
                "--############ Servers ############\n" +
                "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');\n" +
                "\n" +
                "\n" +
                "--############ Schemas ############\n" +
                "CREATE SCHEMA test SERVER pgsql;\n" +
                "\n" +
                "\n" +
                "--############ Roles ############\n" +
                "CREATE ROLE superuser WITH FOREIGN ROLE x y;\n" +
                "\n" +
                "\n" +
                "--############ Schema:test ############\n" +
                "SET SCHEMA test;\n" +
                "\n" +
                "CREATE FOREIGN TABLE G1 (\n" +
                "\te1 integer,\n" +
                "\te2 string,\n" +
                "\te3 date\n" +
                ");\n" +
                "--############ Grants ############\n" +
                "\n" +
                "\n" +
                "--############ Policies ############\n" +
                "CREATE POLICY p ON test.G1 TO superuser USING ((e1 = 1) AND (e2 = 2));\n" +
                "CREATE POLICY p1 ON test.G1 FOR SELECT TO superuser USING (TRUE);\n" +
                "CREATE POLICY p3 ON test.G1 FOR SELECT TO superuser USING (e1 = 1);\n" +
                "\n" +
                "\n" +
                "/*\n" +
                "###########################################\n" +
                "# END DATABASE FOO\n" +
                "###########################################\n" +
                "*/\n" +
                "\n", DDLStringVisitor.getDDLString(db));
    }

    @Test(expected=org.teiid.metadata.ParseException.class)
    public void testDuplicatePolicy() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"
                + "CREATE SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "CREATE ROLE superuser WITH FOREIGN ROLE x,y;"
                + "CREATE POLICY p ON test.G1 TO superuser USING (e1 =1);"
                + "CREATE POLICY p ON test.G1 FOR SELECT TO superuser USING (true);";

        helpParse(ddl);
    }

    @Test(expected=org.teiid.metadata.ParseException.class)
    public void testStringCondition() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"
                + "CREATE SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "CREATE ROLE superuser WITH FOREIGN ROLE x,y;"
                + "CREATE POLICY p ON test.G1 TO superuser USING ('e1 =1');";

        helpParse(ddl);
    }

    @Test
    public void testDropPolicy() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"
                + "CREATE SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "CREATE ROLE superuser WITH FOREIGN ROLE x,y;"
                + "CREATE POLICY p ON test.G1 TO superuser USING (e1 =1);"
                + "DROP POLICY p ON test.G1 TO superuser";

        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);

        Collection<Role> roles = db.getRoles();
        assertEquals(1, roles.size());
        Role r = roles.iterator().next();
        assertEquals(0, r.getGrants().size());
        assertEquals(1, r.getPolicies().size());
        assertEquals(0, r.getPolicies().values().iterator().next().size());
    }

    @Test(expected=org.teiid.metadata.ParseException.class)
    public void testDropMissingPolicy() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"
                + "CREATE SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "CREATE ROLE superuser WITH FOREIGN ROLE x,y;"
                + "DROP POLICY p1 ON test.G1 TO superuser";

        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);

        Collection<Role> roles = db.getRoles();
        assertEquals(1, roles.size());
        Role r = roles.iterator().next();
        assertEquals(0, r.getGrants().size());
        assertEquals(1, r.getPolicies().size());
        assertEquals("e1 = 1", r.getPolicies().values().iterator().next().get("p").getCondition());
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
                + "CREATE ROLE superuser WITH FOREIGN ROLE x,y;"
                + "GRANT SELECT,INSERT,DELETE ON TABLE test.G1 TO superuser;"
                + "GRANT UPDATE ON TABLE test.G1 TO superuser;";

        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);

        Collection<Role> roles = db.getRoles();
        assertEquals(1, roles.size());
        Role r = roles.iterator().next();
        assertEquals(1, r.getGrants().size());
        Permission p = r.getGrants().values().iterator().next();
        assertTrue(p.hasPrivilege(Privilege.SELECT));
        assertTrue(p.hasPrivilege(Privilege.INSERT));
        assertTrue(p.hasPrivilege(Privilege.DELETE));
        assertTrue(p.hasPrivilege(Privilege.UPDATE));
        assertNull(p.hasPrivilege(Privilege.DROP));
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
                + "CREATE ROLE superuser WITH JAAS ROLE x,y;"
                + "GRANT ALL PRIVILEGES TO superuser;";

        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);

        Collection<Role> roles = db.getRoles();
        assertEquals(1, roles.size());
        Role r = roles.iterator().next();
        assertEquals(1, r.getGrants().size());
        Permission p = r.getGrants().values().iterator().next();
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
                + "CREATE ROLE superuser WITH JAAS ROLE x,y;"
                + "CREATE ROLE otheruser WITH JAAS ROLE y;"
                + "CREATE ROLE someone WITH JAAS ROLE x;"
                + "GRANT SELECT ON TABLE test.G1 CONDITION CONSTRAINT 'foo=bar' TO superuser;"
                + "GRANT SELECT ON TABLE test.G1 CONDITION 'foo>bar' TO otheruser;"
                + "GRANT SELECT ON TABLE test.G1 CONDITION NOT CONSTRAINT 'foo<bar' TO someone;"
                + "GRANT SELECT ON COLUMN test.G1.e1 MASK ORDER 1 'null' CONDITION foo>1 TO someone;";

        Database db = helpParse(ddl);
        Role role = db.getRole("otheruser");
        assertNotNull(role);
        Collection<Permission> grants = role.getGrants().values();
        assertEquals(1, grants.size());
        Permission p = grants.iterator().next();
        assertTrue(p.hasPrivilege(Privilege.SELECT));
        assertEquals("foo>bar", p.getCondition());
        assertNull(p.isConditionAConstraint());

        role = db.getRole("someone");
        assertNotNull(role);
        grants = role.getGrants().values();
        assertEquals(2, grants.size());
        Iterator<Permission> iter = grants.iterator();
        p = iter.next();
        assertTrue(p.hasPrivilege(Privilege.SELECT));
        assertEquals("foo<bar", p.getCondition());
        assertFalse(p.isConditionAConstraint());
        p = iter.next();
        assertTrue(p.hasPrivilege(Privilege.SELECT));
        assertEquals("foo > 1", p.getCondition());
        assertEquals("null", p.getMask());
        assertEquals(Integer.valueOf(1), p.getMaskOrder());

        role = db.getRole("superuser");
        assertNotNull(role);
        grants = role.getGrants().values();
        assertEquals(1, grants.size());
        p = grants.iterator().next();
        assertTrue(p.hasPrivilege(Privilege.SELECT));
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
                + "CREATE ROLE superuser WITH ANY AUTHENTICATED;"
                + "GRANT SELECT,INSERT,DELETE ON TABLE test.G1 TO superuser;"
                + "GRANT UPDATE ON TABLE test.G1 TO superuser;"
                + "REVOKE SELECT ON TABLE test.G1 FROM superuser;";

        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);

        Collection<Role> roles = db.getRoles();
        assertEquals(1, roles.size());
        Role r = roles.iterator().next();
        assertEquals(1, r.getGrants().size());
        Permission p = r.getGrants().values().iterator().next();
        assertNull(p.hasPrivilege(Privilege.SELECT));
        assertTrue(p.hasPrivilege(Privilege.INSERT));
        assertTrue(p.hasPrivilege(Privilege.DELETE));
        assertTrue(p.hasPrivilege(Privilege.UPDATE));
        assertNull(p.hasPrivilege(Privilege.DROP));
    }

    @Test
    public void testRevokeAll() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN TABLE G1( e1 integer, e2 varchar, e3 date);"
                + "CREATE ROLE superuser WITH ANY AUTHENTICATED;"
                + "GRANT SELECT,INSERT,DELETE ON TABLE G1 TO superuser;"
                + "GRANT UPDATE ON TABLE test.G1 TO superuser;"
                + "REVOKE ALL PRIVILEGES FROM superuser;";

        Database db = helpParse(ddl);
        Role role = db.getRole("superuser");
        assertNotNull(role);

        Collection<Role> roles = db.getRoles();
        assertEquals(1, roles.size());
        Role r = roles.iterator().next();
        assertEquals(3, r.getGrants().size());
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
        assertEquals("boolean", method.getInputParameters().get(0).getRuntimeType());
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

    @Test(expected=MetadataException.class)
    public void testDropProcedureWrongType() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER postgresql;"
                + "CREATE SERVER pgsql TYPE 'custom' FOREIGN DATA WRAPPER postgresql OPTIONS (\"jndi-name\" 'jndiname');"
                + "CREATE  SCHEMA test SERVER pgsql;"
                + "SET SCHEMA test;"
                + "CREATE FOREIGN PROCEDURE procG1(P1 integer) RETURNS (e1 integer, e2 varchar);"
                + "DROP VIRTUAL PROCEDURE procG1";

        Database db = helpParse(ddl);
        Schema s = db.getSchema("test");
        Procedure p = s.getProcedure("procG1");
        assertNull(p);
    }

    @Test(expected=MetadataException.class)
    public void testCreateDomainAlreadyExists() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2.0.0'; USE DATABASE FOO VERSION '2.0.0';"
                 + "CREATE DOMAIN \"INTEGER\" AS string(4000)";

        helpParse(ddl);
    }

    @Test
    public void testCreateDomain() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2.0.0'; USE DATABASE FOO VERSION '2.0.0';"
                 + "CREATE DOMAIN nnint AS integer not null;";

        Database db = helpParse(ddl);
        assertEquals(NullType.No_Nulls, db.getMetadataStore().getDatatypes().get("NNINT").getNullType());
    }

    @Test
    public void testCreateDomainNonReserved() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2.0.0'; USE DATABASE FOO VERSION '2.0.0';"
                 + "CREATE DOMAIN text AS string;";

        Database db = helpParse(ddl);
        assertEquals(NullType.Nullable, db.getMetadataStore().getDatatypes().get("text").getNullType());
    }

    @Test(expected=org.teiid.metadata.ParseException.class)
    public void testCreateDomainEffectivelyReserved() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2.0.0'; USE DATABASE FOO VERSION '2.0.0';"
                 + "CREATE DOMAIN exception AS integer not null;";

        Database db = helpParse(ddl);
        assertEquals(NullType.No_Nulls, db.getMetadataStore().getDatatypes().get("NNINT").getNullType());
    }

    @Test(expected=org.teiid.metadata.ParseException.class)
    public void testCreateInvalidDomain() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2.0.0'; USE DATABASE FOO VERSION '2.0.0';"
                 + "CREATE DOMAIN foo.bar AS integer not null;";

        helpParse(ddl);
    }

    @Test
    public void testCreateDomainUsedInSchema() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2.0.0'; USE DATABASE FOO VERSION '2.0.0';"
                 + "CREATE DOMAIN my_string AS string(1000) not null;"
                 + "CREATE SCHEMA S1; SET SCHEMA S1;"
                 + "CREATE VIEW X (y my_string) as select 'a';";

        Database db = helpParse(ddl);
        assertEquals(1000, db.getMetadataStore().getDatatypes().get("my_string").getLength());
    }

    @Test(expected=MetadataException.class) public void testDDLNotAllowed() throws Exception {
        String ddl = "CREATE DATABASE FOO;";

        helpParse(ddl, "model");
    }

    @Test(expected=MetadataException.class)
    public void testDDLOutOfSequence() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2.0.0'; USE DATABASE FOO VERSION '2.0.0';"
                 + "CREATE SCHEMA S1; SET SCHEMA S1;"
                 + "CREATE VIEW X (y string) as select 'a';"
                 + "CREATE SCHEMA S2;";

        helpParse(ddl, Mode.DATABASE_STRUCTURE);
    }

    @Test(expected=org.teiid.metadata.ParseException.class)
    public void testCreateInvalidQualifiedName() throws Exception {
        String ddl = "CREATE DATABASE FOO VERSION '2.0.0'; USE DATABASE FOO VERSION '2.0.0';"
                 + "CREATE SCHEMA S1; SET SCHEMA S1;"
                 + "CREATE VIEW S2.X (y string) as select 'a';";

        helpParse(ddl);
    }

    @Test
    public void testVdbPropertyNamespace() throws Exception {
        String ddl = "create database vdb version '1.2.0' OPTIONS (\"teiid_rest:auto-generate\" 'true'); "
                + "USE DATABASE vdb VERSION '1.2.0';"
                + "CREATE SCHEMA S1; SET SCHEMA S1;"
                + "CREATE VIEW X (y varchar) as select 'a';";

        Database db = helpParse(ddl);
        assertEquals("true", db.getProperty("teiid_rest:auto-generate", false));
    }

    @Test
    public void testViewConstraintWithoutColumns() throws Exception {
        String ddl = "create view v as select 1 as c1, 2 as c2;\n" +
                "alter view v add primary key (c1);";

        MetadataFactory s = helpParse(ddl, "S1");

        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("myVDB"); //$NON-NLS-1$
        ModelMetaData modelOne = new ModelMetaData();
        modelOne.setName("S1"); //$NON-NLS-1$
        vdb.addModel(modelOne);


        MetadataStore mds = new MetadataStore();

        s.mergeInto(mds);

        KeyRecord primaryKey = mds.getSchema("S1").getTable("v").getPrimaryKey();
        //not yet known
        assertNull(primaryKey.getColumns().get(0).getParent());

        TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mds, "myVDB");
        vdb.addAttachment(QueryMetadataInterface.class, tm);

        ValidatorReport report = new MetadataValidator().validate(vdb, s.asMetadataStore());

        assertFalse(report.hasItems());

        primaryKey = mds.getSchema("S1").getTable("v").getPrimaryKey();
        //should be resolved
        assertNotNull(primaryKey.getColumns().get(0).getParent());
    }

    @Test
    public void testViewConstraintWithoutColumnsFails() throws Exception {
        String ddl = "create view v as select 1 as c1, 2 as c2;\n" +
                "alter view v add primary key (c3);";

        MetadataFactory s = helpParse(ddl, "S1");

        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("myVDB"); //$NON-NLS-1$
        ModelMetaData modelOne = new ModelMetaData();
        modelOne.setName("S1"); //$NON-NLS-1$
        vdb.addModel(modelOne);

        MetadataStore mds = new MetadataStore();

        s.mergeInto(mds);

        KeyRecord primaryKey = mds.getSchema("S1").getTable("v").getPrimaryKey();
        //not yet known
        assertNull(primaryKey.getColumns().get(0).getParent());

        TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mds, "myVDB");
        vdb.addAttachment(QueryMetadataInterface.class, tm);

        ValidatorReport report = new MetadataValidator().validate(vdb, s.asMetadataStore());

        assertTrue(report.hasItems());
    }

    @Test public void testPreceedingLineComment() throws Exception {
        String ddl = "create foreign table testRemove (id integer) options (updatable true);"
                + "create virtual procedure deleteTableTest (\n" +
                ") AS \n" +
                "-- \n" +
                "begin\n" +
                "    declare string v;\n" +
                "    execute immediate 'DELETE from testRemove' without return ;\n" +
                "end";


        MetadataFactory s = helpParse(ddl, "S1");

        Procedure p = s.getSchema().getProcedures().get("deleteTableTest");

        assertEquals("-- \n" +
                "BEGIN\n" +
                "DECLARE string v;\n" +
                "EXECUTE IMMEDIATE 'DELETE from testRemove' WITHOUT RETURN;\n" +
                "END", p.getQueryPlan());
    }

    @Test(expected=QueryParserException.class) public void testIncompleteProcedure() throws Exception {
        String ddl = "create virtual procedure deleteTableTest (\n" +
                ") AS \n" +
                "    declare string v;\n" +
                "    execute immediate 'DELETE from testRemove' without return ;\n" +
                "end";

        QueryParser.getQueryParser().parseProcedure(ddl, false);
    }

    @Ignore("alter schema not yet implemented")
    @Test
    public void testAlterSchemaDropOptions() throws Exception {
        String ddl = "CREATE DATABASE FOO;"
                + "USE DATABASE FOO ;"
                + "CREATE FOREIGN DATA WRAPPER orcl;"
                + "CREATE SERVER x TYPE 'oracle' VERSION '2.0' FOREIGN DATA WRAPPER orcl OPTIONS (k1 'v1');"
                + "CREATE SCHEMA S1 SERVER x OPTIONS (x 'y', visible true);"
                + "ALTER SCHEMA S1 OPTIONS (DROP visible);";

        Database db = helpParse(ddl);

        // schema test
        Schema schema = db.getSchema("S1");
        assertTrue(schema.isVisible());
    }

    @Test
    public void testTimestampPrecisionScale() throws Exception {

        String ddl = "CREATE FOREIGN TABLE G1(\n" +
                        "e1 timestamp,\n" +
                        "e2 timestamp(10),\n" +
                        "e3 timestamp(0),\n" +
                        "e4 timestamp(1));";

        Schema s = helpParse(ddl, "model").getSchema();
        Map<String, Table> tableMap = s.getTables();

        assertTrue("Table not found", tableMap.containsKey("G1"));
        Table table = tableMap.get("G1");

        assertEquals(4, table.getColumns().size());

        List<Column> columns = table.getColumns();
        Column e1 = columns.get(0);
        Column e2 = columns.get(1);
        Column e3 = columns.get(2);
        Column e4 = columns.get(3);

        assertTrue(e1.isDefaultPrecisionScale());

        assertTrue(e2.isDefaultPrecisionScale());

        assertEquals(19, e3.getPrecision());
        assertEquals(0, e3.getScale());

        assertEquals(21, e4.getPrecision());
        assertEquals(1, e4.getScale());

        assertEquals("CREATE FOREIGN TABLE G1 (\n" +
                "\te1 timestamp,\n" +
                "\te2 timestamp,\n" +
                "\te3 timestamp(0),\n" +
                "\te4 timestamp(1)\n" +
                ");", DDLStringVisitor.getDDLString(s, null, null));
    }
}
