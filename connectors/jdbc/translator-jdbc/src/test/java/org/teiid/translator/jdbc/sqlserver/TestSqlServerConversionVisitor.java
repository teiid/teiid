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

package org.teiid.translator.jdbc.sqlserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.CommandBuilder;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Command;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestSqlServerConversionVisitor {

    private static SQLServerExecutionFactory trans = new SQLServerExecutionFactory();

    @Before
    public void setUp() throws Exception {
        trans = new SQLServerExecutionFactory();
        trans.setDatabaseVersion(SQLServerExecutionFactory.V_2005);
        trans.start();
    }

    public String getTestVDB() {
        return TranslationHelper.PARTS_VDB;
    }

    public String getBQTVDB() {
        return TranslationHelper.BQT_VDB;
    }

    public void helpTestVisitor(String vdb, String input, String expectedOutput) throws TranslatorException {
        TranslationHelper.helpTestVisitor(vdb, input, expectedOutput, trans);
    }

    @Test
    public void testModFunction() throws Exception {
        String input = "SELECT mod(CONVERT(PART_ID, INTEGER), 13) FROM parts"; //$NON-NLS-1$
        String output = "SELECT (cast(PARTS.PART_ID AS int) % 13) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }

    @Test
    public void testConcatFunction() throws Exception {
        String input = "SELECT concat(part_name, 'b') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT (PARTS.PART_NAME + 'b') FROM PARTS"; //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }

    @Test
    public void testDayOfMonthFunction() throws Exception {
        String input = "SELECT dayofmonth(convert(PARTS.PART_ID, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT {fn dayofmonth(cast(PARTS.PART_ID AS datetime))} FROM PARTS"; //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }

    @Test
    public void testRowLimit() throws Exception {
        String input = "select intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT TOP 100 SmallA.IntKey FROM SmallA"; //$NON-NLS-1$

        helpTestVisitor(getBQTVDB(),
            input,
            output);
    }

    @Test
    public void testRowLimitAndCTE() throws Exception {
        String input = "with x (a) as (select intkey from bqt1.smalla) select a from x union all select a from x limit 100"; //$NON-NLS-1$
        String output = "WITH x (a) AS (SELECT SmallA.IntKey FROM SmallA) SELECT TOP 100 * FROM (SELECT x.a FROM x UNION ALL SELECT x.a FROM x) AS X"; //$NON-NLS-1$

        helpTestVisitor(getBQTVDB(),
            input,
            output);
    }

    @Test
    public void testUnionLimitWithOrderBy() throws Exception {
        String input = "select intkey from bqt1.smalla union select intnum from bqt1.smalla order by intkey limit 100"; //$NON-NLS-1$
        String output = "SELECT TOP 100 * FROM (SELECT SmallA.IntKey FROM SmallA UNION SELECT SmallA.IntNum FROM SmallA) AS X ORDER BY intkey"; //$NON-NLS-1$

        helpTestVisitor(getBQTVDB(),
            input,
            output);
    }

    @Test public void testLimitWithOrderByUnrelated() throws Exception {
        String input = "select intkey from bqt1.smalla order by intnum limit 100"; //$NON-NLS-1$
        String output = "SELECT TOP 100 SmallA.IntKey FROM SmallA ORDER BY SmallA.IntNum"; //$NON-NLS-1$

        helpTestVisitor(getBQTVDB(),
            input,
            output);
    }

    @Test
    public void testDateFunctions() throws Exception {
        String input = "select dayName(timestampValue), dayOfWeek(timestampValue), quarter(timestampValue), week(timestampvalue) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT {fn dayname(SmallA.TimestampValue)}, {fn dayofweek(SmallA.TimestampValue)}, {fn quarter(SmallA.TimestampValue)}, DATEPART(ISO_WEEK, SmallA.TimestampValue) FROM SmallA"; //$NON-NLS-1$

        helpTestVisitor(getBQTVDB(),
            input,
            output);
    }

    @Test public void testConvert() throws Exception {
        String input = "select convert(timestampvalue, date), convert(timestampvalue, string), convert(datevalue, string) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT cast(replace(convert(varchar, SmallA.TimestampValue, 102), '.', '-') AS datetime), convert(varchar, SmallA.TimestampValue, 21), replace(convert(varchar, SmallA.DateValue, 102), '.', '-') FROM SmallA"; //$NON-NLS-1$

        helpTestVisitor(getBQTVDB(),
            input,
            output);
    }

    @Test public void testConvertDate() throws Exception {
        String input = "select stringkey from bqt1.smalla where BQT1.SmallA.DateValue IN (convert('2000-01-12', date), convert('2000-02-02', date))"; //$NON-NLS-1$
        String output = "SELECT SmallA.StringKey FROM SmallA WHERE SmallA.DateValue IN (CAST('2000-01-12 00:00:00.0' AS DATETIME), CAST('2000-02-02 00:00:00.0' AS DATETIME))"; //$NON-NLS-1$

        helpTestVisitor(getBQTVDB(),
            input,
            output);
    }

    @Test public void testConvertTime() throws Exception {
        String input = "select cast('12:00:00' as time) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT CAST('1970-01-01 12:00:00.0' AS DATETIME) FROM SmallA"; //$NON-NLS-1$

        helpTestVisitor(getBQTVDB(),
            input,
            output);
    }

    @Test public void testConvertDate2008() throws Exception {
        trans = new SQLServerExecutionFactory();
        trans.setDatabaseVersion(SQLServerExecutionFactory.V_2008);
        trans.start();
        String input = "select stringkey from bqt1.smalla where BQT1.SmallA.DateValue IN (convert('2000-01-12', date), convert('2000-02-02', date))"; //$NON-NLS-1$
        String output = "SELECT SmallA.StringKey FROM SmallA WHERE SmallA.DateValue IN (CAST('2000-01-12' AS DATE), CAST('2000-02-02' AS DATE))"; //$NON-NLS-1$

        helpTestVisitor(getBQTVDB(),
            input,
            output);
    }

    @Test public void testTimeLiteral2008() throws Exception {
        trans = new SQLServerExecutionFactory();
        trans.setDatabaseVersion(SQLServerExecutionFactory.V_2008);
        trans.start();
        String input = "select stringkey from bqt1.smalla where BQT1.SmallA.TimeValue = {t '00:00:00'}"; //$NON-NLS-1$
        String output = "SELECT SmallA.StringKey FROM SmallA WHERE SmallA.TimeValue = cast('00:00:00' as time)"; //$NON-NLS-1$

        helpTestVisitor(getBQTVDB(),
            input,
            output);
    }

    @Test public void testTimestampConversion2008() throws Exception {
        trans = new SQLServerExecutionFactory();
        trans.setDatabaseVersion(SQLServerExecutionFactory.V_2008);
        trans.start();
        String input = "select stringkey from bqt1.smalla where cast(BQT1.SmallA.Timestampvalue as time) = timevalue"; //$NON-NLS-1$
        String output = "SELECT SmallA.StringKey FROM SmallA WHERE cast(SmallA.TimestampValue AS time) = SmallA.TimeValue"; //$NON-NLS-1$

        helpTestVisitor(getBQTVDB(),
            input,
            output);
    }

    @Test public void testUniqueidentifier() throws Exception {
        MetadataStore metadataStore = new MetadataStore();
        Schema foo = RealMetadataFactory.createPhysicalModel("foo", metadataStore); //$NON-NLS-1$
        Table table = RealMetadataFactory.createPhysicalGroup("bar", foo); //$NON-NLS-1$
        String[] elemNames = new String[] {
            "x",  //$NON-NLS-1$
            "y"  //$NON-NLS-1$
        };
        String[] elemTypes = new String[] {
            DataTypeManager.DefaultDataTypes.STRING,
            DataTypeManager.DefaultDataTypes.STRING
        };
        List<Column> cols =RealMetadataFactory.createElements(table, elemNames, elemTypes);

        Column obj = cols.get(0);
        obj.setNativeType("uniqueidentifier"); //$NON-NLS-1$

        CompositeMetadataStore store = new CompositeMetadataStore(metadataStore);
        QueryMetadataInterface metadata = new TransformationMetadata(null, store, null, RealMetadataFactory.SFM.getSystemFunctions(), null);

        TranslationUtility tu = new TranslationUtility(metadata);
        Command command = tu.parseCommand("select max(x) from bar"); //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("SELECT MAX(bar.x) FROM bar", trans, command); //$NON-NLS-1$

        command = tu.parseCommand("select * from (select max(x) as max from bar) x"); //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("SELECT x.max FROM (SELECT MAX(bar.x) AS max FROM bar) x", trans, command); //$NON-NLS-1$

        command = tu.parseCommand("insert into bar (x) values ('a')"); //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("INSERT INTO bar (x) VALUES ('a')", trans, command); //$NON-NLS-1$

        command = tu.parseCommand("select x from bar where x = 'abc'"); //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("SELECT bar.x FROM bar WHERE bar.x = 'abc'", trans, command); //$NON-NLS-1$

        command = tu.parseCommand("select x from bar where x = y"); //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("SELECT bar.x FROM bar WHERE cast(bar.x as char(36)) = bar.y", trans, command); //$NON-NLS-1$

        command = tu.parseCommand("select x || 'a' from bar"); //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("SELECT (cast(bar.x as char(36)) + 'a') FROM bar", trans, command); //$NON-NLS-1$

        trans = new SQLServerExecutionFactory();
        trans.setDatabaseVersion(SQLServerExecutionFactory.V_2000);
        trans.start();

        command = tu.parseCommand("select max(x) from bar"); //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("SELECT MAX(cast(bar.x as char(36))) FROM bar", trans, command); //$NON-NLS-1$

        command = tu.parseCommand("select * from (select max(x) as max from bar) x"); //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("SELECT x.max FROM (SELECT MAX(cast(bar.x as char(36))) AS max FROM bar) x", trans, command); //$NON-NLS-1$

        command = tu.parseCommand("insert into bar (x) values ('a')"); //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("INSERT INTO bar (x) VALUES ('a')", trans, command); //$NON-NLS-1$

        command = tu.parseCommand("select x from bar where x = 'abc'"); //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("SELECT cast(bar.x as char(36)) FROM bar WHERE cast(bar.x as char(36)) = 'abc'", trans, command); //$NON-NLS-1$

    }

    @Test public void testRowLimitWithInlineViewOrderBy() throws Exception {
        String input = "select intkey from (select intkey from bqt1.smalla) as x order by intkey limit 100"; //$NON-NLS-1$
        String output = "SELECT TOP 100 v_0.c_0 FROM (SELECT g_0.IntKey AS c_0 FROM SmallA g_0) v_0 ORDER BY v_0.c_0"; //$NON-NLS-1$

        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, trans, obj);
    }

    @Test public void testWith() throws Exception {
        String input = "with x as /*+ no_inline */ (select intkey from bqt1.smalla) select intkey from x limit 100"; //$NON-NLS-1$
        String output = "WITH x (IntKey) AS (SELECT g_0.IntKey FROM SmallA g_0) SELECT TOP 100 g_1.intkey AS c_0 FROM x g_1"; //$NON-NLS-1$

        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, trans, obj);
    }

    @Test public void testParseFormat() throws Exception {
        String input = "select parsetimestamp(smalla.timestampvalue, 'yyyy.MM.dd'), formattimestamp(smalla.timestampvalue, 'yy.MM.dd') from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT CONVERT(DATETIME, convert(varchar, g_0.TimestampValue, 21), 102), CONVERT(VARCHAR, g_0.TimestampValue, 2) FROM SmallA g_0"; //$NON-NLS-1$

        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, trans, obj);
    }

    @Test public void testTempTable() throws Exception {
        assertEquals("create table foo (COL1 int, COL2 varchar(100)) ", TranslationHelper.helpTestTempTable(trans, true));
    }

    @Test public void testUnicodeLiteral() throws Exception {
        String input = "select N'\u0FFF'"; //$NON-NLS-1$
        String output = "SELECT N'\u0FFF'"; //$NON-NLS-1$

        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, trans, obj);
    }

    @Test public void testConvertSupport() throws Exception {
        assertTrue(trans.supportsConvert(TypeFacility.RUNTIME_CODES.OBJECT, TypeFacility.RUNTIME_CODES.TIME));
        assertTrue(trans.supportsConvert(TypeFacility.RUNTIME_CODES.OBJECT, TypeFacility.RUNTIME_CODES.INTEGER));
        assertFalse(trans.supportsConvert(TypeFacility.RUNTIME_CODES.OBJECT, TypeFacility.RUNTIME_CODES.CLOB));
        assertTrue(trans.supportsConvert(TypeFacility.RUNTIME_CODES.TIMESTAMP, TypeFacility.RUNTIME_CODES.TIME));
    }

    @Test public void testWithInsert() throws Exception {
        String input = "insert into bqt1.smalla (intkey) with a (x) as /*+ no_inline */ (select intnum from bqt1.smallb) select x from a"; //$NON-NLS-1$
        String output = "WITH a (x) AS (SELECT SmallB.IntNum FROM SmallB) INSERT INTO SmallA (IntKey) SELECT a.x FROM a"; //$NON-NLS-1$

        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, trans, obj);
    }

    @Test public void testLocate() throws Exception {
        String input = "select locate('a', stringkey, 2) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT CHARINDEX('a', g_0.StringKey, 2) FROM SmallA g_0"; //$NON-NLS-1$

        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, trans, obj);
    }

    @Test public void testDateFormat() throws Exception {
        trans = new SQLServerExecutionFactory();
        trans.setDatabaseVersion(SQLServerExecutionFactory.V_2008);
        trans.start();
        Connection c = Mockito.mock(Connection.class);
        Mockito.stub(c.getMetaData()).toReturn(Mockito.mock(DatabaseMetaData.class));
        trans.initCapabilities(c);

        String input = "select cast(smalla.stringkey as date), formatdate(smalla.datevalue, 'dd/MM/yy'), parsedate(smalla.stringkey, 'dd/MM/yy') from bqt1.smalla where smalla.datevalue = {d'2000-01-01'}"; //$NON-NLS-1$
        String output = "SELECT cast(SmallA.StringKey AS date), CONVERT(VARCHAR, cast(SmallA.DateValue AS datetime2), 3), cast(CONVERT(DATETIME2, SmallA.StringKey, 3) AS DATE) FROM SmallA WHERE SmallA.DateValue = CAST('2000-01-01' AS DATE)"; //$NON-NLS-1$

        helpTestVisitor(getBQTVDB(),
            input,
            output);
    }

    @Test public void testRecursiveCTEWithTypeMatching() throws Exception {
        String input = "with a (intkey, stringkey, bigintegervalue) as (select intkey, NULL as stringkey, bigintegervalue from bqt1.smalla where intkey = 1 "
                + "union all "
                + " select n.intkey, n.stringkey, 1 from bqt1.smalla n inner join a rcte on n.intkey = rcte.intkey + 1) "
                + "select * from a";
        String output = "WITH a (intkey, stringkey, bigintegervalue) AS (SELECT cast(g_2.IntKey AS int) AS c_0, cast(NULL AS char) AS c_1, cast(g_2.BigIntegerValue AS numeric(38, 0)) AS c_2 FROM SmallA g_2 WHERE g_2.IntKey = 1 UNION ALL SELECT cast(g_0.IntKey AS int) AS c_0, g_0.StringKey AS c_1, cast(1 AS numeric(38, 0)) AS c_2 FROM SmallA g_0 INNER JOIN a g_1 ON g_0.IntKey = (g_1.intkey + 1)) SELECT g_3.intkey, g_3.stringkey, g_3.bigintegervalue FROM a g_3";

        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, trans, obj);
    }

    @Test
    public void testOffset() throws Exception {
        String input = "select intkey from bqt1.smalla limit 10, 20"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT v.*, ROW_NUMBER() OVER (ORDER BY @@version) ROWNUM_ FROM (SELECT SmallA.IntKey FROM SmallA) v) v WHERE ROWNUM_ <= 30 AND ROWNUM_ > 10"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans);

        input = "select intkey c1 from bqt1.smalla offset 20 rows"; //$NON-NLS-1$
        output = "SELECT c1 FROM (SELECT v.*, ROW_NUMBER() OVER (ORDER BY @@version) ROWNUM_ FROM (SELECT SmallA.IntKey AS c1 FROM SmallA) v) v WHERE ROWNUM_ > 20"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans);

        input = "select intkey c1 from bqt1.smalla order by stringkey offset 20 rows "; //$NON-NLS-1$
        output = "SELECT c1 FROM (SELECT SmallA.IntKey AS c1, ROW_NUMBER() OVER (ORDER BY SmallA.StringKey) AS ROWNUM_ FROM SmallA) v WHERE ROWNUM_ > 20 ORDER BY ROWNUM_"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans);

        input = "select intkey c1, stringkey c2 from bqt1.smalla union all select 1, '2' from bqt1.smallb order by c2 limit 10, 20"; //$NON-NLS-1$
        output = "SELECT c1, c2 FROM (SELECT v.*, ROW_NUMBER() OVER (ORDER BY c2) ROWNUM_ FROM (SELECT SmallA.IntKey AS c1, SmallA.StringKey AS c2 FROM SmallA UNION ALL SELECT 1, '2' FROM SmallB) v) v WHERE ROWNUM_ <= 30 AND ROWNUM_ > 10 ORDER BY ROWNUM_"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans);

        input = "select distinct intkey c1 from bqt1.smalla order by c1 limit 10, 20"; //$NON-NLS-1$
        output = "SELECT c1 FROM (SELECT v.*, ROW_NUMBER() OVER (ORDER BY c1) ROWNUM_ FROM (SELECT DISTINCT SmallA.IntKey AS c1 FROM SmallA) v) v WHERE ROWNUM_ <= 30 AND ROWNUM_ > 10 ORDER BY ROWNUM_"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans);
    }

    @Test
    public void testOffset2012() throws Exception {
        String input = "select intkey from bqt1.smalla limit 10, 20"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA ORDER BY @@version OFFSET 10 ROWS FETCH NEXT 20 ROWS ONLY"; //$NON-NLS-1$

        SQLServerExecutionFactory trans1 = new SQLServerExecutionFactory();
        trans1.setDatabaseVersion(SQLServerExecutionFactory.V_2012);
        trans1.start();

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans1);

        input = "select intkey from bqt1.smalla order by stringkey limit 20"; //$NON-NLS-1$
        output = "SELECT SmallA.IntKey FROM SmallA ORDER BY SmallA.StringKey OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans1);

        input = "select distinct intkey c1 from bqt1.smalla limit 10, 20"; //$NON-NLS-1$
        output = "SELECT c1 FROM (SELECT v.*, ROW_NUMBER() OVER (ORDER BY @@version) ROWNUM_ FROM (SELECT DISTINCT SmallA.IntKey AS c1 FROM SmallA) v) v WHERE ROWNUM_ <= 30 AND ROWNUM_ > 10"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans1);

        input = "select distinct intkey c1 from bqt1.smalla limit 20"; //$NON-NLS-1$
        output = "SELECT c1 FROM (SELECT v.*, ROW_NUMBER() OVER (ORDER BY @@version) ROWNUM_ FROM (SELECT DISTINCT SmallA.IntKey AS c1 FROM SmallA) v) v WHERE ROWNUM_ <= 20 AND ROWNUM_ > 0"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans1);

        input = "select intkey c1 from bqt1.smalla union select intkey c1 from bqt1.smalla limit 10"; //$NON-NLS-1$
        output = "SELECT c1 FROM (SELECT v.*, ROW_NUMBER() OVER (ORDER BY @@version) ROWNUM_ FROM (SELECT SmallA.IntKey AS c1 FROM SmallA UNION SELECT SmallA.IntKey AS c1 FROM SmallA) v) v WHERE ROWNUM_ <= 10 AND ROWNUM_ > 0"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans1);

        input = "select intkey c1 from bqt1.smalla union select intkey c1 from bqt1.smalla limit 1, 10"; //$NON-NLS-1$
        output = "SELECT c1 FROM (SELECT v.*, ROW_NUMBER() OVER (ORDER BY @@version) ROWNUM_ FROM (SELECT SmallA.IntKey AS c1 FROM SmallA UNION SELECT SmallA.IntKey AS c1 FROM SmallA) v) v WHERE ROWNUM_ <= 11 AND ROWNUM_ > 1"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans1);

        input = "select intkey c1 from bqt1.smalla union all select intkey c1 from bqt1.smalla order by c1 limit 1, 10"; //$NON-NLS-1$
        output = "SELECT SmallA.IntKey AS c1 FROM SmallA UNION ALL SELECT SmallA.IntKey AS c1 FROM SmallA ORDER BY c1 OFFSET 1 ROWS FETCH NEXT 10 ROWS ONLY"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans1);
    }

    @Test
    public void testHashFunctions() throws Exception {
        String input = "select md5(stringkey), sha2_256(stringkey) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT HASHBYTES('MD5', SmallA.StringKey), HASHBYTES('SHA2_256', SmallA.StringKey) FROM SmallA"; //$NON-NLS-1$

        SQLServerExecutionFactory trans1 = new SQLServerExecutionFactory();
        trans1.setDatabaseVersion(SQLServerExecutionFactory.V_2012);
        trans1.start();

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans1);
    }

    @Test
    public void testDateTime2() throws Exception {
        String input = "insert into bqt1.smalla (timestampvalue) values ('2001-01-01')"; //$NON-NLS-1$
        String output = "INSERT INTO SmallA (TimestampValue) VALUES ({ts '2001-01-01 00:00:00.0'})"; //$NON-NLS-1$

        SQLServerExecutionFactory trans1 = new SQLServerExecutionFactory();
        trans1.setDatabaseVersion(SQLServerExecutionFactory.V_2008);
        trans1.start();

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, trans1);
    }

    /**
     * the first column expression needs converted to a single clause case
     * the second column condition in the case should not be altered
     * the third column nested boolean expression needs converted accounting for 3 level logic
     */
    @Test public void testBooleanExpression() throws Exception {
        String input = "select stringkey is null, case when stringkey = 'b' then 1 end, coalesce(intkey = 1, true) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN g_0.StringKey IS NULL THEN 1 ELSE 0 END, CASE WHEN g_0.StringKey = 'b' THEN 1 END, isnull(CASE WHEN g_0.IntKey = 1 THEN 1 WHEN NOT (g_0.IntKey = 1) THEN 0 END, 1) FROM SmallA g_0"; //$NON-NLS-1$

        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, trans, obj);
    }

    @Test
    public void testUpperLower() throws Exception {
        String input = "select upper(txt), lower(ntxt), upper(str) from tbl"; //$NON-NLS-1$
        String output = "SELECT upper(cast(tbl.txt AS varchar(max))), lower(cast(tbl.ntxt AS nvarchar(max))), upper(tbl.str) FROM tbl"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor("create foreign table tbl (txt string options (native_type 'text'), ntxt string options (native_type 'ntext'), str string)", input, output, trans);
    }

    @Test
    public void testUpperLowerView() throws Exception {
        String input = "select upper(txt) from (select txt from tbl) v"; //$NON-NLS-1$
        String output = "SELECT upper(v.txt) FROM (SELECT tbl.txt FROM tbl) v"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor("create foreign table tbl (txt string options (native_type 'text'), ntxt string options (native_type 'ntext'), str string)", input, output, trans);
    }

    @Test
    public void testNCharCast() throws Exception {
        String input = "select cast(txt as char) from tbl"; //$NON-NLS-1$
        String output = "SELECT cast(tbl.txt AS nchar(1)) FROM tbl"; //$NON-NLS-1$

        SQLServerExecutionFactory trans1 = new SQLServerExecutionFactory();
        trans1.setDatabaseVersion(SQLServerExecutionFactory.V_2008);
        trans1.start();

        TranslationHelper.helpTestVisitor("create foreign table tbl (txt string options (native_type 'ntext'))", input, output, trans);
    }

    @Test public void testRecursiveCTEWithStringLiteral() throws Exception {
        String input = "WITH tmp_cte(id, name, fk, fkname, lvl) AS \n" +
                "    (SELECT id, name, fk, cast(NULL as string) as fkname, 0 as lvl \n" +
                "            FROM cte_source WHERE fk IS NULL \n" +
                "     UNION ALL \n" +
                "     SELECT e.id, e.name, e.fk, ecte.name as fkname, lvl + 1 as lvl \n" +
                "           FROM cte_source AS e \n" +
                "           INNER JOIN tmp_cte AS ecte ON ecte.id = e.fk\n" +
                "     ) \n" +
                "SELECT * FROM tmp_cte order by lvl"; //$NON-NLS-1$
        String output = "WITH tmp_cte (id, name, fk, fkname, lvl) AS (SELECT cast(cte_source.id AS int), cte_source.name, cast(cte_source.fk AS int), cast(NULL AS varchar(4000)) AS fkname, cast(0 AS int) AS lvl FROM cte_source WHERE cte_source.fk IS NULL UNION ALL SELECT cast(e.id AS int), e.name, cast(e.fk AS int), cast(ecte.name AS varchar(4000)) AS fkname, cast((ecte.lvl + 1) AS int) AS lvl FROM cte_source e INNER JOIN tmp_cte ecte ON ecte.id = e.fk) "
                + "SELECT tmp_cte.id, tmp_cte.name, tmp_cte.fk, tmp_cte.fkname, tmp_cte.lvl FROM tmp_cte ORDER BY tmp_cte.lvl";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor("create foreign table cte_source (id integer, name string options (native_type 'varchar(255)'), fk integer)", input,
                output, trans);
    }

    @Test
    public void testVariantCast() throws Exception {
        assertTrue(trans.supportsConvert(FunctionModifier.OBJECT, FunctionModifier.STRING));
    }

    @Test
    public void testPadFunctions() throws TranslatorException {
        String input = "SELECT lpad(PART_NAME,2), lpad(part_name,2,'x'), rpad(PART_NAME,2), rpad(part_name,2,'x') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT RIGHT(REPLICATE(' ', 2) + LEFT(PARTS.PART_NAME, 2), 2), RIGHT(REPLICATE('x', 2) + LEFT(PARTS.PART_NAME, 2), 2), LEFT(PARTS.PART_NAME + REPLICATE(' ', 2), 2), LEFT(PARTS.PART_NAME + REPLICATE('x', 2), 2) FROM PARTS"; //$NON-NLS-1$
        helpTestVisitor(getTestVDB(),
            input,
            output);
    }

}
