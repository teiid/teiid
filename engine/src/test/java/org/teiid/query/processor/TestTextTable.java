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

package org.teiid.query.processor;

import static org.junit.Assert.*;
import static org.teiid.query.optimizer.TestOptimizer.*;
import static org.teiid.query.processor.TestProcessor.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.sql.rowset.serial.SerialClob;

import org.junit.Test;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.relational.JoinNode;
import org.teiid.query.processor.relational.NestedTableJoinStrategy;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.TestValidator;

@SuppressWarnings({"unchecked", "nls"})
public class TestTextTable {

    @Test public void testCorrelatedTextTable() throws Exception {
        String sql = "select x.* from pm1.g1, texttable(e1 || ',' || e2 COLUMNS x string, y integer) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("a", 0),
                Arrays.asList("a", 3),
                Arrays.asList("c", 1),
                Arrays.asList("b", 2),
                Arrays.asList("a", 0),
        };

        process(sql, expected);
    }

    @Test public void testTextTableFixed() throws Exception {
        String sql = "select max(compkey), max(cdm_id), max(currency), max(\"start\"), max(maturity), max(amount), count(*) from texttable(? COLUMNS compkey string width 76, CDM_ID string width 14, CURRENCY string width 9, \"START\" string width 31, MATURITY string width 31, AMOUNT double width 21, RECORDSOURCE string width 13, SUMMIT_ID string width 15, RATE double width 20, SPREAD double width 20, DESK string width 14) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("000369USD05/20/200405/20/2007", "000369", "USD", "12/18/2000", "12/19/2005", 6.7209685146E8, 52),
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/cdm_dos.txt")));
    }

    @Test public void testTextTableFixedWin() throws Exception {
        String sql = "select max(compkey), max(cdm_id), max(currency), max(\"start\"), max(maturity), max(amount), count(*) from texttable(? COLUMNS compkey string width 76, CDM_ID string width 14, CURRENCY string width 9, \"START\" string width 31, MATURITY string width 31, AMOUNT double width 21, RECORDSOURCE string width 13, SUMMIT_ID string width 15, RATE double width 20, SPREAD double width 20, DESK string width 14) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("000369USD05/20/200405/20/2007", "000369", "USD", "12/18/2000", "12/19/2005", 6.7209685146E8, 52),
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/cdm_dos_win.txt")));
    }

    @Test public void testTextTableFixedPartial() throws Exception {
        String sql = "select max(length(compkey)) from texttable(? COLUMNS compkey string width 76) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(30),
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/cdm_dos.txt")));
    }

    @Test public void testNamedMultilineHeader() throws Exception {
        String sql = "SELECT * from texttable(? COLUMNS Col3Head string HEADER) x";

        List<?>[] expected = new List<?>[] {
            Arrays.asList("VAL2"),
            Arrays.asList("VAL4"),
            Arrays.asList("abc"),
            Arrays.asList("VAL9"),
        };

        FakeDataManager dataManager = new FakeDataManager();

        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/test-file.txt.csv")));
    }

    @Test public void testNamedMultilineHeaderWithOrdinality() throws Exception {
        String sql = "SELECT * from texttable(? COLUMNS Col3Head string, y for ordinality HEADER) x";

        List<?>[] expected = new List<?>[] {
            Arrays.asList("VAL2", 1),
            Arrays.asList("VAL4", 2),
            Arrays.asList("abc", 3),
            Arrays.asList("VAL9", 4),
        };

        FakeDataManager dataManager = new FakeDataManager();

        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/test-file.txt.csv")));
    }

    @Test public void testHeaderWithSkip() throws Exception {
        String sql = "select count(*) from texttable(? COLUMNS PARTNAME string HEADER 3 SKIP 5) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(21),
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/TextParts_HeaderRow2.csv")));
    }

    @Test public void testEscape() throws Exception {
        String sql = "select * from texttable('a\\,b,c\\\na\na,b\\\\' COLUMNS c1 string, c2 string ESCAPE '\\') x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("a,b", "c\na"),
                Arrays.asList("a", "b\\"),
        };

        process(sql, expected);
    }

    @Test(expected=TeiidProcessingException.class) public void testEscapeError() throws Exception {
        String sql = "select * from texttable('axq' COLUMNS c1 string ESCAPE 'x') x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {};

        process(sql, expected);
    }

    @Test public void testDelimiter() throws Exception {
        String sql = "select * from texttable('\na\\,b,c' COLUMNS c1 string, c2 string DELIMITER 'b') x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("a\\,", ",c"),
        };

        process(sql, expected);
    }

    @Test public void testNoRowDelimiter() throws Exception {
        String sql = "select * from texttable('abcdef' COLUMNS c1 string width 1, c2 string width 1 no row delimiter) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("a","b"),
                Arrays.asList("c","d"),
                Arrays.asList("e","f"),
        };

        process(sql, expected);
    }

    @Test public void testNoTrim() throws Exception {
        String sql = "select * from texttable('a b \nc  d' COLUMNS c1 string width 2, c2 string width 2 no trim) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("a","b "),
                Arrays.asList("c"," d"),
        };

        process(sql, expected);
    }

    @Test public void testNoRows() throws Exception {
        String sql = "select * from texttable('' COLUMNS c1 string, c2 string SKIP 3) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {};

        process(sql, expected);
    }

    @Test public void testMissingValues() throws Exception {
        String sql = "select * from texttable('a,b\nc' COLUMNS c1 string, c2 string) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("a", "b"),
                Arrays.asList("c", null),
        };

        process(sql, expected);
    }

    @Test public void testCarraigeReturn() throws Exception {
        String sql = "select * from texttable('a,b\r\nc,d\r\ne,f' COLUMNS c1 string, c2 string) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("a", "b"),
                Arrays.asList("c", "d"),
                Arrays.asList("e", "f"),
        };

        process(sql, expected);
    }

    @Test public void testQuote() throws Exception {
        String sql = "select * from texttable('  \" a\", \" \"\" \"' COLUMNS c1 string, c2 string) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(" a", " \" ")
        };

        process(sql, expected);
    }

    @Test(expected=TeiidProcessingException.class) public void testUnclosedQuoteError() throws Exception {
        String sql = "select * from texttable('  \" a\", \" \"\"' COLUMNS c1 string, c2 string) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {};

        process(sql, expected);
    }

    @Test(expected=TeiidProcessingException.class) public void testQuoteError() throws Exception {
        String sql = "select * from texttable('  \" a\", x\" \"\" \"' COLUMNS c1 string, c2 string) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {};

        process(sql, expected);
    }

    @Test public void testHeaderError() throws Exception {
        String sql = "select * from texttable('notc1,notc2\n1,2' COLUMNS c1 string, c2 string HEADER) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {Arrays.asList(null, null)};

        process(sql, expected);
    }

    @Test public void testTextTableCriteria() throws Exception {
        String sql = "select x.* from texttable('a' || ',' || '1' COLUMNS x string, y integer) x where x.y = 1"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("a", 1),
        };

        process(sql, expected);
    }

    @Test public void testTextTableGroupBy() throws Exception {
        String sql = "select max(x) from texttable('a' || ',' || '1' COLUMNS x string, y integer) x group by y"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("a"),
        };

        process(sql, expected);
    }

    @Test public void testTextTableSubquery() throws Exception {
        String sql = "select x.* from pm1.g1, texttable(e1 || ',' || e2 COLUMNS x string, y integer) x where y < (select avg(e2) from pm1.g1 as x)";

        List<?>[] expected = new List<?>[] {
                Arrays.asList("a", 0),
                Arrays.asList("c", 1),
                Arrays.asList("a", 0),
        };

        process(sql, expected);
    }

    @Test public void testTextTableMultiBatch() throws Exception {
        String sql = "select x.* from (select * from pm1.g1 where e1 = 'c') y, texttable(e1 || '\n' || e2 || '\n' || e3 COLUMNS x string) x";

        List<?>[] expected = new List<?>[] {
                Arrays.asList("c"),
                Arrays.asList("1"),
                Arrays.asList("true"),
        };

        process(sql, expected);
    }

    @Test public void testTextTableJoin() throws Exception {
        String sql = "select z.* from (select x.* from (select * from pm1.g1 where e1 = 'c') y, texttable(e1 || '\n' || e2 || '\n' || e3 COLUMNS x string) x) as z, " +
                "(select x.* from (select * from pm1.g1 where e1 = 'c') y, texttable(e1 || '\n' || e2 || '\n' || e3 COLUMNS x string) x) as z1 where z.x = z1.x";

        List<?>[] expected = new List<?>[] {
                Arrays.asList("c"),
                Arrays.asList("1"),
                Arrays.asList("true"),
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        RelationalPlan plan = (RelationalPlan)helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        JoinNode join = (JoinNode) plan.getRootNode().getChildren()[0];
        assertTrue(!(join.getJoinStrategy() instanceof NestedTableJoinStrategy));
        helpProcess(plan, createCommandContext(), dataManager, expected);
    }


    @Test public void testTextTableJoinPrefetch() throws Exception {
        String sql = "select z.* from (select x.* from (select * from pm1.g1 where e1 = 'c') y, texttable(e1 || '\n' || e2 || '\n' || e3 COLUMNS x string) x) as z";

        List<?>[] expected = new List<?>[] {
                Arrays.asList("c"),
                Arrays.asList("1"),
                Arrays.asList("true"),
        };

        FakeDataManager dataManager = new FakeDataManager();
        dataManager.setBlockOnce();
        sampleData1(dataManager);
        RelationalPlan plan = (RelationalPlan)helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        helpProcess(plan, createCommandContext(), dataManager, expected);
    }

    @Test public void testTextTableJoin1() throws Exception {
        String sql = "select e1, e2 from texttable('a' COLUMNS col string) x, pm1.g1 where col = e1";

        List<?>[] expected = new List<?>[] {
                Arrays.asList("a", 0),
                Arrays.asList("a", 3),
                Arrays.asList("a", 0),
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        RelationalPlan plan = (RelationalPlan)helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        helpProcess(plan, createCommandContext(), dataManager, expected);
    }

    @Test public void testTextTableSelector() throws Exception {
        String sql = "select x.* from (select * from pm1.g1) y, texttable(e1 || '\n' || e2 || '\n' || e3 SELECTOR 'c' COLUMNS x string) x";
        Command c = QueryParser.getQueryParser().parseCommand(sql);

        assertEquals("SELECT x.* FROM (SELECT * FROM pm1.g1) AS y, TEXTTABLE(((((e1 || '\\u000A') || e2) || '\\u000A') || e3) SELECTOR 'c' COLUMNS x string) AS x", c.toString());
        assertEquals("SELECT x.* FROM (SELECT * FROM pm1.g1) AS y, TEXTTABLE(((((e1 || '\\u000A') || e2) || '\\u000A') || e3) SELECTOR 'c' COLUMNS x string) AS x", c.clone().toString());

        List<?>[] expected = new List<?>[] {
                Arrays.asList("c"),
        };

        process(sql, expected);
    }

    @Test public void testTextTableSelector1() throws Exception {
        String sql = "select x.* from texttable('cc,bb' SELECTOR 'c' COLUMNS x string) x";

        Command c = QueryParser.getQueryParser().parseCommand(sql);

        assertEquals("SELECT x.* FROM TEXTTABLE('cc,bb' SELECTOR 'c' COLUMNS x string) AS x", c.toString());
        assertEquals("SELECT x.* FROM TEXTTABLE('cc,bb' SELECTOR 'c' COLUMNS x string) AS x", c.clone().toString());

        List<?>[] expected = new List<?>[] {
        };

        process(sql, expected);
    }

    @Test public void testTextTableSelector2() throws Exception {
        String sql = "select x.* from texttable('A,10-dec-2011,12345,3322,3000,222\nB,1,123,Sprockets Black,30,50,1500\nB,2,333,Sprockets Blue,300,5,1500' SELECTOR 'B' COLUMNS x string, y integer, z string SELECTOR 'A' 2) x";

        List<?>[] expected = new List<?>[] {
                Arrays.asList("B", 1, "10-dec-2011"),
                Arrays.asList("B", 2, "10-dec-2011"),
        };

        process(sql, expected);
    }

    @Test public void testTextTableSelectorFixedWidth() throws Exception {
        String sql = "select x.* from texttable('A10-dec-20111234533223000222\nB112330501500\nB233330051500\nCX' SELECTOR 'B' COLUMNS x string width 1, y integer width 6, z integer width 6) x";

        List<?>[] expected = new List<?>[] {
                Arrays.asList("B", 112330, 501500),
                Arrays.asList("B", 233330,  51500),
        };

        process(sql, expected);
    }

    public static void process(String sql, List<?>[] expectedResults) throws Exception {
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        helpProcess(plan, createCommandContext(), dataManager, expectedResults);
    }

    public static ClobType clobFromFile(final String file) {
        return new ClobType(new ClobImpl(new InputStreamFactory.FileInputStreamFactory(UnitTestUtil.getTestDataFile(file)), -1));
    }

    @Test public void testTextAgg() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("select convert(to_chars(textagg(for pm1.g1.e1, pm1.g1.e2 header order by e2), 'UTF-8'), string) as x from pm1.g1", metadata,  null, capFinder, //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", new List<?>[] {Arrays.asList("z", 2), Arrays.asList("b", 1)});
        hdm.setBlockOnce(true);

        String nl = System.getProperty("line.separator");
        ArrayList<Object> list = new ArrayList<Object>();
        list.add("\"e1\",\"e2\""+nl+"\"b\",\"1\""+nl+"\"z\",\"2\""+nl);
        List<?>[] expected = new List<?>[] {
                list,
        };

        helpProcess(plan, hdm, expected);
    }

    @Test public void testTextAggBinary() throws Exception {
        TestValidator.helpValidate("select textagg(X'ab') from pm1.g1", new String[] {"TEXTAGG(FOR X'AB')"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testTextAggOrderByUnrelated() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("select convert(to_chars(textagg(for pm1.g1.e1 header order by e2), 'UTF-8'), string) as x from pm1.g1", metadata,  null, capFinder, //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", new List<?>[] {Arrays.asList("z", 2), Arrays.asList("b", 1)});
        hdm.setBlockOnce(true);

        String nl = System.getProperty("line.separator");
        ArrayList<Object> list = new ArrayList<Object>();
        list.add("\"e1\""+nl+"\"b\""+nl+"\"z\""+nl);
        List<?>[] expected = new List<?>[] {
                list,
        };

        helpProcess(plan, hdm, expected);
    }

    @Test public void testTextAggGroupBy() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        BasicSourceCapabilities caps = getTypicalCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("select convert(to_chars(textagg(pm1.g1.e1 order by pm1.g1.e1), 'UTF-8'), string) as x from pm1.g1 group by e2", metadata,  null, capFinder, //$NON-NLS-1$
            new String[] { "SELECT g_0.e2, g_0.e1 FROM pm1.g1 AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.e2, g_0.e1 FROM pm1.g1 AS g_0", new List<?>[] {
                Arrays.asList(2, "z"), Arrays.asList(1, "b"),
                Arrays.asList(2, "z"), Arrays.asList(1, "b"),
                Arrays.asList(2, "c"), Arrays.asList(2, "a")});
        hdm.setBlockOnce(true);

        String nl = System.getProperty("line.separator");
        ArrayList<Object> list = new ArrayList<Object>();
        list.add("\"b\""+nl+"\"b\""+nl);
        ArrayList<Object> list1 = new ArrayList<Object>();
        list1.add("\"a\""+nl+"\"c\""+nl+"\"z\""+nl+"\"z\""+nl);
        List<?>[] expected = new List<?>[] {
                list, list1
        };

        CommandContext context = createCommandContext();
        context.setBufferManager(BufferManagerFactory.getTestBufferManager(0, 2));
        helpProcess(plan, context, hdm, expected);
    }

    @Test(expected=TeiidProcessingException.class) public void testTextTableInvalidData() throws Exception {
        String sql = "select count(*) from texttable(? COLUMNS PARTNAME string) x"; //$NON-NLS-1$

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        char[] data = new char[5000];
        processPreparedStatement(sql, null, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(new ClobType(new SerialClob(data))));
    }

    @Test public void testTextTableInvalidData1() throws Exception {
        String sql = "select * from texttable(to_chars(X'610D810D', 'ascii') COLUMNS PARTNAME string) x"; //$NON-NLS-1$
        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {});
        try {
            helpProcess(plan, createCommandContext(), new HardcodedDataManager(), new List[] {Arrays.asList("a")});
            fail();
        } catch (TeiidProcessingException e) {
            assertEquals("TEIID10082 Error converting byte stream to characters using the US-ASCII charset at byte 3.", e.getCause().getMessage());
        }
    }

    @Test public void testTextTableFixedBestEffort() throws Exception {
        String sql = "select x.* from texttable('abc\nde\nfghi\n' COLUMNS x string width 1, y string width 1, z string width 1) x"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList("a", "b", "c"),
                Arrays.asList("d", "e", null), //too short, but still parsed
                Arrays.asList("f", "g", "h"),  //truncated
        };

        HardcodedDataManager dataManager = new HardcodedDataManager();
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testNoTrimDelimited() throws Exception {
        String sql = "select x.* from texttable('x, y\\u000A a , \"b\"' COLUMNS x string, \" y\" string HEADER NO TRIM) x"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList(" a ", "b"),
        };

        HardcodedDataManager dataManager = new HardcodedDataManager();
        Command cmd = helpParse(sql);
        assertEquals("SELECT x.* FROM TEXTTABLE('x, y\\u000A a , \"b\"' COLUMNS x string, \" y\" string HEADER NO TRIM) AS x", cmd.toString());
        ProcessorPlan plan = helpGetPlan(cmd, RealMetadataFactory.example1Cached());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testRowDelimiter() throws Exception {
        String sql = "select x.* from texttable('x-1, y\n a -2, \"b\"-3' COLUMNS x string, \"1\" string ROW DELIMITER ',' DELIMITER '-' HEADER) x"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList("y\n a", "2"),
                Arrays.asList("b", "3"),
        };

        HardcodedDataManager dataManager = new HardcodedDataManager();
        Command cmd = helpParse(sql);
        assertEquals("SELECT x.* FROM TEXTTABLE('x-1, y\\u000A a -2, \"b\"-3' COLUMNS x string, \"1\" string ROW DELIMITER ',' DELIMITER '-' HEADER) AS x", cmd.toString());
        ProcessorPlan plan = helpGetPlan(cmd, RealMetadataFactory.example1Cached());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testRowDelimiterValidation() throws Exception {
        String sql = "select x.* from texttable('x' COLUMNS x string ROW DELIMITER '-' DELIMITER '-' HEADER) x"; //$NON-NLS-1$

        TestValidator.helpValidate(sql, new String[]{"TEXTTABLE('x' COLUMNS x string ROW DELIMITER '-' DELIMITER '-' HEADER) AS x"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testDotHeader() throws Exception {
        String sql = "select x.x from texttable('h.1\na' COLUMNS x header 'h.1' string HEADER) x"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList("a"),
        };

        HardcodedDataManager dataManager = new HardcodedDataManager();
        Command cmd = helpParse(sql);
        assertEquals("SELECT x.x FROM TEXTTABLE('h.1\\u000Aa' COLUMNS x HEADER 'h.1' string HEADER) AS x", cmd.toString());
        ProcessorPlan plan = helpGetPlan(cmd, RealMetadataFactory.example1Cached());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testTextAggNull() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("select convert(to_chars(textagg(for pm1.g1.e1, pm1.g1.e2 header order by e2), 'UTF-8'), string) as x from pm1.g1", metadata,  null, capFinder, //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", new List<?>[] {Arrays.asList(null, 2), Arrays.asList("b", null)});
        hdm.setBlockOnce(true);

        String nl = System.getProperty("line.separator");
        ArrayList<Object> list = new ArrayList<Object>();
        list.add("\"e1\",\"e2\""+nl+"\"b\","+nl+",\"2\""+nl);
        List<?>[] expected = new List<?>[] {
                list,
        };

        helpProcess(plan, hdm, expected);
    }

    @Test public void testTextAggNoQuote() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        String sql = "select convert(to_chars(textagg(col, col1 no quote), 'UTF-8'), string) as x from (select '1' as col, 2 as col1 union all select 'abc', 3) as v";

        Command c = TestProcessor.helpParse(sql);
        assertEquals("SELECT convert(to_chars(TEXTAGG(FOR col, col1 NO QUOTE), 'UTF-8'), string) AS x FROM (SELECT '1' AS col, 2 AS col1 UNION ALL SELECT 'abc', 3) AS v", c.toString());

        ProcessorPlan plan = helpPlan(sql, metadata,  null, capFinder, //$NON-NLS-1$
            new String[] { }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        String nl = System.getProperty("line.separator");
        ArrayList<Object> list = new ArrayList<Object>();
        list.add("1,2"+nl+"abc,3"+nl);
        List<?>[] expected = new List<?>[] {
                list,
        };

        helpProcess(plan, new HardcodedDataManager(), expected);
    }

    @Test(expected=TeiidProcessingException.class) public void testTextAggNoQuoteException() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan("select convert(to_chars(textagg(col, col1 no quote), 'UTF-8'), string) as x from (select ',1' as col, 2 as col1 union all select 'abc', 3) as v", metadata,  null, capFinder, //$NON-NLS-1$
            new String[] { }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        helpProcess(plan, TestProcessor.createCommandContext(), new HardcodedDataManager(), null);
    }

    @Test public void testUTF8Bom() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        List<?>[] expected = new List[] {
                Arrays.asList("\u00EF\u00BB\u00BFx"),
                Arrays.asList("y"),
        };

        String sql = "select x.* from texttable(to_chars(to_bytes('\u00EF\u00BB\u00BFx, y', 'UTF_8_BOM'), 'utf-8-bom') COLUMNS x string ROW DELIMITER ',' DELIMITER '-') x"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, metadata,  null, capFinder, //$NON-NLS-1$
            new String[] { }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        helpProcess(plan, TestProcessor.createCommandContext(), new HardcodedDataManager(), expected);
    }

    @Test public void testEscapedRowDelimiter() throws Exception {
        String sql = "select * from TextTable (\n" +
                "    'greetings, \"hello, world,,\"'\n" +
                "    Columns \n" +
                "        x string\n" +
                "    Row Delimiter ','\n" +
                "    Delimiter ';'\n" +
                "    Quote '\"'\n" +
                ")x "; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("greetings"),
                Arrays.asList("hello, world,,"),
        };

        process(sql, expected);
    }

}
