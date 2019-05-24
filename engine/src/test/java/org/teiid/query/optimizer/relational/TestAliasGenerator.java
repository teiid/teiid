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

package org.teiid.query.optimizer.relational;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.dqp.internal.datamgr.LanguageBridgeFactory;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.TestResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestAliasGenerator {

    private Command helpTest(String sql,
                          String expected,
                          boolean aliasGroups,
                          boolean stripColumnAliases, QueryMetadataInterface metadata) throws TeiidComponentException, TeiidProcessingException {
        AliasGenerator visitor = new AliasGenerator(aliasGroups, stripColumnAliases);
        return helpTest(sql, expected, metadata, visitor);
    }

    private Command helpTest(String sql, String expected,
            QueryMetadataInterface metadata, AliasGenerator visitor)
            throws TeiidComponentException, TeiidProcessingException {
        Command command = TestResolver.helpResolve(sql, metadata);
        command = QueryRewriter.rewrite(command, metadata, null);
        command = (Command) command.clone();
        command.acceptVisitor(visitor);
        assertEquals(expected, command.toString());
        return command;
    }

    /**
     * Ensures that views are named with v_ even without metadata
     */
    @Test public void testViewAliasing() throws Exception {
        String sql = "select y.e1 from (select pm1.g1.e1 from pm1.g1) y"; //$NON-NLS-1$
        Query command = (Query)QueryParser.getQueryParser().parseCommand(sql);
        ((ElementSymbol)command.getSelect().getSymbol(0)).setGroupSymbol(new GroupSymbol("y")); //$NON-NLS-1$
        command.acceptVisitor(new AliasGenerator(true));
        assertEquals("SELECT v_0.c_0 FROM (SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0) AS v_0", command.toString()); //$NON-NLS-1$
    }

    @Test public void testNestedViewAliasing() throws Exception {
        String sql = "select e1, e2 from (select y.e1, y.e2 from (select pm1.g1.e1, 1 as e2 from pm1.g1) y) z"; //$NON-NLS-1$
        Query command = (Query)QueryParser.getQueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
        command = (Query) command.clone();
        command.acceptVisitor(new AliasGenerator(true));
        assertEquals("SELECT v_1.c_0, v_1.c_1 FROM (SELECT v_0.c_0, v_0.c_1 FROM (SELECT g_0.e1 AS c_0, 1 AS c_1 FROM pm1.g1 AS g_0) AS v_0) AS v_1", command.toString()); //$NON-NLS-1$
    }

    @Test public void testLongOrderByAlias() throws Exception {
        String sql = "select pm1.g1.e1 || pm1.g1.e2 as asfasdfadfasdfasdfadfasdfadsfasdfasdfasdfasdfasdfadfa, pm1.g1.e2 from pm1.g1 order by asfasdfadfasdfasdfadfasdfadsfasdfasdfasdfasdfasdfadfa"; //$NON-NLS-1$
        String expected = "SELECT concat(g_0.e1, convert(g_0.e2, string)) AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0"; //$NON-NLS-1$
        helpTest(sql, expected, true, false, RealMetadataFactory.example1Cached());
    }

    @Test public void testOrderBySymbolName() throws Exception {
        String sql = "select e1 from pm1.g1 order by e1"; //$NON-NLS-1$
        String expected = "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0"; //$NON-NLS-1$
        Query command = (Query)helpTest(sql, expected, true, false, RealMetadataFactory.example1Cached());
        assertEquals(((Symbol)command.getOrderBy().getSortKeys().get(0)).getName(), "c_0"); //$NON-NLS-1$
        assertEquals(((Symbol)command.getProjectedSymbols().get(0)).getShortName(), "c_0"); //$NON-NLS-1$
    }

    @Test public void testInlineViewWithSubQuery() throws Exception {
        String sql = "select intnum from (select intnum from bqt1.smallb where intnum in (select intnum a from bqt1.smalla)) b"; //$NON-NLS-1$
        String expected = "SELECT v_0.c_0 FROM (SELECT g_0.IntNum AS c_0 FROM BQT1.SmallB AS g_0 WHERE g_0.IntNum IN (SELECT g_1.IntNum FROM BQT1.SmallA AS g_1)) AS v_0"; //$NON-NLS-1$
        helpTest(sql, expected, true, false, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testInlineViewOrderBy() throws Exception {
        String sql = "select intnum from (select intnum from bqt1.smallb) b order by b.intnum"; //$NON-NLS-1$
        String expected = "SELECT v_0.c_0 FROM (SELECT g_0.IntNum AS c_0 FROM BQT1.SmallB AS g_0) AS v_0 ORDER BY c_0"; //$NON-NLS-1$
        Command command = helpTest(sql, expected, true, false, RealMetadataFactory.exampleBQTCached());
        LanguageBridgeFactory lbf = new LanguageBridgeFactory(RealMetadataFactory.exampleBQTCached());
        org.teiid.language.Command c = lbf.translate(command);
        assertEquals("SELECT v_0.c_0 FROM (SELECT g_0.IntNum AS c_0 FROM SmallB AS g_0) AS v_0 ORDER BY v_0.c_0", c.toString());
    }

    @Test public void testNestedInlineViewOrderBy() throws Exception {
        String sql = "select x from (select intnum x from (select intnum from bqt1.smallb) b order by x) y order by x"; //$NON-NLS-1$
        String expected = "SELECT v_1.c_0 FROM (SELECT v_0.c_0 FROM (SELECT g_0.IntNum AS c_0 FROM BQT1.SmallB AS g_0) AS v_0) AS v_1 ORDER BY c_0"; //$NON-NLS-1$
        helpTest(sql, expected, true, false, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testInlineViewWithOnClause() throws Exception {
        String sql = "select abcd.efg from (select intkey as efg from bqt1.smalla) abcd inner join (select intnum from bqt1.smallb) b on (b.intnum = abcd.efg)"; //$NON-NLS-1$
        String expected = "SELECT v_0.c_0 FROM (SELECT g_0.IntKey AS c_0 FROM BQT1.SmallA AS g_0) AS v_0 INNER JOIN (SELECT g_1.IntNum AS c_0 FROM BQT1.SmallB AS g_1) AS v_1 ON v_1.c_0 = v_0.c_0"; //$NON-NLS-1$
        helpTest(sql, expected, true, false, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testUnionOrderBy() throws Exception {
        String sql = "select e1, e2 as c_0 from pm1.g1 union all select 1, e1 from pm1.g2 order by e1"; //$NON-NLS-1$
        String expected = "SELECT g_1.e1 AS c_0, convert(g_1.e2, string) AS c_1 FROM pm1.g1 AS g_1 UNION ALL SELECT '1' AS c_0, g_0.e1 AS c_1 FROM pm1.g2 AS g_0 ORDER BY c_0"; //$NON-NLS-1$
        helpTest(sql, expected, true, false, RealMetadataFactory.example1Cached());
    }

    @Test public void testDuplicateShortElementName() throws Exception {
        String sql = "select pm1.g1.e1, pm1.g2.e1 from pm1.g1, pm1.g2 order by pm1.g1.e1, pm1.g2.e1"; //$NON-NLS-1$
        String expected = "SELECT g_0.e1 AS c_0, g_1.e1 AS c_1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 ORDER BY c_0, c_1"; //$NON-NLS-1$
        helpTest(sql, expected, true, false, RealMetadataFactory.example1Cached());
    }

    @Test public void testCorrelatedRefernce() throws Exception {
        String sql = "select intnum, stringnum from (select intnum, stringnum from bqt1.smallb) b where intnum in (select b.stringnum || b.intnum from (select intnum from bqt1.smalla) b) "; //$NON-NLS-1$
        String expected = "SELECT v_0.c_0, v_0.c_1 FROM (SELECT g_0.IntNum AS c_0, g_0.StringNum AS c_1 FROM BQT1.SmallB AS g_0) AS v_0 WHERE convert(v_0.c_0, string) IN (SELECT concat(v_0.c_1, convert(v_1.c_0, string)) FROM (SELECT g_1.IntNum AS c_0 FROM BQT1.SmallA AS g_1) AS v_1)"; //$NON-NLS-1$
        helpTest(sql, expected, true, false, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testCorrelatedRefernce1() throws Exception {
        String sql = "select intnum, stringnum from bqt1.smallb where intnum in (select stringnum || b.intnum from (select intnum from bqt1.smalla) b) "; //$NON-NLS-1$
        String expected = "SELECT g_0.IntNum, g_0.StringNum FROM BQT1.SmallB AS g_0 WHERE convert(g_0.IntNum, string) IN (SELECT concat(g_0.StringNum, convert(v_0.c_0, string)) FROM (SELECT g_1.IntNum AS c_0 FROM BQT1.SmallA AS g_1) AS v_0)"; //$NON-NLS-1$
        helpTest(sql, expected, true, false, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testGroupAliasNotSupported() throws Exception {
        String sql = "select b.intkey from bqt1.smalla b"; //$NON-NLS-1$
        String expected = "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA"; //$NON-NLS-1$
        helpTest(sql, expected, false, false, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testUnionAliasing() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT1.SmallA"; //$NON-NLS-1$
        String expected = "SELECT BQT1.SmallA.IntKey AS c_0 FROM BQT1.SmallA UNION ALL SELECT BQT1.SmallA.IntNum AS c_0 FROM BQT1.SmallA"; //$NON-NLS-1$
        helpTest(sql, expected, false, false, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testUnrelatedOrderBy() throws Exception {
        String sql = "SELECT b.IntKey FROM BQT1.SmallA a, BQT1.SmallA b ORDER BY a.StringKey"; //$NON-NLS-1$
        String expected = "SELECT g_1.IntKey AS c_0 FROM BQT1.SmallA AS g_0, BQT1.SmallA AS g_1 ORDER BY g_0.StringKey"; //$NON-NLS-1$
        helpTest(sql, expected, true, false, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testUnrelatedOrderBy1() throws Exception {
        String sql = "SELECT b.IntKey FROM (select intkey, stringkey from BQT1.SmallA) a, (select intkey, stringkey from BQT1.SmallA) b ORDER BY a.StringKey"; //$NON-NLS-1$
        String expected = "SELECT v_1.c_0 FROM (SELECT g_0.IntKey AS c_0, g_0.StringKey AS c_1 FROM BQT1.SmallA AS g_0) AS v_0, (SELECT g_1.IntKey AS c_0, g_1.StringKey AS c_1 FROM BQT1.SmallA AS g_1) AS v_1 ORDER BY v_0.c_1"; //$NON-NLS-1$
        helpTest(sql, expected, true, false, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testUnrelatedOrderBy2() throws Exception {
        String sql = "SELECT b.IntKey FROM (select intkey, stringkey from BQT1.SmallA) a, (select intkey, stringkey from BQT1.SmallA) b ORDER BY a.StringKey || b.intKey"; //$NON-NLS-1$
        String expected = "SELECT v_1.c_0 FROM (SELECT g_0.IntKey AS c_0, g_0.StringKey AS c_1 FROM BQT1.SmallA AS g_0) AS v_0, (SELECT g_1.IntKey AS c_0, g_1.StringKey AS c_1 FROM BQT1.SmallA AS g_1) AS v_1 ORDER BY concat(v_0.c_1, convert(v_1.c_0, string))"; //$NON-NLS-1$
        helpTest(sql, expected, true, false, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testStripAliases() throws Exception {
        String sql = "select intkey as a, stringkey as b from BQT1.SmallA ORDER BY a, b"; //$NON-NLS-1$
        String expected = "SELECT g_0.IntKey, g_0.StringKey FROM BQT1.SmallA AS g_0 ORDER BY g_0.IntKey, g_0.StringKey"; //$NON-NLS-1$
        helpTest(sql, expected, true, true, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testStripAliases1() throws Exception {
        String sql = "select intkey as a, stringkey as b from BQT1.SmallA ORDER BY a, b"; //$NON-NLS-1$
        String expected = "SELECT BQT1.SmallA.IntKey, BQT1.SmallA.StringKey FROM BQT1.SmallA ORDER BY BQT1.SmallA.IntKey, BQT1.SmallA.StringKey"; //$NON-NLS-1$
        Command command = helpTest(sql, expected, false, true, RealMetadataFactory.exampleBQTCached());
        LanguageBridgeFactory lbf = new LanguageBridgeFactory(RealMetadataFactory.exampleBQTCached());
        org.teiid.language.Command c = lbf.translate(command);
        assertEquals("SELECT SmallA.IntKey, SmallA.StringKey FROM SmallA ORDER BY SmallA.IntKey, SmallA.StringKey", c.toString());
    }

    @Test public void testKeepAliases() throws Exception {
        String sql = "select g.intkey as a, g.stringkey as b from BQT1.SmallA g, BQT1.SmallB ORDER BY a, b"; //$NON-NLS-1$
        String expected = "SELECT g.IntKey AS c_0, g.StringKey AS c_1 FROM BQT1.SmallA AS g, BQT1.SmallB AS g_1 ORDER BY c_0, c_1"; //$NON-NLS-1$
        AliasGenerator av = new AliasGenerator(true, false);
        Map<String, String> aliasMap = new HashMap<String, String>();
        aliasMap.put("g", "g");
        av.setAliasMapping(aliasMap);
        helpTest(sql, expected, RealMetadataFactory.exampleBQTCached(), av);
    }

    @Test(expected=TeiidRuntimeException.class) public void testKeepAliases1() throws Exception {
        String sql = "select g_1.intkey as a, g_1.stringkey as b from BQT1.SmallA g_1, BQT1.SmallB ORDER BY a, b"; //$NON-NLS-1$
        String expected = "SELECT g.IntKey AS c_0, g.StringKey AS c_1 FROM BQT1.SmallA AS g ORDER BY c_0, c_1"; //$NON-NLS-1$
        AliasGenerator av = new AliasGenerator(true, false);
        Map<String, String> aliasMap = new HashMap<String, String>();
        aliasMap.put("g_1", "g_1");
        av.setAliasMapping(aliasMap);
        helpTest(sql, expected, RealMetadataFactory.exampleBQTCached(), av);
    }

    @Test public void testNestedCommonTables() throws Exception {
        String sql = "WITH CTE0 (e1, e2) AS /*+ no_inline */ (SELECT e1, e2 FROM pm1.g2) SELECT g_0.e1, g_0.e2 FROM CTE0 AS g_0 WHERE g_0.e1 = (WITH CTE1 (e1) AS /*+ no_inline */ (SELECT g_0.e1 FROM pm1.g1 AS g_1) SELECT g_2.e1 FROM CTE1 AS g_2)";
        String expected = "WITH CTE0 (e1, e2) AS /*+ no_inline */ (SELECT g_0.e1, g_0.e2 FROM pm1.g2 AS g_0) SELECT g_1.e1, g_1.e2 FROM CTE0 AS g_1 WHERE g_1.e1 = (WITH CTE1 (e1) AS /*+ no_inline */ (SELECT g_1.e1 FROM pm1.g1 AS g_2) SELECT g_3.e1 AS c_0 FROM CTE1 AS g_3 LIMIT 2)"; //$NON-NLS-1$
        AliasGenerator av = new AliasGenerator(true, false);
        helpTest(sql, expected, RealMetadataFactory.example1Cached(), av);
    }

}
