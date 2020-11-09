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

package org.teiid.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.query.sql.lang.AbstractCompareCriteria;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Option;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.translator.CacheDirective.Scope;

@SuppressWarnings("nls")
public class TestOptionsAndHints {

    /*+* Select a From db.g1 MAKENOTDEP, db.g2 MAKENOTDEP WHERE a = b */
    @Test public void testOptionMakeNotDepInline4(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("c", "db.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$

        CompareCriteria crit = new CompareCriteria(a, CompareCriteria.EQ, b);

        From from = new From();
        FromClause clause = new UnaryFromClause(g1);
        clause.setMakeNotDep(true);
        from.addClause(clause);
        FromClause clause1 = new UnaryFromClause(g2);
        clause1.setMakeNotDep(true);
        from.addClause(clause1);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        TestParser.helpTest("Select a From db.g1 MAKENOTDEP, db.g2 AS c MAKENOTDEP WHERE a = b",  //$NON-NLS-1$
                 "SELECT a FROM /*+ MAKENOTDEP */ db.g1, /*+ MAKENOTDEP */ db.g2 AS c WHERE a = b",  //$NON-NLS-1$
                 query);
    }

    /*+* Select a From db.g1 JOIN db.g2 MAKEDEP ON a = b */
    @Test public void testOptionMakeDepInline1(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$

        List<Object> crits = new ArrayList<Object>();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.getRightClause().setMakeDep(true);
        From from = new From();
        from.addClause(jp);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From db.g1 JOIN db.g2 MAKEDEP ON a = b",  //$NON-NLS-1$
                 "SELECT a FROM db.g1 INNER JOIN /*+ MAKEDEP */ db.g2 ON a = b",  //$NON-NLS-1$
                 query);
    }

    /*+* Select a From db.g1 MAKEDEP JOIN db.g2 ON a = b */
    @Test public void testOptionMakeDepInline2(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$

        List<Object> crits = new ArrayList<Object>();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.getLeftClause().setMakeDep(true);
        From from = new From();
        from.addClause(jp);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From db.g1 MAKEDEP JOIN db.g2 ON a = b",  //$NON-NLS-1$
                 "SELECT a FROM /*+ MAKEDEP */ db.g1 INNER JOIN db.g2 ON a = b",  //$NON-NLS-1$
                 query);
    }

    /*+* Select a From (db.g1 MAKEDEP JOIN db.g2 ON a = b) LEFT OUTER JOIN db.g3 MAKEDEP ON a = c */
    @Test public void testOptionMakeDepInline3(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        GroupSymbol g3 = new GroupSymbol("db.g3"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$
        ElementSymbol c = new ElementSymbol("c");  //$NON-NLS-1$

        List<Object> crits = new ArrayList<Object>();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.getLeftClause().setMakeDep(true);
        List<Object> crits2 = new ArrayList<Object>();
        crits2.add(new CompareCriteria(a, CompareCriteria.EQ, c));
        JoinPredicate jp2 = new JoinPredicate(jp, new UnaryFromClause(g3), JoinType.JOIN_LEFT_OUTER, crits2);
        jp2.getRightClause().setMakeDep(true);
        From from = new From();
        from.addClause(jp2);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From (db.g1 MAKEDEP JOIN db.g2 ON a = b) LEFT OUTER JOIN db.g3 MAKEDEP ON a = c",  //$NON-NLS-1$
                 "SELECT a FROM (/*+ MAKEDEP */ db.g1 INNER JOIN db.g2 ON a = b) LEFT OUTER JOIN /*+ MAKEDEP */ db.g3 ON a = c",  //$NON-NLS-1$
                 query);
    }

    /*+* Select a From db.g1 MAKEDEP, db.g2 MAKEDEP WHERE a = b */
    @Test public void testOptionMakeDepInline4(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("c", "db.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$

        CompareCriteria crit = new CompareCriteria(a, CompareCriteria.EQ, b);

        From from = new From();
        FromClause clause = new UnaryFromClause(g1);
        clause.setMakeDep(true);
        from.addClause(clause);
        FromClause clause1 = new UnaryFromClause(g2);
        clause1.setMakeDep(true);
        from.addClause(clause1);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        TestParser.helpTest("Select a From db.g1 MAKEDEP, db.g2 AS c MAKEDEP WHERE a = b",  //$NON-NLS-1$
                 "SELECT a FROM /*+ MAKEDEP */ db.g1, /*+ MAKEDEP */ db.g2 AS c WHERE a = b",  //$NON-NLS-1$
                 query);
    }

    @Test public void testOptionMakedep() throws Exception {
        String sql = "SELECT A.alert_id " + //$NON-NLS-1$
            "FROM (FSK_ALERT AS A MAKEDEP INNER JOIN Core.FSC_PARTY_DIM AS C ON A.primary_entity_key = C.PARTY_KEY) " +//$NON-NLS-1$
            "LEFT OUTER JOIN FSK_SCENARIO AS S ON A.scenario_id = S.scenario_id ";//$NON-NLS-1$
        Query command = (Query)new QueryParser().parseCommand(sql);
        JoinPredicate predicate = (JoinPredicate)command.getFrom().getClauses().get(0);
        assertTrue(((JoinPredicate)predicate.getLeftClause()).getLeftClause().isMakeDep());
    }

    /*+* Select a From db.g1 JOIN db.g2 MAKENOTDEP ON a = b */
    @Test public void testOptionMakeNotDepInline1(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$

        List<Object> crits = new ArrayList<Object>();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.getRightClause().setMakeNotDep(true);
        From from = new From();
        from.addClause(jp);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From db.g1 JOIN db.g2 MAKENOTDEP ON a = b",  //$NON-NLS-1$
                 "SELECT a FROM db.g1 INNER JOIN /*+ MAKENOTDEP */ db.g2 ON a = b",  //$NON-NLS-1$
                 query);
    }

    /*+* Select a From db.g1 MAKENOTDEP JOIN db.g2 ON a = b */
    @Test public void testOptionMakeNotDepInline2(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$

        List<Object> crits = new ArrayList<Object>();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.getLeftClause().setMakeNotDep(true);
        From from = new From();
        from.addClause(jp);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From db.g1 MAKENOTDEP JOIN db.g2 ON a = b",  //$NON-NLS-1$
                 "SELECT a FROM /*+ MAKENOTDEP */ db.g1 INNER JOIN db.g2 ON a = b",  //$NON-NLS-1$
                 query);
    }

    /*+* Select a From (db.g1 MAKENOTDEP JOIN db.g2 ON a = b) LEFT OUTER JOIN db.g3 MAKENOTDEP ON a = c */
    @Test public void testOptionMakeNotDepInline3(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        GroupSymbol g3 = new GroupSymbol("db.g3"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$
        ElementSymbol c = new ElementSymbol("c");  //$NON-NLS-1$

        List<Object> crits = new ArrayList<Object>();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.getLeftClause().setMakeNotDep(true);
        List<Object> crits2 = new ArrayList<Object>();
        crits2.add(new CompareCriteria(a, CompareCriteria.EQ, c));
        JoinPredicate jp2 = new JoinPredicate(jp, new UnaryFromClause(g3), JoinType.JOIN_LEFT_OUTER, crits2);
        jp2.getRightClause().setMakeNotDep(true);
        From from = new From();
        from.addClause(jp2);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From (db.g1 MAKENOTDEP JOIN db.g2 ON a = b) LEFT OUTER JOIN db.g3 MAKENOTDEP ON a = c",  //$NON-NLS-1$
                 "SELECT a FROM (/*+ MAKENOTDEP */ db.g1 INNER JOIN db.g2 ON a = b) LEFT OUTER JOIN /*+ MAKENOTDEP */ db.g3 ON a = c",  //$NON-NLS-1$
                 query);
    }

    @Test public void testDepOptions2() {
        GroupSymbol a = new GroupSymbol("a"); //$NON-NLS-1$
        GroupSymbol b = new GroupSymbol("b"); //$NON-NLS-1$
        ElementSymbol x = new ElementSymbol("a.x", true); //$NON-NLS-1$
        ElementSymbol y = new ElementSymbol("b.y", true); //$NON-NLS-1$

        Criteria criteria = new CompareCriteria(x, CompareCriteria.EQ, new Function("func", new Expression[] {y})); //$NON-NLS-1$
        JoinPredicate predicate = new JoinPredicate(new UnaryFromClause(a), new UnaryFromClause(b), JoinType.JOIN_INNER, Arrays.asList(new Object[] {criteria}));
        From from = new From(Arrays.asList(predicate));
        predicate.getLeftClause().setMakeNotDep(true);
        predicate.getRightClause().setMakeDep(true);
        Select select = new Select(Arrays.asList(x, y));

        Query query = new Query(select, from, null, null, null, null, null);
        TestParser.helpTest("Select a.x, b.y From a MAKENOTDEP INNER JOIN b MAKEDEP ON a.x = func(b.y)",  //$NON-NLS-1$
                 "SELECT a.x, b.y FROM /*+ MAKENOTDEP */ a INNER JOIN /*+ MAKEDEP */ b ON a.x = func(b.y)",  //$NON-NLS-1$
                 query);
    }

    @Test public void testOptionNoCache1(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.setNoCache(true);
        option.addNoCacheGroup("a.b.c"); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option nocache a.b.c",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION NOCACHE a.b.c",  //$NON-NLS-1$
                 query);
    }

    @Test public void testOptionNoCache2(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.setNoCache(true);
        option.addNoCacheGroup("a.b.c"); //$NON-NLS-1$
        option.addNoCacheGroup("d.e.f"); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option nocache a.b.c, d.e.f",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION NOCACHE a.b.c, d.e.f",  //$NON-NLS-1$
                 query);
    }

//  related to defect 14423
    @Test public void testOptionNoCache3(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.setNoCache(true);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option nocache",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION NOCACHE",  //$NON-NLS-1$
                 query);
    }

    /*+* SELECT a from g OPTION xyx */
    @Test public void testFailsIllegalOption(){
        TestParser.helpException("SELECT a from g OPTION xyx");         //$NON-NLS-1$
    }

    @Test public void testInsertWithOption() {
        Insert insert = new Insert();
        insert.setGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        List<ElementSymbol> vars = new ArrayList<ElementSymbol>();
        vars.add(new ElementSymbol("a"));         //$NON-NLS-1$
        insert.setVariables(vars);
        List<Reference> values = new ArrayList<Reference>();
        values.add(new Reference(0));
        insert.setValues(values);
        Option option = new Option();
        option.setNoCache(true);
        insert.setOption(option);
        TestParser.helpTest("INSERT INTO m.g (a) VALUES (?) OPTION NOCACHE",  //$NON-NLS-1$
                 "INSERT INTO m.g (a) VALUES (?) OPTION NOCACHE",  //$NON-NLS-1$
                 insert);
    }

    @Test public void testDeleteWithOption() {
        Delete delete = new Delete();
        delete.setGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        Option option = new Option();
        option.setNoCache(true);
        delete.setOption(option);
        TestParser.helpTest("DELETE FROM m.g OPTION NOCACHE",  //$NON-NLS-1$
                 "DELETE FROM m.g OPTION NOCACHE",  //$NON-NLS-1$
                 delete);
    }

    @Test public void testUpdateWithOption() {
        Update update = new Update();
        update.setGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        update.addChange(new ElementSymbol("a"), new Reference(0));
        Option option = new Option();
        option.setNoCache(true);
        Criteria crit = new CompareCriteria(new ElementSymbol("b"), CompareCriteria.EQ, new Reference(1)); //$NON-NLS-1$
        update.setCriteria(crit);
        update.setOption(option);
        TestParser.helpTest("UPDATE m.g SET a = ? WHERE b = ? OPTION NOCACHE",  //$NON-NLS-1$
                 "UPDATE m.g SET a = ? WHERE b = ? OPTION NOCACHE",  //$NON-NLS-1$
                 update);
    }

    @Test public void testOptionalFromClause1() {
        String sql = "SELECT * FROM /*+ optional */ t1, t2"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc);
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT * FROM /*+ optional */ t1, t2", query);         //$NON-NLS-1$
    }

    @Test public void testOptionalFromClause1_1() {
        String sql = "SELECT * FROM /*+ optional*/ t1, t2"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc);
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT * FROM /*+ optional */ t1, t2", query);         //$NON-NLS-1$
    }

    @Test public void testOptionalFromClause1_2() {
        String sql = "SELECT * FROM /*+optional */ t1, t2"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc);
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT * FROM /*+ optional */ t1, t2", query);         //$NON-NLS-1$
    }

    @Test public void testOptionalFromClause1_3() {
        String sql = "SELECT * FROM /*+ optional  */ t1, t2"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc);
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT * FROM /*+ optional */ t1, t2", query);         //$NON-NLS-1$
    }

    @Test public void testOptionalFromClause1_4() {
        String sql = "SELECT * /*+ optional */ FROM /*+ OptiOnal  */ t1, t2"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc);
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT * FROM /*+ optional */ t1, t2", query);         //$NON-NLS-1$
    }

    @Test public void testOptionalFromClause1_5() {
        String sql = "SELECT * FROM /*+ OptiOnal  */ t1, t2"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc);
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT * FROM /*+ optional */ t1, t2", query);         //$NON-NLS-1$
    }

    @Test public void testOptionalFromClause2() {
        String sql = "SELECT * FROM t1, /*+ optional */ t2"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc);
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT * FROM t1, /*+ optional */ t2", query);         //$NON-NLS-1$
    }

    @Test public void testOptionalFromClause3() {
        String sql = "SELECT * FROM /*+ optional */ t1 AS a, t2"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("a", "t1")); //$NON-NLS-1$ //$NON-NLS-2$
        ufc.setOptional(true);
        from.addClause(ufc);
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT * FROM /*+ optional */ t1 AS a, t2", query);         //$NON-NLS-1$
    }

    @Test public void testOptionalFromClause4() {
        String sql = "SELECT * FROM t1, /*+ optional */ t2 as a"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("a", "t2")); //$NON-NLS-1$ //$NON-NLS-2$
        ufc.setOptional(true);
        from.addClause(ufc);
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT * FROM t1, /*+ optional */ t2 AS a", query);         //$NON-NLS-1$
    }

    @Test public void testOptionalFromClause5() {
        String sql = "SELECT * FROM t1, /*+ optional */ (select * from t1, t2) as x"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();

        Query query2 = new Query();
        select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query2.setSelect(select);
        From from2 = new From();
        from2.addGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        from2.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query2.setFrom(from2);

        SubqueryFromClause sfc = new SubqueryFromClause("x", query2);//$NON-NLS-1$
        sfc.setOptional(true);
        from.addGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        from.addClause(sfc);
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT * FROM t1, /*+ optional */ (SELECT * FROM t1, t2) AS x", query);         //$NON-NLS-1$
    }

    @Test public void testOptionalFromClause6() {
        String sql = "SELECT * FROM t1 INNER JOIN /*+ optional */ (select a from t1, t2) AS x ON t1.a=x.a"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();

        Query query2 = new Query();
        select = new Select();
        select.addSymbol(new ElementSymbol("a"));//$NON-NLS-1$
        From from2 = new From();
        from2.addGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        from2.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query2.setSelect(select);
        query2.setFrom(from2);

        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$

        SubqueryFromClause sfc = new SubqueryFromClause("x", query2);//$NON-NLS-1$
        sfc.setOptional(true);

        List<Object> criteria = new ArrayList<Object>();
        criteria.add(new CompareCriteria(new ElementSymbol("t1.a"), AbstractCompareCriteria.EQ, new ElementSymbol("x.a")));//$NON-NLS-1$//$NON-NLS-2$
        JoinPredicate joinPredicate = new JoinPredicate(ufc, sfc, JoinType.JOIN_INNER, criteria);
        from.addClause(joinPredicate);
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT * FROM t1 INNER JOIN /*+ optional */ (SELECT a FROM t1, t2) AS x ON t1.a = x.a", query);         //$NON-NLS-1$
    }

    @Test public void testOptionalFromClause7() {
        String sql = "SELECT b FROM t1, /*+ optional */ (t2 INNER JOIN t3 ON t2.a = t3.a)"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("b"));//$NON-NLS-1$
        query.setSelect(select);
        From from = new From();

        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t2")); //$NON-NLS-1$

        UnaryFromClause ufc2 = new UnaryFromClause();
        ufc2.setGroup(new GroupSymbol("t3")); //$NON-NLS-1$

        List<Object> criteria = new ArrayList<Object>();
        criteria.add(new CompareCriteria(new ElementSymbol("t2.a"), AbstractCompareCriteria.EQ, new ElementSymbol("t3.a")));//$NON-NLS-1$//$NON-NLS-2$
        JoinPredicate joinPredicate = new JoinPredicate(ufc, ufc2, JoinType.JOIN_INNER, criteria);
        joinPredicate.setOptional(true);

        UnaryFromClause ufc3 = new UnaryFromClause();
        ufc3.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        from.addClause(ufc3);
        from.addClause(joinPredicate);
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT b FROM t1, /*+ optional */ (t2 INNER JOIN t3 ON t2.a = t3.a)", query);         //$NON-NLS-1$
    }

    @Test public void testOptionalFromClause8() {
        String sql = "SELECT b FROM t1, /*+ optional */ (/*+ optional */ (SELECT * FROM t1, t2) AS x INNER JOIN t3 ON x.a = t3.a)"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("b"));//$NON-NLS-1$
        query.setSelect(select);
        From from = new From();


        Query query2 = new Query();
        select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        From from2 = new From();
        from2.addGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        from2.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query2.setSelect(select);
        query2.setFrom(from2);
        SubqueryFromClause sfc = new SubqueryFromClause("x", query2);//$NON-NLS-1$
        sfc.setOptional(true);

        UnaryFromClause ufc2 = new UnaryFromClause();
        ufc2.setGroup(new GroupSymbol("t3")); //$NON-NLS-1$

        List<Object> criteria = new ArrayList<Object>();
        criteria.add(new CompareCriteria(new ElementSymbol("x.a"), AbstractCompareCriteria.EQ, new ElementSymbol("t3.a")));//$NON-NLS-1$//$NON-NLS-2$
        JoinPredicate joinPredicate = new JoinPredicate(sfc, ufc2, JoinType.JOIN_INNER, criteria);
        joinPredicate.setOptional(true);

        UnaryFromClause ufc3 = new UnaryFromClause();
        ufc3.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        from.addClause(ufc3);
        from.addClause(joinPredicate);
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT b FROM t1, /*+ optional */ (/*+ optional */ (SELECT * FROM t1, t2) AS x INNER JOIN t3 ON x.a = t3.a)", query);         //$NON-NLS-1$
    }

    @Test public void testOptionalFromClause9() {
        String sql = "SELECT b FROM (t1 LEFT OUTER JOIN /*+ optional */t2 on t1.a = t2.a) LEFT OUTER JOIN /*+ optional */t3 on t1.a = t3.a"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("b"));//$NON-NLS-1$
        query.setSelect(select);
        From from = new From();

        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$

        UnaryFromClause ufc2 = new UnaryFromClause();
        ufc2.setGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        ufc2.setOptional(true);

        List<Object> criteria = new ArrayList<Object>();
        criteria.add(new CompareCriteria(new ElementSymbol("t1.a"), AbstractCompareCriteria.EQ, new ElementSymbol("t2.a")));//$NON-NLS-1$//$NON-NLS-2$
        JoinPredicate joinPredicate = new JoinPredicate(ufc, ufc2, JoinType.JOIN_LEFT_OUTER, criteria);

        UnaryFromClause ufc3 = new UnaryFromClause();
        ufc3.setGroup(new GroupSymbol("t3")); //$NON-NLS-1$
        ufc3.setOptional(true);

        criteria = new ArrayList<Object>();
        criteria.add(new CompareCriteria(new ElementSymbol("t1.a"), AbstractCompareCriteria.EQ, new ElementSymbol("t3.a")));//$NON-NLS-1$//$NON-NLS-2$
        JoinPredicate joinPredicate2 = new JoinPredicate(joinPredicate, ufc3, JoinType.JOIN_LEFT_OUTER, criteria);

        from.addClause(joinPredicate2);
        query.setFrom(from);

        TestParser.helpTest(sql, "SELECT b FROM (t1 LEFT OUTER JOIN /*+ optional */ t2 ON t1.a = t2.a) LEFT OUTER JOIN /*+ optional */ t3 ON t1.a = t3.a", query);         //$NON-NLS-1$
    }

    @Test public void testStoredQueryWithOption(){
        StoredProcedure storedQuery = new StoredProcedure();
        storedQuery.setProcedureName("proc1"); //$NON-NLS-1$
        Option option = new Option();
        option.setNoCache(true);
        storedQuery.setOption(option);
        TestParser.helpTest("exec proc1() option nocache", "EXEC proc1() OPTION NOCACHE", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /*+* Select a From db.g Option SHOWPLAN */
    /*+* Select a From db.g Option makedep a.b.c */
    @Test public void testOptionMakeDependent1(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.addDependentGroup("a.b.c"); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option makedep a.b.c",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION MAKEDEP a.b.c",  //$NON-NLS-1$
                 query);
    }

    /*+* Select a From db.g Option makedep a.b.c, d.e.f showplan */
    @Test public void testOptionMakeDependent2(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.addDependentGroup("a.b.c"); //$NON-NLS-1$
        option.addDependentGroup("d.e.f"); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option makedep a.b.c, d.e.f",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION MAKEDEP a.b.c, d.e.f",  //$NON-NLS-1$
                 query);
    }

    /*+* Select a From db.g Option makedep a.b.c, d.e.f, x.y.z */
    @Test public void testOptionMakeDependent3(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.addDependentGroup("a.b.c"); //$NON-NLS-1$
        option.addDependentGroup("d.e.f"); //$NON-NLS-1$
        option.addDependentGroup("x.y.z"); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option makedep a.b.c, d.e.f, x.y.z",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION MAKEDEP a.b.c, d.e.f, x.y.z",  //$NON-NLS-1$
                 query);
    }

    /*+* Select a From db.g Option makenotdep a.b.c */
    @Test public void testOptionMakeNotDependent1(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.addNotDependentGroup("a.b.c"); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option makenotdep a.b.c",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION MAKENOTDEP a.b.c",  //$NON-NLS-1$
                 query);
    }

    /*+* Select a From db.g Option makenotdep a.b.c, d.e.f showplan */
    @Test public void testOptionMakeNotDependent2(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.addNotDependentGroup("a.b.c"); //$NON-NLS-1$
        option.addNotDependentGroup("d.e.f"); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option makeNOTdep a.b.c, d.e.f",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION MAKENOTDEP a.b.c, d.e.f",  //$NON-NLS-1$
                 query);
    }

    /*+* Select a From db.g Option makenotdep a.b.c, d.e.f, x.y.z */
    @Test public void testOptionMakeNotDependent3(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.addNotDependentGroup("a.b.c"); //$NON-NLS-1$
        option.addNotDependentGroup("d.e.f"); //$NON-NLS-1$
        option.addNotDependentGroup("x.y.z"); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option makenotdep a.b.c, d.e.f, x.y.z",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION MAKENOTDEP a.b.c, d.e.f, x.y.z",  //$NON-NLS-1$
                 query);
    }

    @Test public void testDepOptions1() {
        GroupSymbol a = new GroupSymbol("a"); //$NON-NLS-1$
        GroupSymbol b = new GroupSymbol("b"); //$NON-NLS-1$
        ElementSymbol x = new ElementSymbol("a.x", true); //$NON-NLS-1$
        ElementSymbol y = new ElementSymbol("b.y", true); //$NON-NLS-1$

        From from = new From(Arrays.asList(new UnaryFromClause(a), new UnaryFromClause(b)));

        Option option = new Option();
        option.addDependentGroup("a"); //$NON-NLS-1$
        option.addNotDependentGroup("b"); //$NON-NLS-1$

        Select select = new Select(Arrays.asList(x, y));

        Criteria criteria = new CompareCriteria(x, CompareCriteria.EQ, y);
        Query query = new Query(select, from, criteria, null, null, null, option);
        TestParser.helpTest("Select a.x, b.y From a, b WHERE a.x = b.y option makedep a makenotdep b",  //$NON-NLS-1$
                 "SELECT a.x, b.y FROM a, b WHERE a.x = b.y OPTION MAKEDEP a MAKENOTDEP b",  //$NON-NLS-1$
                 query);
    }

    @Test public void testOptionMakeDepInline5(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        GroupSymbol g3 = new GroupSymbol("db.g3"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$
        ElementSymbol c = new ElementSymbol("c");  //$NON-NLS-1$

        List<Object> crits = new ArrayList<Object>();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.setMakeDep(true);
        List<Object> crits2 = new ArrayList<Object>();
        crits2.add(new CompareCriteria(a, CompareCriteria.EQ, c));
        JoinPredicate jp2 = new JoinPredicate(jp, new UnaryFromClause(g3), JoinType.JOIN_LEFT_OUTER, crits2);
        From from = new From();
        from.addClause(jp2);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From (db.g1 JOIN db.g2 ON a = b) makedep LEFT OUTER JOIN db.g3 ON a = c",  //$NON-NLS-1$
                 "SELECT a FROM /*+ MAKEDEP */ (db.g1 INNER JOIN db.g2 ON a = b) LEFT OUTER JOIN db.g3 ON a = c",  //$NON-NLS-1$
                 query);

        //ensure that the new string form is parsable
        TestParser.helpTest(query.toString(), query.toString(), query);
    }

    @Test public void testOptionMakeDepOptionsComment(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        GroupSymbol g3 = new GroupSymbol("db.g3"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$
        ElementSymbol c = new ElementSymbol("c");  //$NON-NLS-1$

        List<Object> crits = new ArrayList<Object>();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        Option.MakeDep makeDep = new Option.MakeDep();
        makeDep.setMax(5);
        makeDep.setJoin(false);
        jp.setMakeDep(makeDep);
        List<Object> crits2 = new ArrayList<Object>();
        crits2.add(new CompareCriteria(a, CompareCriteria.EQ, c));
        JoinPredicate jp2 = new JoinPredicate(jp, new UnaryFromClause(g3), JoinType.JOIN_LEFT_OUTER, crits2);
        From from = new From();
        from.addClause(jp2);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From /*+ MAKEDEP(max:5 no join) */ (db.g1 JOIN db.g2 ON a = b) LEFT OUTER JOIN db.g3 ON a = c",  //$NON-NLS-1$
                 "SELECT a FROM /*+ MAKEDEP(MAX:5 NO JOIN) */ (db.g1 INNER JOIN db.g2 ON a = b) LEFT OUTER JOIN db.g3 ON a = c",  //$NON-NLS-1$
                 query);

        //ensure that the new string form is parsable
        TestParser.helpTest(query.toString(), query.toString(), query);
    }

    @Test public void testCache() {
        String sql = "/*+ cache */ SELECT * FROM t1"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        from.addClause(ufc);
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        query.setFrom(from);
        query.setCacheHint(new CacheHint());
        TestParser.helpTest(sql, "/*+ cache */ SELECT * FROM t1", query);         //$NON-NLS-1$
    }

    @Test public void testCacheMultipleComments() {
        String sql = "/* hello */ /*+ cache */ /* world */ SELECT * FROM t1"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        from.addClause(ufc);
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        query.setFrom(from);
        query.setCacheHint(new CacheHint());
        TestParser.helpTest(sql, "/*+ cache */ SELECT * FROM t1", query);         //$NON-NLS-1$
    }

    @Test public void testCacheProc() {
        String sql = "/*+ cache */ CREATE VIRTUAL PROCEDURE BEGIN END"; //$NON-NLS-1$
        CreateProcedureCommand command = new CreateProcedureCommand(new Block());
        command.setCacheHint(new CacheHint());
        TestParser.helpTest(sql, "/*+ cache */ BEGIN\nEND", command);         //$NON-NLS-1$
    }

    @Test public void testCacheScope() {
        String sql = "/*+ cache(pref_mem scope:session) */ SELECT * FROM t1"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        from.addClause(ufc);
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        query.setFrom(from);
        CacheHint hint = new CacheHint();
        hint.setScope(Scope.SESSION);
        hint.setPrefersMemory(true);
        query.setCacheHint(hint);
        TestParser.helpTest(sql, "/*+ cache(pref_mem scope:SESSION) */ SELECT * FROM t1", query);         //$NON-NLS-1$
    }

    @Test public void testCache1() {
        String sql = "/*+ cache */ execute foo()"; //$NON-NLS-1$

        StoredProcedure sp = new StoredProcedure();
        sp.setCacheHint(new CacheHint());
        sp.setProcedureName("foo"); //$NON-NLS-1$

        TestParser.helpTest(sql, "/*+ cache */ EXEC foo()", sp);         //$NON-NLS-1$
    }

    @Test public void testExpandedCacheHint() {
        String sql = "/*+ cache( pref_mem ttl:2000) */ SELECT * FROM t1"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        from.addClause(ufc);
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        query.setFrom(from);
        CacheHint hint = new CacheHint();
        hint.setPrefersMemory(true);
        hint.setTtl(Long.valueOf(2000));
        query.setCacheHint(hint);
        TestParser.helpTest(sql, "/*+ cache(pref_mem ttl:2000) */ SELECT * FROM t1", query);         //$NON-NLS-1$
    }

    @Test public void testCacheHintUnion() {
        String sql = "/*+ cache( pref_mem) */ SELECT * FROM t1 union select * from t2"; //$NON-NLS-1$

        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        from.addClause(ufc);
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        query.setFrom(from);

        Query query1 = new Query();
        select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query1.setSelect(select);
        from = new From();
        ufc = new UnaryFromClause();
        from.addClause(ufc);
        ufc.setGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query1.setFrom(from);

        SetQuery sq = new SetQuery(Operation.UNION, false, query, query1);
        CacheHint hint = new CacheHint();
        hint.setPrefersMemory(true);
        sq.setCacheHint(hint);
        TestParser.helpTest(sql, "/*+ cache(pref_mem) */ SELECT * FROM t1 UNION SELECT * FROM t2", sq);         //$NON-NLS-1$
    }

    @Test public void testCacheHintCallableStatement() {
        String sql = "/*+ cache */ { ? = call proc() }"; //$NON-NLS-1$
        StoredProcedure sp = new StoredProcedure();
        SPParameter param = new SPParameter(1, null);
        param.setParameterType(SPParameter.RETURN_VALUE);
        sp.setParameter(param);
        sp.setProcedureName("proc");
        sp.setCallableStatement(true);
        CacheHint hint = new CacheHint();
        sp.setCacheHint(hint);
        TestParser.helpTest(sql, "/*+ cache */ ? = EXEC proc()", sp);         //$NON-NLS-1$
    }

    @Test public void testMergeJoinHint() {
        String sql = "SELECT e1 FROM m.g2 WHERE EXISTS /*+ MJ */ (SELECT e1 FROM m.g1)"; //$NON-NLS-1$
        Query q = TestParser.exampleExists(true);
        TestParser.helpTest(sql, "SELECT e1 FROM m.g2 WHERE EXISTS /*+ MJ */ (SELECT e1 FROM m.g1)", q);         //$NON-NLS-1$
    }

    @Test public void testMergeJoinHint1() {
        String sql = "SELECT a FROM db.g WHERE b IN /*+ MJ */ (SELECT a FROM db.g WHERE a2 = 5)"; //$NON-NLS-1$
        Query q = TestParser.exampleIn(true);
        TestParser.helpTest(sql, "SELECT a FROM db.g WHERE b IN /*+ MJ */ (SELECT a FROM db.g WHERE a2 = 5)", q);         //$NON-NLS-1$
    }

    @Test public void testNoUnnest() throws QueryParserException {
        String sql = "SELECT a FROM /*+ no_unnest */ (SELECT a FROM db.g WHERE a2 = 5) x"; //$NON-NLS-1$
        assertEquals("SELECT a FROM /*+ NO_UNNEST */ (SELECT a FROM db.g WHERE a2 = 5) AS x", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());         //$NON-NLS-1$
    }

    @Test public void testNoUnnest1() throws QueryParserException {
        String sql = "SELECT a FROM db.g WHERE a2 = all /*+ no_unnest */ (select a from db.g)"; //$NON-NLS-1$
        assertEquals("SELECT a FROM db.g WHERE a2 = ALL /*+ NO_UNNEST */ (SELECT a FROM db.g)", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());         //$NON-NLS-1$
    }

    @Test public void testNoUnnest2() throws QueryParserException {
        String sql = "SELECT a FROM db.g WHERE a2 = /*+ no_unnest */ (select a from db.g)"; //$NON-NLS-1$
        assertEquals("SELECT a FROM db.g WHERE a2 = /*+ NO_UNNEST */ (SELECT a FROM db.g)", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());         //$NON-NLS-1$
    }

    @Test public void testNonStrictLimit() throws QueryParserException {
        String sql = "SELECT a FROM x /*+ non_strict */ limit 1"; //$NON-NLS-1$
        assertEquals("SELECT a FROM x /*+ NON_STRICT */ LIMIT 1", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());         //$NON-NLS-1$

        sql = "SELECT a FROM x /*+ non_strict */ offset 1 row"; //$NON-NLS-1$
        assertEquals("SELECT a FROM x /*+ NON_STRICT */ OFFSET 1 ROWS", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());         //$NON-NLS-1$

        sql = "SELECT a FROM x /*+ non_strict */ fetch first 1 rows only"; //$NON-NLS-1$
        assertEquals("SELECT a FROM x /*+ NON_STRICT */ LIMIT 1", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());         //$NON-NLS-1$
    }

    @Test public void testNestedComments() throws QueryParserException {
        String sql = "/*+ /*nested*/ */ SELECT a FROM x limit 1"; //$NON-NLS-1$
        assertEquals("SELECT a FROM x LIMIT 1", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());         //$NON-NLS-1$
    }

    @Test public void testSourceHint() throws QueryParserException {
        String sql = "SELECT /*+ sh:'foo' oracle:'leading' */ a FROM x limit 1"; //$NON-NLS-1$
        assertEquals("SELECT /*+sh:'foo' oracle:'leading' */ a FROM x LIMIT 1", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());         //$NON-NLS-1$

        sql = "(SELECT /*+ sh:'foo' oracle:'leading' */ a FROM x limit 1) union all select 1"; //$NON-NLS-1$
        assertEquals("(SELECT /*+sh:'foo' oracle:'leading' */ a FROM x LIMIT 1) UNION ALL SELECT 1", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());

        sql = "(SELECT /*+ sh keep aliases:'foo' oracle keep aliases:'leading' */ a FROM x limit 1) union all select 1"; //$NON-NLS-1$
        assertEquals("(SELECT /*+sh KEEP ALIASES:'foo' oracle KEEP ALIASES:'leading' */ a FROM x LIMIT 1) UNION ALL SELECT 1", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());

        sql = "(SELECT /*+ sh keep aliases oracle:'leading' */ a FROM x limit 1) union all select 1"; //$NON-NLS-1$
        assertEquals("(SELECT /*+sh KEEP ALIASES oracle:'leading' */ a FROM x LIMIT 1) UNION ALL SELECT 1", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());

        sql = "(SELECT /*+ ashoe */ a FROM x limit 1) union all select 1"; //$NON-NLS-1$
        assertEquals("(SELECT a FROM x LIMIT 1) UNION ALL SELECT 1", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());

        sql = "(SELECT /*+ shoe */ a FROM x limit 1) union all select 1"; //$NON-NLS-1$
        assertEquals("(SELECT a FROM x LIMIT 1) UNION ALL SELECT 1", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());
    }

    @Test public void testNestedSourceHint() throws QueryParserException {
        String sql = "WITH x as (SELECT /*+ sh:'x' */ 1) SELECT /*+ sh:'foo' bar:'leading' */ e1 from pm1.g1 order by e1 limit 1"; //$NON-NLS-1$
        assertEquals("WITH x AS (SELECT /*+sh:'x' */ 1) SELECT /*+sh:'foo' bar:'leading' */ e1 FROM pm1.g1 ORDER BY e1 LIMIT 1", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());

        sql = "create virtual procedure begin loop on (select /*+ sh:'y' */ 1) as x begin end end"; //$NON-NLS-1$
        assertEquals("BEGIN\nLOOP ON (SELECT /*+sh:'y' */ 1) AS x\nBEGIN\nEND\nEND", QueryParser.getQueryParser().parseCommand(sql).toString());
    }

    @Test public void testNoInline() throws QueryParserException {
        String sql = "WITH x as /*+no_inline*/ (SELECT 1) SELECT e1 from pm1.g1 order by e1 limit 1"; //$NON-NLS-1$
        assertEquals("WITH x AS /*+ no_inline */ (SELECT 1) SELECT e1 FROM pm1.g1 ORDER BY e1 LIMIT 1", QueryParser.getQueryParser().parseCommand(sql, ParseInfo.DEFAULT_INSTANCE).toString());
    }

    @Test public void testMakedepOptions() throws QueryParserException {
        String sql = "Select a From db.g1 JOIN db.g2 MAKEDEP(max:300) ON a = b"; //$NON-NLS-1$
        assertEquals("SELECT a FROM db.g1 INNER JOIN /*+ MAKEDEP(MAX:300) */ db.g2 ON a = b", QueryParser.getQueryParser().parseCommand(sql, new ParseInfo()).toString());         //$NON-NLS-1$
    }

    @Test public void testMakeindOptions() throws QueryParserException {
        String sql = "Select a From db.g1 JOIN db.g2 MAKEIND(max:300 no join) ON a = b"; //$NON-NLS-1$
        assertEquals("SELECT a FROM db.g1 INNER JOIN /*+ MAKEIND(MAX:300 NO JOIN) */ db.g2 ON a = b", QueryParser.getQueryParser().parseCommand(sql, new ParseInfo()).toString());         //$NON-NLS-1$
    }

    @Test public void testPreserve() throws QueryParserException {
        String sql = "Select a From /*+ preserve */ db.g1 JOIN db.g2 ON a = b"; //$NON-NLS-1$
        assertEquals("SELECT a FROM /*+ PRESERVE */ db.g1 INNER JOIN db.g2 ON a = b", QueryParser.getQueryParser().parseCommand(sql, new ParseInfo()).clone().toString());         //$NON-NLS-1$
    }

}
