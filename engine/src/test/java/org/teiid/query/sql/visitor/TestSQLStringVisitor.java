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

package org.teiid.query.sql.visitor;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.lang.ExistsCriteria.SubqueryHint;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.proc.AssignmentStatement;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.proc.DeclareStatement;
import org.teiid.query.sql.proc.RaiseStatement;
import org.teiid.query.sql.symbol.*;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TypeFacility;

@SuppressWarnings("nls")
public class TestSQLStringVisitor {

    // ################################## TEST HELPERS ################################

    private void helpTest(LanguageObject obj, String expectedStr) {
        String actualStr = SQLStringVisitor.getSQLString(obj);
        assertEquals("Expected and actual strings don't match: ", expectedStr, actualStr); //$NON-NLS-1$
    }

    // ################################## ACTUAL TESTS ################################

    @Test public void testNull() {
        String sql = SQLStringVisitor.getSQLString(null);

        assertEquals("Incorrect string for null object", SQLStringVisitor.UNDEFINED, sql); //$NON-NLS-1$
    }

    @Test public void testBetweenCriteria1() {
        BetweenCriteria bc = new BetweenCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            new Constant(new Integer(1000)),
            new Constant(new Integer(2000)) );
        helpTest(bc, "m.g.c1 BETWEEN 1000 AND 2000"); //$NON-NLS-1$
    }

    @Test public void testBetweenCriteria2() {
        BetweenCriteria bc = new BetweenCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            new Constant(new Integer(1000)),
            new Constant(new Integer(2000)) );
        bc.setNegated(true);
        helpTest(bc, "m.g.c1 NOT BETWEEN 1000 AND 2000"); //$NON-NLS-1$
    }

    @Test public void testCompareCriteria1() {
        CompareCriteria cc = new CompareCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            CompareCriteria.EQ,
            new Constant("abc") ); //$NON-NLS-1$

        helpTest(cc, "m.g.c1 = 'abc'"); //$NON-NLS-1$
    }

    @Test public void testCompareCriteria2() {
        CompareCriteria cc = new CompareCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            CompareCriteria.NE,
            new Constant("abc") ); //$NON-NLS-1$

        helpTest(cc, "m.g.c1 <> 'abc'"); //$NON-NLS-1$
    }

    @Test public void testCompareCriteria3() {
        CompareCriteria cc = new CompareCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            CompareCriteria.GT,
            new Constant("abc") ); //$NON-NLS-1$

        helpTest(cc, "m.g.c1 > 'abc'"); //$NON-NLS-1$
    }

    @Test public void testCompareCriteria4() {
        CompareCriteria cc = new CompareCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            CompareCriteria.GE,
            new Constant("abc") ); //$NON-NLS-1$

        helpTest(cc, "m.g.c1 >= 'abc'"); //$NON-NLS-1$
    }

    @Test public void testCompareCriteria5() {
        CompareCriteria cc = new CompareCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            CompareCriteria.LT,
            new Constant("abc") ); //$NON-NLS-1$

        helpTest(cc, "m.g.c1 < 'abc'"); //$NON-NLS-1$
    }

    @Test public void testCompareCriteria6() {
        CompareCriteria cc = new CompareCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            CompareCriteria.LE,
            new Constant("abc") ); //$NON-NLS-1$

        helpTest(cc, "m.g.c1 <= 'abc'"); //$NON-NLS-1$
    }

    @Test public void testCompareCriteria7() {
        CompareCriteria cc = new CompareCriteria(
            null,
            CompareCriteria.EQ,
            null );

        helpTest(cc, "<undefined> = <undefined>"); //$NON-NLS-1$
    }

    @Test public void testCompoundCriteria1() {
        CompareCriteria cc = new CompareCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            CompareCriteria.EQ,
            new Constant("abc") ); //$NON-NLS-1$
        List<Criteria> crits = new ArrayList<Criteria>();
        crits.add(cc);
        CompoundCriteria comp = new CompoundCriteria(CompoundCriteria.AND, crits);

        helpTest(comp, "m.g.c1 = 'abc'"); //$NON-NLS-1$
    }

    @Test public void testCompoundCriteria2() {
        CompareCriteria cc1 = new CompareCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            CompareCriteria.EQ,
            new Constant("abc") ); //$NON-NLS-1$
        CompareCriteria cc2 = new CompareCriteria(
            new ElementSymbol("m.g.c2"), //$NON-NLS-1$
            CompareCriteria.EQ,
            new Constant("abc") ); //$NON-NLS-1$
        List<Criteria> crits = new ArrayList<Criteria>();
        crits.add(cc1);
        crits.add(cc2);
        CompoundCriteria comp = new CompoundCriteria(CompoundCriteria.AND, crits);

        helpTest(comp, "(m.g.c1 = 'abc') AND (m.g.c2 = 'abc')"); //$NON-NLS-1$
    }

    @Test public void testCompoundCriteria3() {
        CompareCriteria cc1 = new CompareCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            CompareCriteria.EQ,
            new Constant("abc") ); //$NON-NLS-1$
        CompareCriteria cc2 = new CompareCriteria(
            new ElementSymbol("m.g.c2"), //$NON-NLS-1$
            CompareCriteria.EQ,
            new Constant("abc") ); //$NON-NLS-1$
        CompareCriteria cc3 = new CompareCriteria(
            new ElementSymbol("m.g.c3"), //$NON-NLS-1$
            CompareCriteria.EQ,
            new Constant("abc") ); //$NON-NLS-1$
        List<Criteria> crits = new ArrayList<Criteria>();
        crits.add(cc1);
        crits.add(cc2);
        crits.add(cc3);
        CompoundCriteria comp = new CompoundCriteria(CompoundCriteria.OR, crits);

        helpTest(comp, "(m.g.c1 = 'abc') OR (m.g.c2 = 'abc') OR (m.g.c3 = 'abc')"); //$NON-NLS-1$
    }

    @Test public void testCompoundCriteria4() {
        CompareCriteria cc1 = new CompareCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            CompareCriteria.EQ,
            new Constant("abc") ); //$NON-NLS-1$
        List<Criteria> crits = new ArrayList<Criteria>();
        crits.add(cc1);
        crits.add(null);
        CompoundCriteria comp = new CompoundCriteria(CompoundCriteria.OR, crits);

        helpTest(comp, "(m.g.c1 = 'abc') OR (<undefined>)"); //$NON-NLS-1$
    }

    @Test public void testCompoundCriteria5() {
        CompareCriteria cc1 = new CompareCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            CompareCriteria.EQ,
            new Constant("abc") ); //$NON-NLS-1$
        List<Criteria> crits = new ArrayList<Criteria>();
        crits.add(null);
        crits.add(cc1);
        CompoundCriteria comp = new CompoundCriteria(CompoundCriteria.OR, crits);

        helpTest(comp, "(<undefined>) OR (m.g.c1 = 'abc')"); //$NON-NLS-1$
    }

    @Test public void testCompoundCriteria6() {
        CompareCriteria cc1 = new CompareCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            CompareCriteria.EQ,
            new Constant("abc") ); //$NON-NLS-1$
        List<Criteria> crits = new ArrayList<Criteria>();
        crits.add(cc1);
        crits.add(null);
        CompoundCriteria comp = new CompoundCriteria(CompoundCriteria.OR, crits);

        helpTest(comp, "(m.g.c1 = 'abc') OR (<undefined>)"); //$NON-NLS-1$
    }

    @Test public void testDelete1() {
        Delete delete = new Delete();
        delete.setGroup(new GroupSymbol("m.g"));     //$NON-NLS-1$

        helpTest(delete, "DELETE FROM m.g"); //$NON-NLS-1$
    }

    @Test public void testDelete2() {
        Delete delete = new Delete();
        delete.setGroup(new GroupSymbol("m.g"));    //$NON-NLS-1$
        delete.setCriteria(new CompareCriteria(
            new ElementSymbol("m.g.c1"), //$NON-NLS-1$
            CompareCriteria.EQ,
            new Constant("abc")) ); //$NON-NLS-1$


        helpTest(delete, "DELETE FROM m.g WHERE m.g.c1 = 'abc'"); //$NON-NLS-1$
    }

    @Test public void testFrom1() {
        From from = new From();
        from.addGroup(new GroupSymbol("m.g1"));    //$NON-NLS-1$
        from.addGroup(new GroupSymbol("m.g2")); //$NON-NLS-1$

        helpTest(from, "FROM m.g1, m.g2");     //$NON-NLS-1$
    }

    @Test public void testFrom2() {
        From from = new From();
        from.addClause(new UnaryFromClause(new GroupSymbol("m.g1")));   //$NON-NLS-1$
        from.addClause(new JoinPredicate(
            new UnaryFromClause(new GroupSymbol("m.g2")), //$NON-NLS-1$
            new UnaryFromClause(new GroupSymbol("m.g3")),  //$NON-NLS-1$
            JoinType.JOIN_CROSS) );

        helpTest(from, "FROM m.g1, m.g2 CROSS JOIN m.g3");     //$NON-NLS-1$
    }

    @Test public void testGroupBy1() {
        GroupBy gb = new GroupBy();
        gb.addSymbol(new ElementSymbol("m.g.e1")); //$NON-NLS-1$

        helpTest(gb, "GROUP BY m.g.e1");         //$NON-NLS-1$
    }

    @Test public void testGroupBy2() {
        GroupBy gb = new GroupBy();
        gb.addSymbol(new ElementSymbol("m.g.e1")); //$NON-NLS-1$
        gb.addSymbol(new ElementSymbol("m.g.e2")); //$NON-NLS-1$
        gb.addSymbol(new ElementSymbol("m.g.e3")); //$NON-NLS-1$

        helpTest(gb, "GROUP BY m.g.e1, m.g.e2, m.g.e3");        //$NON-NLS-1$
    }

    @Test public void testInsert1() {
           Insert insert = new Insert();
           insert.setGroup(new GroupSymbol("m.g1"));      //$NON-NLS-1$

           List<ElementSymbol> vars = new ArrayList<ElementSymbol>();
           vars.add(new ElementSymbol("e1")); //$NON-NLS-1$
           vars.add(new ElementSymbol("e2")); //$NON-NLS-1$
           insert.setVariables(vars);
           List<Constant> values = new ArrayList<Constant>();
           values.add(new Constant(new Integer(5)));
           values.add(new Constant("abc")); //$NON-NLS-1$
           insert.setValues(values);

           helpTest(insert, "INSERT INTO m.g1 (e1, e2) VALUES (5, 'abc')"); //$NON-NLS-1$
    }

    @Test public void testMerge1() {
           Insert insert = new Insert();
           insert.setUpsert(true);
           insert.setGroup(new GroupSymbol("m.g1"));      //$NON-NLS-1$

           List<ElementSymbol> vars = new ArrayList<ElementSymbol>();
           vars.add(new ElementSymbol("e1")); //$NON-NLS-1$
           vars.add(new ElementSymbol("e2")); //$NON-NLS-1$
           insert.setVariables(vars);
           List<Constant> values = new ArrayList<Constant>();
           values.add(new Constant(new Integer(5)));
           values.add(new Constant("abc")); //$NON-NLS-1$
           insert.setValues(values);

           helpTest(insert, "UPSERT INTO m.g1 (e1, e2) VALUES (5, 'abc')"); //$NON-NLS-1$
    }

      @Test public void testIsNullCriteria1() {
          IsNullCriteria inc = new IsNullCriteria();
          inc.setExpression(new Constant("abc")); //$NON-NLS-1$

          helpTest(inc, "'abc' IS NULL"); //$NON-NLS-1$
      }

      @Test public void testIsNullCriteria2() {
          IsNullCriteria inc = new IsNullCriteria();
          inc.setExpression(new ElementSymbol("m.g.e1")); //$NON-NLS-1$

          helpTest(inc, "m.g.e1 IS NULL"); //$NON-NLS-1$
      }

    @Test public void testIsNullCriteria3() {
        IsNullCriteria inc = new IsNullCriteria();
        helpTest(inc, "<undefined> IS NULL"); //$NON-NLS-1$
    }

    @Test public void testIsNullCriteria4() {
        IsNullCriteria inc = new IsNullCriteria();
        inc.setExpression(new ElementSymbol("m.g.e1")); //$NON-NLS-1$
        inc.setNegated(true);
        helpTest(inc, "m.g.e1 IS NOT NULL"); //$NON-NLS-1$
    }

    @Test public void testJoinPredicate1() {
        JoinPredicate jp = new JoinPredicate(
            new UnaryFromClause(new GroupSymbol("m.g2")), //$NON-NLS-1$
            new UnaryFromClause(new GroupSymbol("m.g3")),  //$NON-NLS-1$
            JoinType.JOIN_CROSS);

        helpTest(jp, "m.g2 CROSS JOIN m.g3"); //$NON-NLS-1$
    }

    @Test public void testOptionalJoinPredicate1() {
        JoinPredicate jp = new JoinPredicate(
            new UnaryFromClause(new GroupSymbol("m.g2")), //$NON-NLS-1$
            new UnaryFromClause(new GroupSymbol("m.g3")),  //$NON-NLS-1$
            JoinType.JOIN_CROSS);
        jp.setOptional(true);
        helpTest(jp, "/*+ optional */ (m.g2 CROSS JOIN m.g3)"); //$NON-NLS-1$
    }

    @Test public void testJoinPredicate2() {
        ArrayList<Criteria> crits = new ArrayList<Criteria>();
        crits.add(new CompareCriteria(new ElementSymbol("m.g2.e1"), CompareCriteria.EQ, new ElementSymbol("m.g3.e1"))); //$NON-NLS-1$ //$NON-NLS-2$
        JoinPredicate jp = new JoinPredicate(
            new UnaryFromClause(new GroupSymbol("m.g2")), //$NON-NLS-1$
            new UnaryFromClause(new GroupSymbol("m.g3")), //$NON-NLS-1$
            JoinType.JOIN_INNER,
            crits );

        helpTest(jp, "m.g2 INNER JOIN m.g3 ON m.g2.e1 = m.g3.e1"); //$NON-NLS-1$
    }

    @Test public void testJoinPredicate3() {
        ArrayList<Criteria> crits = new ArrayList<Criteria>();
        crits.add(new CompareCriteria(new ElementSymbol("m.g2.e1"), CompareCriteria.EQ, new ElementSymbol("m.g3.e1"))); //$NON-NLS-1$ //$NON-NLS-2$
        crits.add(new CompareCriteria(new ElementSymbol("m.g2.e2"), CompareCriteria.EQ, new ElementSymbol("m.g3.e2"))); //$NON-NLS-1$ //$NON-NLS-2$
        JoinPredicate jp = new JoinPredicate(
            new UnaryFromClause(new GroupSymbol("m.g2")), //$NON-NLS-1$
            new UnaryFromClause(new GroupSymbol("m.g3")), //$NON-NLS-1$
            JoinType.JOIN_INNER,
            crits );

        helpTest(jp, "m.g2 INNER JOIN m.g3 ON m.g2.e1 = m.g3.e1 AND m.g2.e2 = m.g3.e2"); //$NON-NLS-1$
    }

    @Test public void testJoinPredicate4() {
        ArrayList<Criteria> crits = new ArrayList<Criteria>();
        crits.add(new CompareCriteria(new ElementSymbol("m.g2.e1"), CompareCriteria.EQ, new ElementSymbol("m.g3.e1"))); //$NON-NLS-1$ //$NON-NLS-2$
        JoinPredicate jp = new JoinPredicate(
            new UnaryFromClause(new GroupSymbol("m.g2")), //$NON-NLS-1$
            new UnaryFromClause(new GroupSymbol("m.g3")), //$NON-NLS-1$
            JoinType.JOIN_INNER,
            crits );

        JoinPredicate jp2 = new JoinPredicate(
            jp,
            new UnaryFromClause(new GroupSymbol("m.g1")), //$NON-NLS-1$
            JoinType.JOIN_CROSS);

        helpTest(jp2, "(m.g2 INNER JOIN m.g3 ON m.g2.e1 = m.g3.e1) CROSS JOIN m.g1"); //$NON-NLS-1$
    }

    @Test public void testJoinPredicate5() {
        ArrayList<Criteria> crits = new ArrayList<Criteria>();
        crits.add(new NotCriteria(new CompareCriteria(new ElementSymbol("m.g2.e1"), CompareCriteria.EQ, new ElementSymbol("m.g3.e1")))); //$NON-NLS-1$ //$NON-NLS-2$
        JoinPredicate jp = new JoinPredicate(
            new UnaryFromClause(new GroupSymbol("m.g2")), //$NON-NLS-1$
            new UnaryFromClause(new GroupSymbol("m.g3")), //$NON-NLS-1$
            JoinType.JOIN_INNER,
            crits );

        helpTest(jp, "m.g2 INNER JOIN m.g3 ON NOT (m.g2.e1 = m.g3.e1)"); //$NON-NLS-1$
    }

    @Test public void testJoinType1() {
        helpTest(JoinType.JOIN_CROSS, "CROSS JOIN");     //$NON-NLS-1$
    }

    @Test public void testJoinType2() {
        helpTest(JoinType.JOIN_INNER, "INNER JOIN");     //$NON-NLS-1$
    }

    @Test public void testJoinType3() {
        helpTest(JoinType.JOIN_RIGHT_OUTER, "RIGHT OUTER JOIN");     //$NON-NLS-1$
    }

    @Test public void testJoinType4() {
        helpTest(JoinType.JOIN_LEFT_OUTER, "LEFT OUTER JOIN");     //$NON-NLS-1$
    }

    @Test public void testJoinType5() {
        helpTest(JoinType.JOIN_FULL_OUTER, "FULL OUTER JOIN");     //$NON-NLS-1$
    }

    @Test public void testMatchCriteria1() {
        MatchCriteria mc = new MatchCriteria();
        mc.setLeftExpression(new ElementSymbol("m.g.e1"));     //$NON-NLS-1$
        mc.setRightExpression(new Constant("abc")); //$NON-NLS-1$

        helpTest(mc, "m.g.e1 LIKE 'abc'"); //$NON-NLS-1$
    }

    @Test public void testMatchCriteria2() {
        MatchCriteria mc = new MatchCriteria();
        mc.setLeftExpression(new ElementSymbol("m.g.e1"));     //$NON-NLS-1$
        mc.setRightExpression(new Constant("%")); //$NON-NLS-1$
        mc.setEscapeChar('#');

        helpTest(mc, "m.g.e1 LIKE '%' ESCAPE '#'"); //$NON-NLS-1$
    }

    @Test public void testMatchCriteria3() {
        MatchCriteria mc = new MatchCriteria();
        mc.setLeftExpression(new ElementSymbol("m.g.e1"));     //$NON-NLS-1$
        mc.setRightExpression(new Constant("abc")); //$NON-NLS-1$
        mc.setNegated(true);
        helpTest(mc, "m.g.e1 NOT LIKE 'abc'"); //$NON-NLS-1$
    }

    @Test public void testNotCriteria1() {
        NotCriteria not = new NotCriteria(new IsNullCriteria(new ElementSymbol("m.g.e1"))); //$NON-NLS-1$
        helpTest(not, "NOT (m.g.e1 IS NULL)"); //$NON-NLS-1$
    }

    @Test public void testNotCriteria2() {
        NotCriteria not = new NotCriteria();
        helpTest(not, "NOT (<undefined>)"); //$NON-NLS-1$
    }

    @Test public void testOption1() {
        Option option = new Option();
        helpTest(option, "OPTION");     //$NON-NLS-1$
    }

    @Test public void testOption5() {
        Option option = new Option();
        option.addDependentGroup("abc"); //$NON-NLS-1$
        option.addDependentGroup("def"); //$NON-NLS-1$
        option.addDependentGroup("xyz"); //$NON-NLS-1$
        helpTest(option, "OPTION MAKEDEP abc, def, xyz");     //$NON-NLS-1$
    }

    @Test public void testOption6() {
        Option option = new Option();
        option.addDependentGroup("abc"); //$NON-NLS-1$
        option.addDependentGroup("def"); //$NON-NLS-1$
        option.addDependentGroup("xyz"); //$NON-NLS-1$
        helpTest(option, "OPTION MAKEDEP abc, def, xyz");     //$NON-NLS-1$
    }

    @Test public void testOption8() {
        Option option = new Option();
        option.addNoCacheGroup("abc"); //$NON-NLS-1$
        option.addNoCacheGroup("def"); //$NON-NLS-1$
        option.addNoCacheGroup("xyz"); //$NON-NLS-1$
        helpTest(option, "OPTION NOCACHE abc, def, xyz");     //$NON-NLS-1$
    }

//  related to defect 14423
    @Test public void testOption9() {
        Option option = new Option();
        option.setNoCache(true);
        helpTest(option, "OPTION NOCACHE");     //$NON-NLS-1$
    }

    @Test public void testOrderBy1() {
        OrderBy ob = new OrderBy();
        ob.addVariable(new ElementSymbol("e1")); //$NON-NLS-1$

        helpTest(ob, "ORDER BY e1");     //$NON-NLS-1$
    }

    @Test public void testOrderBy2() {
        OrderBy ob = new OrderBy();
        ob.addVariable(new ElementSymbol("e1")); //$NON-NLS-1$
        ob.addVariable(new AliasSymbol("x", new ElementSymbol("e2"))); //$NON-NLS-1$ //$NON-NLS-2$

        helpTest(ob, "ORDER BY e1, x");     //$NON-NLS-1$
    }

    @Test public void testOrderBy3() {
        OrderBy ob = new OrderBy();
        ob.addVariable(new ElementSymbol("e1"), OrderBy.DESC); //$NON-NLS-1$
        ob.addVariable(new ElementSymbol("x"), OrderBy.DESC); //$NON-NLS-1$

        helpTest(ob, "ORDER BY e1 DESC, x DESC");     //$NON-NLS-1$
    }

    @Test public void testQuery1() {
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);

        helpTest(query, "SELECT * FROM m.g");             //$NON-NLS-1$
    }

    @Test public void testQuery2() {
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        CompareCriteria cc = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, new Constant(new Integer(5))); //$NON-NLS-1$
        GroupBy groupBy = new GroupBy();
        groupBy.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        CompareCriteria having = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.GT, new Constant(new Integer(0))); //$NON-NLS-1$
        OrderBy orderBy = new OrderBy();
        orderBy.addVariable(new ElementSymbol("e1")); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(cc);
        query.setGroupBy(groupBy);
        query.setHaving(having);
        query.setOrderBy(orderBy);

        helpTest(query, "SELECT * FROM m.g WHERE e1 = 5 GROUP BY e1 HAVING e1 > 0 ORDER BY e1");             //$NON-NLS-1$
    }

    @Test public void testQuery3() {
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        GroupBy groupBy = new GroupBy();
        groupBy.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        CompareCriteria having = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.GT, new Constant(new Integer(0))); //$NON-NLS-1$
        OrderBy orderBy = new OrderBy();
        orderBy.addVariable(new ElementSymbol("e1")); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setGroupBy(groupBy);
        query.setHaving(having);
        query.setOrderBy(orderBy);

        helpTest(query, "SELECT * FROM m.g GROUP BY e1 HAVING e1 > 0 ORDER BY e1");             //$NON-NLS-1$
    }

    @Test public void testQuery4() {
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        CompareCriteria cc = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, new Constant(new Integer(5))); //$NON-NLS-1$
        CompareCriteria having = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.GT, new Constant(new Integer(0))); //$NON-NLS-1$
        OrderBy orderBy = new OrderBy();
        orderBy.addVariable(new ElementSymbol("e1")); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(cc);
        query.setHaving(having);
        query.setOrderBy(orderBy);

        helpTest(query, "SELECT * FROM m.g WHERE e1 = 5 HAVING e1 > 0 ORDER BY e1");             //$NON-NLS-1$
    }

    @Test public void testQuery5() {
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        CompareCriteria cc = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, new Constant(new Integer(5))); //$NON-NLS-1$
        GroupBy groupBy = new GroupBy();
        groupBy.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        OrderBy orderBy = new OrderBy();
        orderBy.addVariable(new ElementSymbol("e1")); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(cc);
        query.setGroupBy(groupBy);
        query.setOrderBy(orderBy);

        helpTest(query, "SELECT * FROM m.g WHERE e1 = 5 GROUP BY e1 ORDER BY e1");             //$NON-NLS-1$
    }

    @Test public void testQuery6() {
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        CompareCriteria cc = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, new Constant(new Integer(5))); //$NON-NLS-1$
        GroupBy groupBy = new GroupBy();
        groupBy.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        CompareCriteria having = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.GT, new Constant(new Integer(0))); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(cc);
        query.setGroupBy(groupBy);
        query.setHaving(having);

        helpTest(query, "SELECT * FROM m.g WHERE e1 = 5 GROUP BY e1 HAVING e1 > 0");             //$NON-NLS-1$
    }

    @Test public void testQuery7() {
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        CompareCriteria cc = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, new Constant(new Integer(5))); //$NON-NLS-1$
        GroupBy groupBy = new GroupBy();
        groupBy.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        CompareCriteria having = new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.GT, new Constant(new Integer(0))); //$NON-NLS-1$
        OrderBy orderBy = new OrderBy();
        orderBy.addVariable(new ElementSymbol("e1")); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(cc);
        query.setGroupBy(groupBy);
        query.setHaving(having);
        query.setOrderBy(orderBy);

        helpTest(query, "SELECT * FROM m.g WHERE e1 = 5 GROUP BY e1 HAVING e1 > 0 ORDER BY e1");             //$NON-NLS-1$
    }

    @Test public void testSelect1() {
        Select select = new Select();
        select.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$

        helpTest(select, " e1"); //$NON-NLS-1$
    }

    @Test public void testSelect2() {
        Select select = new Select();
        select.setDistinct(true);
        select.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$

        helpTest(select, " DISTINCT e1"); //$NON-NLS-1$
    }

    @Test public void testSelect3() {
        Select select = new Select();
        select.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        select.addSymbol(new ElementSymbol("e2")); //$NON-NLS-1$

        helpTest(select, " e1, e2"); //$NON-NLS-1$
    }

    @Test public void testSetCriteria1() {
        SetCriteria sc = new SetCriteria();
        sc.setExpression(new ElementSymbol("e1"));         //$NON-NLS-1$
        sc.setValues(new ArrayList<Expression>());

        helpTest(sc, "e1 IN ()"); //$NON-NLS-1$
    }

    @Test public void testSetCriteria2() {
        SetCriteria sc = new SetCriteria();
        sc.setExpression(new ElementSymbol("e1"));     //$NON-NLS-1$
        ArrayList<Expression> values = new ArrayList<Expression>();
        values.add(new ElementSymbol("e2")); //$NON-NLS-1$
        values.add(new Constant("abc")); //$NON-NLS-1$
        sc.setValues(values);

        helpTest(sc, "e1 IN (e2, 'abc')"); //$NON-NLS-1$
    }

    @Test public void testSetCriteria3() {
        SetCriteria sc = new SetCriteria();
        sc.setExpression(new ElementSymbol("e1"));     //$NON-NLS-1$
        ArrayList<Expression> values = new ArrayList<Expression>();
        values.add(null);
        values.add(new Constant("b")); //$NON-NLS-1$
        sc.setValues(values);

        helpTest(sc, "e1 IN (<undefined>, 'b')"); //$NON-NLS-1$
    }

    @Test public void testSetCriteria4() {
        SetCriteria sc = new SetCriteria();
        sc.setExpression(new ElementSymbol("e1"));   //$NON-NLS-1$
        ArrayList<Expression> values = new ArrayList<Expression>();
        values.add(new ElementSymbol("e2")); //$NON-NLS-1$
        values.add(new Constant("abc")); //$NON-NLS-1$
        sc.setValues(values);
        sc.setNegated(true);
        helpTest(sc, "e1 NOT IN (e2, 'abc')"); //$NON-NLS-1$
    }

    @Test public void testSetQuery1() {
        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));         //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));         //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);

        SetQuery sq = new SetQuery(Operation.UNION, false, q1, q2);

        helpTest(sq, "SELECT e1 FROM m.g1 UNION SELECT e1 FROM m.g2"); //$NON-NLS-1$
    }

    @Test public void testSetQuery2() {
        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));         //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));         //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);

        SetQuery sq = new SetQuery(Operation.UNION, true, q1, q2);

        helpTest(sq, "SELECT e1 FROM m.g1 UNION ALL SELECT e1 FROM m.g2"); //$NON-NLS-1$
    }

    @Test public void testSetQuery3() {
        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));         //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));         //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);

        OrderBy orderBy = new OrderBy();
        orderBy.addVariable(new ElementSymbol("e1")); //$NON-NLS-1$

        SetQuery sq = new SetQuery(Operation.UNION, false, q1, q2);
        sq.setOrderBy(orderBy);

        helpTest(sq, "SELECT e1 FROM m.g1 UNION SELECT e1 FROM m.g2 ORDER BY e1"); //$NON-NLS-1$
    }

    @Test public void testSetQuery4() {
        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));         //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));         //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);

        SetQuery sq = new SetQuery(Operation.UNION, false, q1, q2);

        helpTest(sq, "SELECT e1 FROM m.g1 UNION SELECT e1 FROM m.g2"); //$NON-NLS-1$
    }

    @Test public void testSetQuery5() {
        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));         //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));         //$NON-NLS-1$
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);

        Select s3 = new Select();
        s3.addSymbol(new ElementSymbol("e3")); //$NON-NLS-1$
        From f3 = new From();
        f3.addGroup(new GroupSymbol("m.g3"));         //$NON-NLS-1$
        Query q3 = new Query();
        q3.setSelect(s3);
        q3.setFrom(f3);

        SetQuery sq = new SetQuery(Operation.UNION, false, q1, q2);

        SetQuery sq2 = new SetQuery(Operation.UNION, true, q3, sq);

        helpTest(sq2, "SELECT e3 FROM m.g3 UNION ALL (SELECT e1 FROM m.g1 UNION SELECT e1 FROM m.g2)"); //$NON-NLS-1$
    }

    @Test public void testSubqueryFromClause1() {
        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        SubqueryFromClause sfc = new SubqueryFromClause("temp", q1); //$NON-NLS-1$
        helpTest(sfc, "(SELECT e1 FROM m.g1) AS temp");             //$NON-NLS-1$
    }

    @Test public void testOptionalSubqueryFromClause1() {
        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        SubqueryFromClause sfc = new SubqueryFromClause("temp", q1); //$NON-NLS-1$
        sfc.setOptional(true);
        helpTest(sfc, "/*+ optional */ (SELECT e1 FROM m.g1) AS temp");             //$NON-NLS-1$
    }

    @Test public void testSubquerySetCriteria1() {
        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        ElementSymbol expr = new ElementSymbol("e2"); //$NON-NLS-1$

        SubquerySetCriteria ssc = new SubquerySetCriteria(expr, q1);
        helpTest(ssc, "e2 IN (SELECT e1 FROM m.g1)");             //$NON-NLS-1$
    }

    @Test public void testSubquerySetCriteria2() {
        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        ElementSymbol expr = new ElementSymbol("e2"); //$NON-NLS-1$

        SubquerySetCriteria ssc = new SubquerySetCriteria(expr, q1);
        ssc.setNegated(true);
        helpTest(ssc, "e2 NOT IN (SELECT e1 FROM m.g1)");             //$NON-NLS-1$
    }

    @Test public void testUnaryFromClause() {
        helpTest(new UnaryFromClause(new GroupSymbol("m.g1")), "m.g1");     //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testOptionalUnaryFromClause() {
        UnaryFromClause unaryFromClause = new UnaryFromClause(new GroupSymbol("m.g1"));//$NON-NLS-1$
        unaryFromClause.setOptional(true);
        helpTest(unaryFromClause, "/*+ optional */ m.g1");     //$NON-NLS-1$
    }

    @Test public void testUpdate1() {
        Update update = new Update();
        update.setGroup(new GroupSymbol("m.g1"));     //$NON-NLS-1$
        update.addChange(new ElementSymbol("e1"), new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTest(update, "UPDATE m.g1 SET e1 = 'abc'"); //$NON-NLS-1$
    }

    @Test public void testUpdate2() {
        Update update = new Update();
        update.setGroup(new GroupSymbol("m.g1"));     //$NON-NLS-1$
        update.addChange(new ElementSymbol("e1"), new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$
        update.addChange(new ElementSymbol("e2"), new Constant("xyz")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTest(update, "UPDATE m.g1 SET e1 = 'abc', e2 = 'xyz'"); //$NON-NLS-1$
    }

    @Test public void testUpdate3() {
        Update update = new Update();
        update.setGroup(new GroupSymbol("m.g1"));     //$NON-NLS-1$
        update.addChange(new ElementSymbol("e1"), new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$
        update.setCriteria(new CompareCriteria(
            new ElementSymbol("e2"), //$NON-NLS-1$
            CompareCriteria.EQ,
            new Constant("abc")) ); //$NON-NLS-1$


        helpTest(update, "UPDATE m.g1 SET e1 = 'abc' WHERE e2 = 'abc'"); //$NON-NLS-1$
    }

    @Test public void testAggregateSymbol1() {
        AggregateSymbol agg = new AggregateSymbol(NonReserved.COUNT, false, new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(agg, "COUNT('abc')"); //$NON-NLS-1$
    }

    @Test public void testAggregateSymbol2() {
        AggregateSymbol agg = new AggregateSymbol(NonReserved.COUNT, true, new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(agg, "COUNT(DISTINCT 'abc')"); //$NON-NLS-1$
    }

    @Test public void testAggregateSymbol3() {
        AggregateSymbol agg = new AggregateSymbol(NonReserved.COUNT, false, null); //$NON-NLS-1$
        helpTest(agg, "COUNT(*)"); //$NON-NLS-1$
    }

    @Test public void testAggregateSymbol4() {
        AggregateSymbol agg = new AggregateSymbol(NonReserved.AVG, false, new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(agg, "AVG('abc')"); //$NON-NLS-1$
    }

    @Test public void testAggregateSymbol5() {
        AggregateSymbol agg = new AggregateSymbol(NonReserved.SUM, false, new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(agg, "SUM('abc')"); //$NON-NLS-1$
    }

    @Test public void testAggregateSymbol6() {
        AggregateSymbol agg = new AggregateSymbol(NonReserved.MIN, false, new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(agg, "MIN('abc')"); //$NON-NLS-1$
    }

    @Test public void testAggregateSymbol7() {
        AggregateSymbol agg = new AggregateSymbol(NonReserved.MAX, false, new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(agg, "MAX('abc')"); //$NON-NLS-1$
    }

    @Test public void testAliasSymbol1() {
        AliasSymbol as = new AliasSymbol("x", new ElementSymbol("y")); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(as, "y AS x"); //$NON-NLS-1$
    }

    // Test alias symbol with reserved word
    @Test public void testAliasSymbol2() {
        AliasSymbol as = new AliasSymbol("select", new ElementSymbol("y")); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(as, "y AS \"select\""); //$NON-NLS-1$
    }

    @Test public void testAllSymbol() {
        helpTest(new MultipleElementSymbol(), "*");     //$NON-NLS-1$
    }

    @Test public void testAllInGroupSymbol() {
        helpTest(new MultipleElementSymbol("m.g"), "m.g.*"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConstantNull() {
        helpTest(new Constant(null), "null"); //$NON-NLS-1$
    }

    @Test public void testConstantString() {
        helpTest(new Constant("abc"), "'abc'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConstantInteger() {
        helpTest(new Constant(new Integer(5)), "5"); //$NON-NLS-1$
    }

    @Test public void testConstantBigDecimal() {
        helpTest(new Constant(new BigDecimal("5.4")), "5.4"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConstantStringWithTick() {
        helpTest(new Constant("O'Leary"), "'O''Leary'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConstantStringWithTicks() {
        helpTest(new Constant("'abc'"), "'''abc'''"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConstantStringWithMoreTicks() {
        helpTest(new Constant("a'b'c"), "'a''b''c'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConstantStringWithDoubleTick() {
        helpTest(new Constant("group=\"x\""), "'group=\"x\"'");     //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConstantBooleanTrue() {
        helpTest(new Constant(Boolean.TRUE), "TRUE");     //$NON-NLS-1$
    }

    @Test public void testConstantBooleanFalse() {
        helpTest(new Constant(Boolean.FALSE), "FALSE");     //$NON-NLS-1$
    }

    @Test public void testConstantDate() {
        helpTest(new Constant(java.sql.Date.valueOf("2002-10-02")), "{d'2002-10-02'}");     //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConstantTime() {
        helpTest(new Constant(java.sql.Time.valueOf("5:00:00")), "{t'05:00:00'}");     //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConstantTimestamp() {
        helpTest(new Constant(java.sql.Timestamp.valueOf("2002-10-02 17:10:35.0234")), "{ts'2002-10-02 17:10:35.0234'}");     //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testElementSymbol1() {
        ElementSymbol es = new ElementSymbol("elem"); //$NON-NLS-1$
        helpTest(es, "elem"); //$NON-NLS-1$
    }

    @Test public void testElementSymbol2() {
        ElementSymbol es = new ElementSymbol("elem", false); //$NON-NLS-1$
        es.setGroupSymbol(new GroupSymbol("m.g")); //$NON-NLS-1$
        helpTest(es, "elem"); //$NON-NLS-1$
    }

    @Test public void testElementSymbol3() {
        ElementSymbol es = new ElementSymbol("m.g.elem", true); //$NON-NLS-1$
        es.setGroupSymbol(new GroupSymbol("m.g")); //$NON-NLS-1$
        helpTest(es, "m.g.elem"); //$NON-NLS-1$
    }

    @Test public void testElementSymbol4() {
        ElementSymbol es = new ElementSymbol("vdb.m.g.elem", true); //$NON-NLS-1$
        helpTest(es, "vdb.m.g.elem"); //$NON-NLS-1$
    }

    @Test public void testElementSymbol5() {
        ElementSymbol es = new ElementSymbol("m.g.select", false); //$NON-NLS-1$
        es.setGroupSymbol(new GroupSymbol("m.g")); //$NON-NLS-1$
        helpTest(es, "\"select\"");     //$NON-NLS-1$
    }

    @Test public void testExpressionSymbol1() {
        ExpressionSymbol expr = new ExpressionSymbol("abc", new Constant("abc")); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(expr, "'abc'"); //$NON-NLS-1$
    }

    @Test public void testFunction1() {
        Function func = new Function("concat", new Expression[] { //$NON-NLS-1$
            new Constant("a"), null     //$NON-NLS-1$
        });
        helpTest(func, "concat('a', <undefined>)"); //$NON-NLS-1$
    }

    @Test public void testFunction2() {
        Function func = new Function("now", new Expression[] {}); //$NON-NLS-1$
        helpTest(func, "now()"); //$NON-NLS-1$
    }

    @Test public void testFunction3() {
        Function func = new Function("concat", new Expression[] {null, null}); //$NON-NLS-1$
        helpTest(func, "concat(<undefined>, <undefined>)"); //$NON-NLS-1$
    }

    @Test public void testFunction4() {
        Function func1 = new Function("power", new Expression[] { //$NON-NLS-1$
            new Constant(new Integer(5)),
            new Constant(new Integer(3)) });
        Function func2 = new Function("power", new Expression[] { //$NON-NLS-1$
            func1,
            new Constant(new Integer(3)) });
        Function func3 = new Function("+", new Expression[] { //$NON-NLS-1$
            new Constant(new Integer(1000)),
            func2 });
        helpTest(func3, "(1000 + power(power(5, 3), 3))"); //$NON-NLS-1$
    }

    @Test public void testFunction5() {
        Function func1 = new Function("concat", new Expression[] { //$NON-NLS-1$
            new ElementSymbol("elem2"), //$NON-NLS-1$
            null });
        Function func2 = new Function("concat", new Expression[] { //$NON-NLS-1$
            new ElementSymbol("elem1"), //$NON-NLS-1$
            func1 });
        helpTest(func2, "concat(elem1, concat(elem2, <undefined>))"); //$NON-NLS-1$
    }

    @Test public void testConvertFunction1() {
        Function func = new Function("convert", new Expression[] { //$NON-NLS-1$
            new Constant("5"),  //$NON-NLS-1$
            new Constant("integer")     //$NON-NLS-1$
        });
        helpTest(func, "convert('5', integer)"); //$NON-NLS-1$
    }

    @Test public void testConvertFunction2() {
        Function func = new Function("convert", new Expression[] { //$NON-NLS-1$
            null,
            new Constant("integer")     //$NON-NLS-1$
        });
        helpTest(func, "convert(<undefined>, integer)"); //$NON-NLS-1$
    }

    @Test public void testConvertFunction3() {
        Function func = new Function("convert", new Expression[] { //$NON-NLS-1$
            new Constant(null),
            new Constant("integer")     //$NON-NLS-1$
        });
        helpTest(func, "convert(null, integer)"); //$NON-NLS-1$
    }

    @Test public void testConvertFunction4() {
        Function func = new Function("convert", new Expression[] { //$NON-NLS-1$
            new Constant("abc"),  //$NON-NLS-1$
            null
        });
        helpTest(func, "convert('abc', <undefined>)"); //$NON-NLS-1$
    }

    @Test public void testConvertFunction5() {
        Function func = new Function("convert", null); //$NON-NLS-1$
        helpTest(func, "convert()"); //$NON-NLS-1$
    }

    @Test public void testConvertFunction6() {
        Function func = new Function("convert", new Expression[0]); //$NON-NLS-1$
        helpTest(func, "convert()"); //$NON-NLS-1$
    }

    @Test public void testConvertFunction7() {
        Function func = new Function("convert", new Expression[] {new Constant("abc")}); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(func, "convert('abc', <undefined>)"); //$NON-NLS-1$
    }

    @Test public void testCastFunction1() {
        Function func = new Function("cast", new Expression[] { //$NON-NLS-1$
            new Constant("5"),  //$NON-NLS-1$
            new Constant("integer")     //$NON-NLS-1$
        });
        helpTest(func, "cast('5' AS integer)"); //$NON-NLS-1$
    }

    @Test public void testCastFunction2() {
        Function func = new Function("cast", new Expression[] { //$NON-NLS-1$
            null,
            new Constant("integer")     //$NON-NLS-1$
        });
        helpTest(func, "cast(<undefined> AS integer)"); //$NON-NLS-1$
    }

    @Test public void testCastFunction3() {
        Function func = new Function("cast", new Expression[] { //$NON-NLS-1$
            new Constant(null),
            new Constant("integer")     //$NON-NLS-1$
        });
        helpTest(func, "cast(null AS integer)"); //$NON-NLS-1$
    }

    @Test public void testCastFunction4() {
        Function func = new Function("cast", new Expression[] { //$NON-NLS-1$
            new Constant("abc"),  //$NON-NLS-1$
            null
        });
        helpTest(func, "cast('abc' AS <undefined>)"); //$NON-NLS-1$
    }

    @Test public void testArithemeticFunction1() {
        Function func = new Function("-", new Expression[] {  //$NON-NLS-1$
            new Constant(new Integer(-2)),
            new Constant(new Integer(-1))});
        helpTest(func, "(-2 - -1)");     //$NON-NLS-1$
    }

    @Test public void testGroupSymbol1() {
        GroupSymbol gs = new GroupSymbol("g"); //$NON-NLS-1$
        helpTest(gs, "g"); //$NON-NLS-1$
    }

    @Test public void testGroupSymbol2() {
        GroupSymbol gs = new GroupSymbol("x", "g"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(gs, "g AS x"); //$NON-NLS-1$
    }

    @Test public void testGroupSymbol3() {
        GroupSymbol gs = new GroupSymbol("vdb.g"); //$NON-NLS-1$
        helpTest(gs, "vdb.g"); //$NON-NLS-1$
    }

    @Test public void testGroupSymbol4() {
        GroupSymbol gs = new GroupSymbol("x", "vdb.g"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(gs, "vdb.g AS x"); //$NON-NLS-1$
    }

    @Test public void testGroupSymbol5() {
        GroupSymbol gs = new GroupSymbol("from", "m.g"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(gs, "m.g AS \"from\""); //$NON-NLS-1$
    }

    @Test public void testGroupSymbol6() {
        GroupSymbol gs = new GroupSymbol("x", "on.select"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(gs, "\"on\".\"select\" AS x"); //$NON-NLS-1$
    }

    @Test public void testExecNoParams() {
        StoredProcedure proc = new StoredProcedure();
        proc.setProcedureName("myproc"); //$NON-NLS-1$
        helpTest(proc, "EXEC myproc()"); //$NON-NLS-1$
    }

    @Test public void testExecInputParam() {
        StoredProcedure proc = new StoredProcedure();
        proc.setProcedureName("myproc"); //$NON-NLS-1$
        SPParameter param = new SPParameter(1, new Reference(0));
        proc.setParameter(param);
        helpTest(proc, "EXEC myproc(?)"); //$NON-NLS-1$
    }

    @Test public void testExecInputOutputParam() {
        StoredProcedure proc = new StoredProcedure();
        proc.setProcedureName("myproc"); //$NON-NLS-1$
        SPParameter param1 = new SPParameter(1, new Constant(new Integer(5)));
        param1.setParameterType(ParameterInfo.IN);
        proc.setParameter(param1);

        SPParameter param2 = new SPParameter(2, ParameterInfo.OUT, "x"); //$NON-NLS-1$
        proc.setParameter(param2);

        helpTest(proc, "EXEC myproc(5)"); //$NON-NLS-1$
    }

    @Test public void testExecOutputInputParam() {
        StoredProcedure proc = new StoredProcedure();
        proc.setProcedureName("myproc"); //$NON-NLS-1$

        SPParameter param2 = new SPParameter(2, ParameterInfo.OUT, "x"); //$NON-NLS-1$
        proc.setParameter(param2);

        SPParameter param1 = new SPParameter(1, new Constant(new Integer(5)));
        param1.setParameterType(ParameterInfo.IN);
        proc.setParameter(param1);

        helpTest(proc, "EXEC myproc(5)"); //$NON-NLS-1$
    }

    @Test public void testExecReturnParam() {
        StoredProcedure proc = new StoredProcedure();
        proc.setProcedureName("myproc"); //$NON-NLS-1$

        SPParameter param = new SPParameter(1, ParameterInfo.RETURN_VALUE, "ret"); //$NON-NLS-1$
        proc.setParameter(param);
        helpTest(proc, "EXEC myproc()"); //$NON-NLS-1$
    }

    @Test public void testExecNamedParam() {
        StoredProcedure proc = new StoredProcedure();
        proc.setDisplayNamedParameters(true);
        proc.setProcedureName("myproc"); //$NON-NLS-1$
        SPParameter param = new SPParameter(1, new Reference(0));
        param.setName("p1");//$NON-NLS-1$
        proc.setParameter(param);
        helpTest(proc, "EXEC myproc(p1 => ?)"); //$NON-NLS-1$
    }

    @Test public void testExecNamedParams() {
        StoredProcedure proc = new StoredProcedure();
        proc.setDisplayNamedParameters(true);
        proc.setProcedureName("myproc"); //$NON-NLS-1$
        SPParameter param = new SPParameter(1, new Reference(0));
        param.setName("p1");//$NON-NLS-1$
        proc.setParameter(param);
        SPParameter param2 = new SPParameter(2, new Reference(0));
        param2.setName("p2");//$NON-NLS-1$
        proc.setParameter(param2);
        helpTest(proc, "EXEC myproc(p1 => ?, p2 => ?)"); //$NON-NLS-1$
    }

    /**
     * Test when a parameter's name is a reserved word.
     * (Note: parameters should always have short names, not
     * multiple period-delimited name components.)
     *
     * @since 4.3
     */
    @Test public void testExecNamedParamsReservedWord() {
        StoredProcedure proc = new StoredProcedure();
        proc.setDisplayNamedParameters(true);
        proc.setProcedureName("myproc"); //$NON-NLS-1$
        SPParameter param = new SPParameter(1, new Reference(0));
        param.setName("in");//$NON-NLS-1$
        proc.setParameter(param);
        SPParameter param2 = new SPParameter(2, new Reference(0));
        param2.setName("in2");//$NON-NLS-1$
        proc.setParameter(param2);
        helpTest(proc, "EXEC myproc(\"in\" => ?, in2 => ?)"); //$NON-NLS-1$
    }

    // Test methods for Update Procedure Language Objects

    @Test public void testDeclareStatement() {
        DeclareStatement dclStmt = new DeclareStatement(new ElementSymbol("a"), "String"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(dclStmt, "DECLARE String a;"); //$NON-NLS-1$
    }

    @Test public void testRaiseErrorStatement() {
        RaiseStatement errStmt =    new RaiseStatement(new Constant("My Error")); //$NON-NLS-1$
        helpTest(errStmt, "RAISE 'My Error';"); //$NON-NLS-1$
    }

    @Test public void testRaiseErrorStatementWithExpression() {
        RaiseStatement errStmt =   new RaiseStatement(new ElementSymbol("a")); //$NON-NLS-1$
        helpTest(errStmt, "RAISE a;"); //$NON-NLS-1$
    }

    @Test public void testAssignmentStatement1() {
        AssignmentStatement assigStmt =    new AssignmentStatement(new ElementSymbol("a"), new Constant(new Integer(1))); //$NON-NLS-1$
        helpTest(assigStmt, "a = 1;"); //$NON-NLS-1$
    }

    @Test public void testAssignmentStatement2() {
        Query q1 = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("x"));        //$NON-NLS-1$
        q1.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        q1.setFrom(from);

        AssignmentStatement assigStmt =    new AssignmentStatement(new ElementSymbol("a"), q1); //$NON-NLS-1$
        helpTest(assigStmt, "a = (SELECT x FROM g);"); //$NON-NLS-1$
    }

    @Test public void testCommandStatement1() {
        Query q1 = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("x"));        //$NON-NLS-1$
        q1.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        q1.setFrom(from);

        CommandStatement cmdStmt =    new CommandStatement(q1);
        helpTest(cmdStmt, "SELECT x FROM g;"); //$NON-NLS-1$
    }

    @Test public void testCommandStatement1a() {
        Query q1 = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("x"));        //$NON-NLS-1$
        q1.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        q1.setFrom(from);

        CommandStatement cmdStmt =    new CommandStatement(q1);
        cmdStmt.setReturnable(false);
        helpTest(cmdStmt, "SELECT x FROM g WITHOUT RETURN;"); //$NON-NLS-1$
    }

    @Test public void testCommandStatement2() {
        Delete d1 = new Delete();
        d1.setGroup(new GroupSymbol("g")); //$NON-NLS-1$
        CommandStatement cmdStmt =    new CommandStatement(d1);
        helpTest(cmdStmt, "DELETE FROM g;"); //$NON-NLS-1$
    }

    @Test public void testBlock1() {
        Delete d1 = new Delete();
        d1.setGroup(new GroupSymbol("g")); //$NON-NLS-1$
        CommandStatement cmdStmt =    new CommandStatement(d1);
        AssignmentStatement assigStmt =    new AssignmentStatement(new ElementSymbol("a"), new Constant(new Integer(1))); //$NON-NLS-1$
        RaiseStatement errStmt =    new RaiseStatement(new Constant("My Error")); //$NON-NLS-1$
        Block b = new Block();
        b.addStatement(cmdStmt);
        b.addStatement(assigStmt);
        b.addStatement(errStmt);
        helpTest(b, "BEGIN\nDELETE FROM g;\na = 1;\nRAISE 'My Error';\nEND"); //$NON-NLS-1$
    }

    @Test public void testCreateUpdateProcedure1() {
        Delete d1 = new Delete();
        d1.setGroup(new GroupSymbol("g")); //$NON-NLS-1$
        CommandStatement cmdStmt =    new CommandStatement(d1);
        AssignmentStatement assigStmt =    new AssignmentStatement(new ElementSymbol("a"), new Constant(new Integer(1))); //$NON-NLS-1$
        RaiseStatement errStmt =    new RaiseStatement(new Constant("My Error")); //$NON-NLS-1$
        Block b = new Block();
        b.addStatement(cmdStmt);
        b.addStatement(assigStmt);
        b.addStatement(errStmt);
        CreateProcedureCommand cup = new CreateProcedureCommand(b);
        helpTest(cup, "BEGIN\nDELETE FROM g;\na = 1;\nRAISE 'My Error';\nEND");         //$NON-NLS-1$
    }

    @Test public void testCreateUpdateProcedure2() {
        Delete d1 = new Delete();
        d1.setGroup(new GroupSymbol("g")); //$NON-NLS-1$
        CommandStatement cmdStmt =    new CommandStatement(d1);
        AssignmentStatement assigStmt =    new AssignmentStatement(new ElementSymbol("a"), new Constant(new Integer(1))); //$NON-NLS-1$
        RaiseStatement errStmt =    new RaiseStatement(new Constant("My Error")); //$NON-NLS-1$
        Block b = new Block();
        b.addStatement(cmdStmt);
        b.addStatement(assigStmt);
        b.addStatement(errStmt);
        CreateProcedureCommand cup = new CreateProcedureCommand(b);
        helpTest(cup, "BEGIN\nDELETE FROM g;\na = 1;\nRAISE 'My Error';\nEND");         //$NON-NLS-1$
    }

    @Test public void testCreateUpdateProcedure3() {
        Delete d1 = new Delete();
        d1.setGroup(new GroupSymbol("g")); //$NON-NLS-1$
        CommandStatement cmdStmt =    new CommandStatement(d1);
        AssignmentStatement assigStmt =    new AssignmentStatement(new ElementSymbol("a"), new Constant(new Integer(1))); //$NON-NLS-1$
        RaiseStatement errStmt =    new RaiseStatement(new Constant("My Error")); //$NON-NLS-1$
        Block b = new Block();
        b.addStatement(cmdStmt);
        b.addStatement(assigStmt);
        b.addStatement(errStmt);
        CreateProcedureCommand cup = new CreateProcedureCommand(b);
        helpTest(cup, "BEGIN\nDELETE FROM g;\na = 1;\nRAISE 'My Error';\nEND");         //$NON-NLS-1$
    }

    @Test public void testSubqueryCompareCriteria1() {

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        ElementSymbol expr = new ElementSymbol("e2"); //$NON-NLS-1$

        SubqueryCompareCriteria scc = new SubqueryCompareCriteria(expr, q1, SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.ANY);

        helpTest(scc, "e2 = ANY (SELECT e1 FROM m.g1)");             //$NON-NLS-1$
    }

    @Test public void testSubqueryCompareCriteria2() {

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        ElementSymbol expr = new ElementSymbol("e2"); //$NON-NLS-1$

        SubqueryCompareCriteria scc = new SubqueryCompareCriteria(expr, q1, SubqueryCompareCriteria.LE, SubqueryCompareCriteria.SOME);

        helpTest(scc, "e2 <= SOME (SELECT e1 FROM m.g1)");             //$NON-NLS-1$
    }

    @Test public void testExistsCriteria1() {

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        ExistsCriteria ec = new ExistsCriteria(q1);

        helpTest(ec, "EXISTS (SELECT e1 FROM m.g1)");             //$NON-NLS-1$
    }

    @Test public void testDynamicCommand() {
        List<ElementSymbol> symbols = new ArrayList<ElementSymbol>();

        ElementSymbol a1 = new ElementSymbol("a1"); //$NON-NLS-1$
        a1.setType(DataTypeManager.DefaultDataClasses.STRING);
        symbols.add(a1);

        DynamicCommand obj = new DynamicCommand();
        Expression sql = new Constant("SELECT a1 FROM g WHERE a2 = 5"); //$NON-NLS-1$

        obj.setSql(sql);
        obj.setAsColumns(symbols);
        obj.setAsClauseSet(true);
        obj.setIntoGroup(new GroupSymbol("#g")); //$NON-NLS-1$

        helpTest(obj, "EXECUTE IMMEDIATE 'SELECT a1 FROM g WHERE a2 = 5' AS a1 string INTO #g"); //$NON-NLS-1$
    }

    @Test public void testScalarSubquery() {

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        ScalarSubquery obj = new ScalarSubquery(q1);

        helpTest(obj, "(SELECT e1 FROM m.g1)");             //$NON-NLS-1$
    }

    @Test public void testScalarSubqueryWithHint() {
        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        ScalarSubquery obj = new ScalarSubquery(q1);
        SubqueryHint subqueryHint = new SubqueryHint();
        subqueryHint.setMergeJoin(true);
        obj.setSubqueryHint(subqueryHint);

        helpTest(obj, " /*+ MJ */ (SELECT e1 FROM m.g1)");             //$NON-NLS-1$
    }

    @Test public void testNewSubqueryObjects(){

        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        From f1 = new From();
        f1.addGroup(new GroupSymbol("m.g1"));        //$NON-NLS-1$
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);

        Select s2 = new Select();
        s2.addSymbol(new ElementSymbol("e1")); //$NON-NLS-1$
        s2.addSymbol(new ExpressionSymbol("blargh", new ScalarSubquery(q1))); //$NON-NLS-1$
        From f2 = new From();
        f2.addGroup(new GroupSymbol("m.g2"));        //$NON-NLS-1$
        Criteria left = new SubqueryCompareCriteria(new ElementSymbol("e3"), q1, SubqueryCompareCriteria.GE, SubqueryCompareCriteria.ANY); //$NON-NLS-1$
        Criteria right = new ExistsCriteria(q1);
        Criteria outer = new CompoundCriteria(CompoundCriteria.AND, left, right);
        Query q2 = new Query();
        q2.setSelect(s2);
        q2.setFrom(f2);
        q2.setCriteria(outer);

        helpTest(q2, "SELECT e1, (SELECT e1 FROM m.g1) FROM m.g2 WHERE (e3 >= ANY (SELECT e1 FROM m.g1)) AND (EXISTS (SELECT e1 FROM m.g1))");             //$NON-NLS-1$
    }

    @Test public void testCaseExpression1() {
        helpTest(TestCaseExpression.example(2),
                 "CASE x WHEN 'a' THEN 0 WHEN 'b' THEN 1 ELSE 9999 END"); //$NON-NLS-1$
    }

    @Test public void testCaseExpression2() {
        CaseExpression example = TestCaseExpression.example(2);
        example.setElseExpression(null);
        helpTest(example, "CASE x WHEN 'a' THEN 0 WHEN 'b' THEN 1 END"); //$NON-NLS-1$
    }

    @Test public void testCaseExpression3() {
        CaseExpression example = TestCaseExpression.example(3, 0, true);
        helpTest(example, "CASE x WHEN null THEN 0 WHEN 'b' THEN 1 WHEN 'c' THEN 2 ELSE 9999 END"); //$NON-NLS-1$
    }

    @Test public void testCaseExpression4() {
        CaseExpression example = TestCaseExpression.example(3, 2, true);
        example.setElseExpression(null);
        helpTest(example, "CASE x WHEN 'a' THEN 0 WHEN 'b' THEN 1 WHEN null THEN 2 END"); //$NON-NLS-1$
    }

    @Test public void testSearchedCaseExpression1() {
        helpTest(TestSearchedCaseExpression.example(2),
                 "CASE WHEN x = 0 THEN 0 WHEN x = 1 THEN 1 ELSE 9999 END"); //$NON-NLS-1$

    }

    @Test public void testSearchedCaseExpression2() {
        SearchedCaseExpression example = TestSearchedCaseExpression.example(2);
        example.setElseExpression(null);
        helpTest(example,
                 "CASE WHEN x = 0 THEN 0 WHEN x = 1 THEN 1 END"); //$NON-NLS-1$

    }

    /**
     * For some reason this test was outputting
     * SELECT 'A' AS FOO UNION SELECT 'A' AS FOO
     */
    @Test public void testSetQueryUnionOfLiteralsCase3102() {

        String expected = "SELECT 'A' AS FOO UNION SELECT 'B' AS FOO"; //$NON-NLS-1$

        Select s1 = new Select();
        s1.addSymbol(new AliasSymbol("FOO", new ExpressionSymbol("xxx", new Constant("A")))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Query q1 = new Query();
        q1.setSelect(s1);

        Select s2 = new Select();
        s2.addSymbol(new AliasSymbol("FOO", new ExpressionSymbol("xxx", new Constant("B")))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Query q2 = new Query();
        q2.setSelect(s2);

        SetQuery sq = new SetQuery(Operation.UNION, false, q1, q2);

        helpTest(sq, expected);
    }

    /**
     * For some reason this test was outputting
     * SELECT 'A' AS FOO UNION SELECT 'A' AS FOO
     * Same as above except that ExpressionSymbols' internal names (which aren't visible
     * in the query) are different
     */
    @Test public void testSetQueryUnionOfLiteralsCase3102a() {

        String expected = "SELECT 'A' AS FOO UNION SELECT 'B' AS FOO"; //$NON-NLS-1$

        Select s1 = new Select();
        s1.addSymbol(new AliasSymbol("FOO", new ExpressionSymbol("xxx", new Constant("A")))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Query q1 = new Query();
        q1.setSelect(s1);

        Select s2 = new Select();
        s2.addSymbol(new AliasSymbol("FOO", new ExpressionSymbol("yyy", new Constant("B")))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Query q2 = new Query();
        q2.setSelect(s2);

        SetQuery sq = new SetQuery(Operation.UNION, false, q1, q2);

        helpTest(sq, expected);
    }

    @Test public void testLimit() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new MultipleElementSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(null, new Constant(new Integer(100))));
        helpTest(query, "SELECT * FROM a LIMIT 100"); //$NON-NLS-1$
    }

    @Test public void testLimitWithOffset() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new MultipleElementSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Constant(new Integer(50)), new Constant(new Integer(100))));
        helpTest(query, "SELECT * FROM a LIMIT 50, 100"); //$NON-NLS-1$
    }

    @Test public void testUnionOrderBy() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("select pm1.g1.e1 from pm1.g1 union select e2 from pm1.g2 order by e1"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
        helpTest(command, "SELECT pm1.g1.e1 FROM pm1.g1 UNION SELECT e2 FROM pm1.g2 ORDER BY e1"); //$NON-NLS-1$
    }

    @Test public void testUnionBranchOrderBy() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("select pm1.g1.e1 from pm1.g1 union (select e2 from pm1.g2 order by e1)"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
        helpTest(command, "SELECT pm1.g1.e1 FROM pm1.g1 UNION (SELECT e2 FROM pm1.g2 ORDER BY e1)"); //$NON-NLS-1$
    }

    @Test public void testUnionNesting() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("(select col from t1 union all select col from t2) intersect select col from t4"); //$NON-NLS-1$
        helpTest(command, "(SELECT col FROM t1 UNION ALL SELECT col FROM t2) INTERSECT SELECT col FROM t4"); //$NON-NLS-1$
    }

    @Test public void testUnionNesting1() throws Exception {
        //this is not required as intersect has precedence, but it matches the existing logic
        Command command = QueryParser.getQueryParser().parseCommand("select col from t4 union (select col from t1 intersect select col from t2)"); //$NON-NLS-1$
        helpTest(command, "SELECT col FROM t4 UNION (SELECT col FROM t1 INTERSECT SELECT col FROM t2)"); //$NON-NLS-1$
    }

    @Test public void testAliasedOrderBy() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("select pm1.g1.e1 as a from pm1.g1 order by a"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
        helpTest(command, "SELECT pm1.g1.e1 AS a FROM pm1.g1 ORDER BY a"); //$NON-NLS-1$
    }

    @Test public void testNumberOrderBy() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("select pm1.g1.e1 as a from pm1.g1 order by 1"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
        helpTest(command, "SELECT pm1.g1.e1 AS a FROM pm1.g1 ORDER BY 1"); //$NON-NLS-1$
    }

    public Expression helpTestExpression(String sql, String expected) throws QueryParserException {
        Expression expr = QueryParser.getQueryParser().parseExpression(sql);
        helpTest(expr, expected);
        return expr;
    }

    @Test public void testLikeRegex() throws Exception {
        helpTestExpression("x like_regex 'b'", "x LIKE_REGEX 'b'");
    }

    @Test public void testSimilar() throws Exception {
        helpTestExpression("x similar to 'b' escape 'c'", "x SIMILAR TO 'b' ESCAPE 'c'");
    }

    @Test public void testTextTable() throws Exception {
        String sql = "SELECT * from texttable(file columns y for ordinality, x string WIDTH 1 NO TRIM NO ROW DELIMITER) as x"; //$NON-NLS-1$
        helpTest(QueryParser.getQueryParser().parseCommand(sql), "SELECT * FROM TEXTTABLE(file COLUMNS y FOR ORDINALITY, x string WIDTH 1 NO TRIM NO ROW DELIMITER) AS x");
    }

    @Test public void testArray() {
        Array array = new Array(TypeFacility.RUNTIME_TYPES.INTEGER, Arrays.asList(new ElementSymbol("e1"), new Constant(1)));
        helpTest(array, "(e1, 1)");             //$NON-NLS-1$
    }

    @Test public void testReturnStatement() throws QueryParserException {
        helpTest(QueryParser.getQueryParser().parseProcedure("begin if (true) return 1; return; end", false), "BEGIN\nIF(TRUE)\nBEGIN\nRETURN 1;\nEND\nRETURN;\nEND");
    }

    @Test public void testConditionNesting() throws Exception {
        String sql = "select (intkey = intnum) is null, (intkey < intnum) in (true, false) from bqt1.smalla";

        helpTest(QueryParser.getQueryParser().parseCommand(sql), "SELECT (intkey = intnum) IS NULL, (intkey < intnum) IN (TRUE, FALSE) FROM bqt1.smalla"); //$NON-NLS-1$
    }

    @Test public void testSubqueryNameEscaping() throws Exception {
        helpTest(new SubqueryFromClause("user", QueryParser.getQueryParser() .parseCommand("select 1")), "(SELECT 1) AS \"user\"");
    }

    @Test public void testEscaping() throws Exception {
        String sql = "select 'a\\u0000\u0001b''c''d\u0002e\u0003f''' from TEXTTABLE(x COLUMNS y string ESCAPE '\u0000' HEADER) AS A";

        helpTest(QueryParser.getQueryParser().parseCommand(sql), "SELECT 'a\\u0000\\u0001b''c''d\\u0002e\\u0003f''' FROM TEXTTABLE(x COLUMNS y string ESCAPE '\\u0000' HEADER) AS A"); //$NON-NLS-1$
    }

    @Test public void testNestedComparison() {
        CompareCriteria cc = new CompareCriteria(
                new ElementSymbol("m.g.c1"), //$NON-NLS-1$
                CompareCriteria.EQ,
                new Constant("abc") ); //$NON-NLS-1$

        CompareCriteria cc1 = new CompareCriteria(
                cc, //$NON-NLS-1$
                CompareCriteria.EQ,
                new Constant(false) ); //$NON-NLS-1$

        helpTest(cc1, "(m.g.c1 = 'abc') = FALSE"); //$NON-NLS-1$

        cc1.setLeftExpression(new CompoundCriteria(Arrays.asList(cc, new CompareCriteria(
                new ElementSymbol("m.g.c2"), //$NON-NLS-1$
                CompareCriteria.GT,
                new Constant(1)))));

        helpTest(cc1, "((m.g.c1 = 'abc') AND (m.g.c2 > 1)) = FALSE"); //$NON-NLS-1$
    }

    @Test public void testCurrentDate() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("select current_date()"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
        helpTest(command, "SELECT current_date()"); //$NON-NLS-1$
    }

    @Test public void testCurrentDateNoParens() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("select current_date"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
        helpTest(command, "SELECT current_date()"); //$NON-NLS-1$
    }

    @Test public void testPosition() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("select position('a' in e1) from pm1.g1"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
        helpTest(command, "SELECT locate('a', e1) FROM pm1.g1"); //$NON-NLS-1$
    }

    @Test public void testCurrentTime() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("select current_time, current_time(1)"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
        helpTest(command, "SELECT current_time, current_time(1)"); //$NON-NLS-1$
    }

    @Test public void testCurrentTimestamp() throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand("select current_timestamp, current_timestamp(1)"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
        helpTest(command, "SELECT current_timestamp, current_timestamp(1)"); //$NON-NLS-1$
    }

    @Test public void testWindowFunctionOver() throws Exception {
        String input = "select nth_value(e1, 2) over (partition by e2 order by e1 range between unbounded preceding and 1 following) FROM pm1.g1"; //$NON-NLS-1$
        String output = "SELECT nth_value(e1, 2) OVER (PARTITION BY e2 ORDER BY e1 RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM pm1.g1"; //$NON-NLS-1$

        Command command = QueryParser.getQueryParser().parseCommand(input);
        QueryResolver.resolveCommand(command, RealMetadataFactory.example1Cached());
        helpTest(command, output);
    }

}
