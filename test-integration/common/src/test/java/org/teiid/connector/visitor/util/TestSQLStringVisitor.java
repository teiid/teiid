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

package org.teiid.connector.visitor.util;

import static org.junit.Assert.*;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.dqp.internal.datamgr.TestAggregateImpl;
import org.teiid.dqp.internal.datamgr.TestCompareCriteriaImpl;
import org.teiid.dqp.internal.datamgr.TestDeleteImpl;
import org.teiid.dqp.internal.datamgr.TestElementImpl;
import org.teiid.dqp.internal.datamgr.TestExistsCriteriaImpl;
import org.teiid.dqp.internal.datamgr.TestFunctionImpl;
import org.teiid.dqp.internal.datamgr.TestGroupByImpl;
import org.teiid.dqp.internal.datamgr.TestGroupImpl;
import org.teiid.dqp.internal.datamgr.TestInCriteriaImpl;
import org.teiid.dqp.internal.datamgr.TestInsertImpl;
import org.teiid.dqp.internal.datamgr.TestIsNullCriteriaImpl;
import org.teiid.dqp.internal.datamgr.TestJoinImpl;
import org.teiid.dqp.internal.datamgr.TestLikeCriteriaImpl;
import org.teiid.dqp.internal.datamgr.TestLiteralImpl;
import org.teiid.dqp.internal.datamgr.TestNotCriteriaImpl;
import org.teiid.dqp.internal.datamgr.TestOrderByImpl;
import org.teiid.dqp.internal.datamgr.TestProcedureImpl;
import org.teiid.dqp.internal.datamgr.TestQueryImpl;
import org.teiid.dqp.internal.datamgr.TestScalarSubqueryImpl;
import org.teiid.dqp.internal.datamgr.TestSearchedCaseExpressionImpl;
import org.teiid.dqp.internal.datamgr.TestSelectSymbolImpl;
import org.teiid.dqp.internal.datamgr.TestSetQueryImpl;
import org.teiid.dqp.internal.datamgr.TestSubqueryCompareCriteriaImpl;
import org.teiid.dqp.internal.datamgr.TestSubqueryInCriteriaImpl;
import org.teiid.dqp.internal.datamgr.TestUpdateImpl;
import org.teiid.dqp.internal.datamgr.TstLanguageBridgeFactory;
import org.teiid.language.AggregateFunction;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Insert;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.language.Select;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.JoinType;


public class TestSQLStringVisitor  {

    public static final RuntimeMetadata metadata = TstLanguageBridgeFactory.metadataFactory;
        
    private String getString(LanguageObject obj) {
        return SQLStringVisitor.getSQLString(obj);
    }
    
    /*
     * Test for void visit(IAggregate)
     */
    @Test public void testVisitIAggregate() throws Exception {
        String expected = "COUNT(42)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestAggregateImpl.example("COUNT", NonReserved.COUNT, false, 42))); //$NON-NLS-1$
    }

    @Test public void testVisitIAggregateDistinct() throws Exception {
        String expected = "COUNT(DISTINCT *)"; //$NON-NLS-1$
        AggregateFunction impl = new AggregateFunction("COUNT", true, null, Integer.class); //$NON-NLS-1$
        assertEquals(expected, getString(impl)); 
    }

    /*
     * Test for void visit(ICompareCriteria)
     */
    @Test public void testVisitICompareCriteria() throws Exception {
        String expected = "200 = 100"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCompareCriteriaImpl.example(CompareCriteria.EQ, 200, 100)));
    }

    /*
     * Test for void visit(ICompoundCriteria)
     */
    @Test public void testVisitICompoundCriteria() throws Exception {
        String expected = "200 = 100"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCompareCriteriaImpl.example(CompareCriteria.EQ, 200, 100)));
        expected = "200 >= 100"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCompareCriteriaImpl.example(CompareCriteria.GE, 200, 100)));
        expected = "200 > 100"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCompareCriteriaImpl.example(CompareCriteria.GT, 200, 100)));
        expected = "200 <= 100"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCompareCriteriaImpl.example(CompareCriteria.LE, 200, 100)));
        expected = "200 < 100"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCompareCriteriaImpl.example(CompareCriteria.LT, 200, 100)));
        expected = "200 <> 100"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCompareCriteriaImpl.example(CompareCriteria.NE, 200, 100)));
    }

    /*
     * Test for void visit(IDelete)
     */
    @Test public void testVisitIDelete() throws Exception {
        String expected = "DELETE FROM g1 WHERE 100 >= 200 AND 500 < 600"; //$NON-NLS-1$
        assertEquals(expected, getString(TestDeleteImpl.example()));
    }
    
    /*
     * Test for void visit(IElement)
     */
    @Test public void testVisitIElement() throws Exception {
        String expected = "g1.e1"; //$NON-NLS-1$
        assertEquals(expected, getString(TestElementImpl.example("vm1.g1", "e1"))); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /*
     * Test for void visit(IExecute)
     */
//    @Test public void testVisitIExecute() throws Exception {
//        String expected = "EXEC pm1.sq3('x', 1)"; //$NON-NLS-1$
//        assertEquals(expected, getString(TestProcedureImpl.example()));
//    }

    /*
     * Test for void visit(IExistsCriteria)
     */
    @Test public void testVisitIExistsCriteria() throws Exception {
        String expected = "EXISTS (SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestExistsCriteriaImpl.example()));
    }

    /*
     * Test for void visit(IFunction)
     */
    @Test public void testVisitIFunction() throws Exception {
        // TODO more thorough testing needed for built-in operators
        String expected = "testName(100, 200)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestFunctionImpl.example("testName"))); //$NON-NLS-1$
    }
    
    @Test public void testVisitConvertFunctionOracleStyle() throws Exception {
        String expected = "convert(columnA, integer)"; //$NON-NLS-1$
        
        List<? extends Expression> params = Arrays.asList(new ColumnReference(null, "columnA", null, String.class), new Literal("integer", String.class));
        Function test = new Function("convert", params, Integer.class); //$NON-NLS-1$
        
        assertEquals(expected, getString(test)); 
    }

    /*
     * Test for void visit(IGroup)
     */
    @Test public void testVisitIGroup() throws Exception {
        String expected = "g1 AS alias"; //$NON-NLS-1$
        assertEquals(expected, getString(TestGroupImpl.example("alias", "vm1.g1"))); //$NON-NLS-1$ //$NON-NLS-2$
        expected = "g1"; //$NON-NLS-1$
        assertEquals(expected, getString(TestGroupImpl.example("vm1.g1"))); //$NON-NLS-1$
    }

    /*
     * Test for void visit(IGroupBy)
     */
    @Test public void testVisitIGroupBy() throws Exception {
        String expected = "GROUP BY g1.e1, g1.e2, g1.e3, g1.e4"; //$NON-NLS-1$
        assertEquals(expected, getString(TestGroupByImpl.example()));
    }

    /*
     * Test for void visit(IInCriteria)
     */
    @Test public void testVisitIInCriteria() throws Exception {
        String expected = "300 IN (100, 200, 300, 400)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestInCriteriaImpl.example(false)));
        expected = "300 NOT IN (100, 200, 300, 400)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestInCriteriaImpl.example(true)));
    }

    /*
     * Test for void visit(IInsert)
     */
    @Test public void testVisitIInsert() throws Exception {
        String expected = "INSERT INTO g1 (e1, e2, e3, e4) VALUES (1, 2, 3, 4)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestInsertImpl.example("g1"))); //$NON-NLS-1$
    }
  
    /*
     * Test for void visit(IIsNullCriteria)
     */
    @Test public void testVisitIIsNullCriteria() throws Exception {
        String expected = "g1.e1 IS NULL"; //$NON-NLS-1$
        assertEquals(expected, getString(TestIsNullCriteriaImpl.example(false)));
        expected = "g1.e1 IS NOT NULL"; //$NON-NLS-1$
        assertEquals(expected, getString(TestIsNullCriteriaImpl.example(true)));
    }

    /*
     * Test for void visit(IJoin)
     */
    @Test public void testVisitIJoin() throws Exception {
        String expected = "g1 CROSS JOIN g2 ON g1.e1 = g2.e1"; //$NON-NLS-1$
        assertEquals(expected, getString(TestJoinImpl.example(JoinType.JOIN_CROSS)));
        expected = "g1 FULL OUTER JOIN g2 ON g1.e1 = g2.e1"; //$NON-NLS-1$
        assertEquals(expected, getString(TestJoinImpl.example(JoinType.JOIN_FULL_OUTER)));
        expected = "g1 INNER JOIN g2 ON g1.e1 = g2.e1"; //$NON-NLS-1$
        assertEquals(expected, getString(TestJoinImpl.example(JoinType.JOIN_INNER)));
        expected = "g1 LEFT OUTER JOIN g2 ON g1.e1 = g2.e1"; //$NON-NLS-1$
        assertEquals(expected, getString(TestJoinImpl.example(JoinType.JOIN_LEFT_OUTER)));
        expected = "g1 RIGHT OUTER JOIN g2 ON g1.e1 = g2.e1"; //$NON-NLS-1$
        assertEquals(expected, getString(TestJoinImpl.example(JoinType.JOIN_RIGHT_OUTER)));
    }

    /*
     * Test for void visit(ILikeCriteria)
     */
    @Test public void testVisitILikeCriteria() throws Exception {
        String expected = "g1.e1 LIKE 'likeString' ESCAPE '\\'"; //$NON-NLS-1$
        assertEquals(expected, getString(TestLikeCriteriaImpl.example("likeString", '\\', false))); //$NON-NLS-1$
        expected = "g1.e1 NOT LIKE 'likeString' ESCAPE '\\'"; //$NON-NLS-1$
        assertEquals(expected, getString(TestLikeCriteriaImpl.example("likeString", '\\', true))); //$NON-NLS-1$
    }

    /*
     * Test for void visit(ILiteral)
     */
    @Test public void testVisitILiteral() throws Exception {
        String expected = "'string''Literal'"; //$NON-NLS-1$
        assertEquals(expected, getString(TestLiteralImpl.example("string'Literal"))); //$NON-NLS-1$
        expected = "1000"; //$NON-NLS-1$
        assertEquals(expected, getString(TestLiteralImpl.example(new Integer(1000))));
        expected = "TRUE"; //$NON-NLS-1$
        assertEquals(expected, getString(TestLiteralImpl.example(Boolean.TRUE)));
        long now = System.currentTimeMillis();
        Date date = new Date(now);
        expected = "{d '" + date.toString() + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(expected, getString(TestLiteralImpl.example(date)));
        Timestamp ts = new Timestamp(now);
        expected = "{ts '" + ts.toString() + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(expected, getString(TestLiteralImpl.example(ts)));
        Time t = new Time(now);
        expected = "{t '" + t.toString() + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(expected, getString(TestLiteralImpl.example(t)));
    }

    /*
     * Test for void visit(INotCriteria)
     */
    @Test public void testVisitINotCriteria() throws Exception {
        String expected = "NOT (100 >= 200)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestNotCriteriaImpl.example()));
    }

    /*
     * Test for void visit(IOrderBy)
     */
    @Test public void testVisitIOrderBy() throws Exception {
        String expected = "ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC"; //$NON-NLS-1$
        assertEquals(expected, getString(TestOrderByImpl.example()));
    }

    /*
     * Test for void visit(IParameter)
     */
//    @Test public void testVisitIParameter() throws Exception {
//        String expected = "x"; //$NON-NLS-1$
//        assertEquals(expected, getString(TestParameterImpl.example(1)));
//    }

    /*
     * Test for void visit(IQuery)
     */
    @Test public void testVisitIQuery() throws Exception {
        String expected = "SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC"; //$NON-NLS-1$
        assertEquals(expected, getString(TestQueryImpl.example(true)));
    }

    /*
     * Test for void visit(IScalarSubquery)
     */
    @Test public void testVisitIScalarSubquery() throws Exception {
        String expected = "(SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestScalarSubqueryImpl.example()));
    }

    /*
     * Test for void visit(ISearchedCaseExpression)
     */
    @Test public void testVisitISearchedCaseExpression() throws Exception {
        String expected = "CASE WHEN g1.e1 = 0 THEN 0 WHEN g1.e1 = 1 THEN 1 WHEN g1.e1 = 2 THEN 2 ELSE 9999 END"; //$NON-NLS-1$
        assertEquals(expected, getString(TestSearchedCaseExpressionImpl.example()));
    }

    /*
     * Test for void visit(ISelect)
     */
    @Test public void testVisitISelect() throws Exception {
        String expected = "SELECT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC"; //$NON-NLS-1$
        assertEquals(expected, getString(TestQueryImpl.example(false)));
        expected = "SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC"; //$NON-NLS-1$
        assertEquals(expected, getString(TestQueryImpl.example(true)));
    }

    
    /*
     * Test for void visit(ISelectSymbol)
     */
    @Test public void testVisitISelectSymbol() throws Exception {
        String expected = "e1"; //$NON-NLS-1$
        assertEquals(expected, getString(TestSelectSymbolImpl.example("e1", null))); //$NON-NLS-1$
        expected = "e1 AS alias"; //$NON-NLS-1$
        assertEquals(expected, getString(TestSelectSymbolImpl.example("e1", "alias"))); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /*
     * Test for void visit(ISubqueryCompareCriteria)
     */
    @Test public void testVisitISubqueryCompareCriteria() throws Exception {
        String expected = "g1.e1 > SOME (SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestSubqueryCompareCriteriaImpl.example())); 
    }

    /*
     * Test for void visit(ISubqueryInCriteria)
     */
    @Test public void testVisitISubqueryInCriteria() throws Exception {
        String expected = "g1.e1 NOT IN (SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestSubqueryInCriteriaImpl.example())); 
    }

    @Test public void testVisitIUnion1() throws Exception {
        String expected = "SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC UNION (SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC) ORDER BY e1, e2 DESC, e3, e4 DESC";//$NON-NLS-1$
        assertEquals(expected, getString(TestSetQueryImpl.example()));
    }
    
    @Test public void testVisitIUnion2() throws Exception {
        String expected = "SELECT ted.nugent FROM ted UNION ALL SELECT dave.barry FROM dave";//$NON-NLS-1$
        String actual = getString(TestSetQueryImpl.example2());
        assertEquals(expected, actual);
    }

    @Test public void testVisitIUnion3() throws Exception {
        String expected = "SELECT ted.nugent FROM ted UNION ALL SELECT dave.barry FROM dave ORDER BY nugent";//$NON-NLS-1$
        String actual = getString(TestSetQueryImpl.example3());
        assertEquals(expected, actual);
    }
    
    /*
     * Test for void visit(IUpdate)
     */
    @Test public void testVisitIUpdate() throws Exception {
        String expected = "UPDATE g1 SET e1 = 1, e2 = 1, e3 = 1, e4 = 1 WHERE 1 = 1"; //$NON-NLS-1$
        assertEquals(expected, getString(TestUpdateImpl.example()));
    }
    
    @Test public void testVisitProcedure() throws Exception {
        String expected = "EXEC sq3('x', 1)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestProcedureImpl.example()));
    }
    
    @Test public void testTimestampAddFunction() throws Exception {
    	String sql = "select timestampadd(" +NonReserved.SQL_TSI_DAY+ ", 2, timestampvalue) from bqt1.smalla"; //$NON-NLS-1$ //$NON-NLS-2$
    	
    	Command command = FakeTranslationFactory.getInstance().getBQTTranslationUtility().parseCommand(sql);
    	assertEquals("SELECT timestampadd(SQL_TSI_DAY, 2, SmallA.TimestampValue) FROM SmallA", command.toString()); //$NON-NLS-1$
    }
    
    @Test public void testInsertWithQuery() throws Exception {
    	String sql = "insert into pm1.g1 values (null, null, null, null)"; //$NON-NLS-1$

    	Insert insert = (Insert)FakeTranslationFactory.getInstance().getExampleTranslationUtility().parseCommand(sql); 
    	
    	Select command = (Select)FakeTranslationFactory.getInstance().getExampleTranslationUtility().parseCommand("select * from pm1.g2"); //$NON-NLS-1$
    	insert.setValueSource(command);
    	assertEquals("INSERT INTO g1 (e1, e2, e3, e4) SELECT g2.e1, g2.e2, g2.e3, g2.e4 FROM g2", insert.toString()); //$NON-NLS-1$
    }
    
    @Test public void testUnrelatedOrderBy() throws Exception {
    	String sql = "select intkey from bqt1.smalla order by stringkey"; //$NON-NLS-1$ 
    	
    	Command command = FakeTranslationFactory.getInstance().getBQTTranslationUtility().parseCommand(sql, true, true);
    	assertEquals("SELECT g_0.IntKey AS c_0 FROM SmallA AS g_0 ORDER BY g_0.StringKey", command.toString()); //$NON-NLS-1$
    }
    
    @Test public void testOrderByDerivedColumn() throws Exception {
    	String sql = "select intkey as x from bqt1.smalla order by intkey"; //$NON-NLS-1$ 
    	
    	Command command = FakeTranslationFactory.getInstance().getBQTTranslationUtility().parseCommand(sql, true, true);
    	assertEquals("SELECT g_0.IntKey AS c_0 FROM SmallA AS g_0 ORDER BY c_0", command.toString()); //$NON-NLS-1$
    }
    
    @Test public void testOrderByAlias() throws Exception {
    	String sql = "select intkey as x from bqt1.smalla order by x"; //$NON-NLS-1$ 
    	
    	Command command = FakeTranslationFactory.getInstance().getBQTTranslationUtility().parseCommand(sql, true, true);
    	assertEquals("SELECT g_0.IntKey AS c_0 FROM SmallA AS g_0 ORDER BY c_0", command.toString()); //$NON-NLS-1$
    }
    
    @Test public void testOrderByNullOrdering() throws Exception {
    	String sql = "select intkey as x from bqt1.smalla order by x nulls first"; //$NON-NLS-1$ 
    	
    	Command command = FakeTranslationFactory.getInstance().getBQTTranslationUtility().parseCommand(sql, true, true);
    	assertEquals("SELECT g_0.IntKey AS c_0 FROM SmallA AS g_0 ORDER BY c_0 NULLS FIRST", command.toString()); //$NON-NLS-1$
    }

}
