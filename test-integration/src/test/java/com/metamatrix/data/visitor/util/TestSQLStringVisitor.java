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

package com.metamatrix.data.visitor.util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import junit.framework.TestCase;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.connector.language.IElement;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.IGroup;
import com.metamatrix.connector.language.ILanguageObject;
import com.metamatrix.connector.metadata.runtime.MetadataID;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
import com.metamatrix.connector.visitor.util.SQLStringVisitor;
import com.metamatrix.dqp.internal.datamgr.language.AggregateImpl;
import com.metamatrix.dqp.internal.datamgr.language.ElementImpl;
import com.metamatrix.dqp.internal.datamgr.language.FunctionImpl;
import com.metamatrix.dqp.internal.datamgr.language.GroupImpl;
import com.metamatrix.dqp.internal.datamgr.language.LiteralImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestAggregateImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestCaseExpressionImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestCompareCriteriaImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestDeleteImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestElementImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestExistsCriteriaImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestFromImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestFunctionImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestGroupByImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestGroupImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestInCriteriaImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestInsertImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestIsNullCriteriaImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestJoinImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestLikeCriteriaImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestLiteralImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestNotCriteriaImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestOrderByImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestProcedureImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestQueryImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestScalarSubqueryImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestSearchedCaseExpressionImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestSelectImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestSelectSymbolImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestSubqueryCompareCriteriaImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestSubqueryInCriteriaImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestSetQueryImpl;
import com.metamatrix.dqp.internal.datamgr.language.TestUpdateImpl;
import com.metamatrix.dqp.internal.datamgr.language.TstLanguageBridgeFactory;
import com.metamatrix.dqp.internal.datamgr.metadata.MetadataFactory;
import com.metamatrix.dqp.internal.datamgr.metadata.RuntimeMetadataImpl;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;
import com.metamatrix.query.unittest.FakeMetadataStore;

public class TestSQLStringVisitor extends TestCase {

    public static final RuntimeMetadata metadata = TstLanguageBridgeFactory.metadataFactory.createRuntimeMetadata();
        
  
    /**
     * Constructor for TestSQLStringVisitor.
     * @param name
     */
    public TestSQLStringVisitor(String name) {
        super(name);
    }

    private String getString(ILanguageObject obj) {
        return SQLStringVisitor.getSQLString(obj, metadata);
    }
    
    private String getString(ILanguageObject obj, RuntimeMetadata metadata) {
        return SQLStringVisitor.getSQLString(obj, metadata);
    }
        
    /** create fake BQT metadata to test this case, name in source is important */
    private RuntimeMetadata exampleRuntimeMetadata(QueryMetadataInterface metadata) {  
        return new RuntimeMetadataImpl(new MetadataFactory(metadata));
    }
    
    /** create fake BQT metadata to test this case, name in source is important */
    private FakeMetadataStore exampleMetadataStore() {  
        // Create models
        FakeMetadataObject bqt1 = FakeMetadataFactory.createPhysicalModel("BQT1"); //$NON-NLS-1$
        FakeMetadataObject bqt1SmallA = FakeMetadataFactory.createPhysicalGroup("BQT1.SmallA", bqt1); //$NON-NLS-1$
        bqt1SmallA.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, "SmallishA");//$NON-NLS-1$
        FakeMetadataObject doubleNum = FakeMetadataFactory.createElement("DoubleNum", bqt1SmallA, DataTypeManager.DefaultDataTypes.DOUBLE, 0); //$NON-NLS-1$
        doubleNum.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, "doublishNum");//$NON-NLS-1$

        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(bqt1);
        store.addObject(bqt1SmallA);
        store.addObject(doubleNum);
        return store;
    }

    /*
     * Test for void visit(IAggregate)
     */
    public void testVisitIAggregate() throws Exception {
        String expected = "COUNT(42)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestAggregateImpl.example("COUNT", ReservedWords.COUNT, false, 42))); //$NON-NLS-1$
    }

    public void testVisitIAggregateDistinct() throws Exception {
        String expected = "COUNT(DISTINCT *)"; //$NON-NLS-1$
        AggregateImpl impl = new AggregateImpl("COUNT", true, null, Integer.class); //$NON-NLS-1$
        assertEquals(expected, getString(impl)); 
    }

    /*
     * Test for void visit(ICaseExpression)
     */
    public void testVisitICaseExpression() throws Exception {
        String expected = "CASE WHEN g1.e1='a' THEN 0 WHEN g1.e1='b' THEN 1 WHEN g1.e1='c' THEN 2 ELSE 9999 END"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCaseExpressionImpl.example()));
    }

    /*
     * Test for void visit(ICaseExpression)
     */
    public void testVisitICaseExpressionNulFirst() throws Exception {
        String expected = "CASE WHEN g1.e1 IS NULL THEN 0 WHEN g1.e1='b' THEN 1 WHEN g1.e1='c' THEN 2 ELSE 9999 END"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCaseExpressionImpl.exampleNullFirst()));
    }

    /*
     * Test for void visit(ICaseExpression)
     */
    public void testVisitICaseExpressionNullMiddle() throws Exception {
        String expected = "CASE WHEN g1.e1 IS NULL THEN 1 WHEN g1.e1='a' THEN 0 WHEN g1.e1='c' THEN 2 ELSE 9999 END"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCaseExpressionImpl.exampleNullMiddle()));
    }
    
    /*
     * Test for void visit(ICaseExpression)
     */
    public void testVisitICaseExpressionNullLast() throws Exception {
        String expected = "CASE WHEN g1.e1 IS NULL THEN 2 WHEN g1.e1='a' THEN 0 WHEN g1.e1='b' THEN 1 ELSE 9999 END"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCaseExpressionImpl.exampleNullLast()));
    }
    
    /*
     * Test for void visit(ICaseExpression)
     */
    public void testVisitICaseExpressionNullElse() throws Exception {
        String expected = "CASE WHEN g1.e1='a' THEN 0 WHEN g1.e1='b' THEN 1 WHEN g1.e1='c' THEN 2 ELSE g1.e1 END"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCaseExpressionImpl.exampleElementElse() ) );
    }
    
    /*
     * Test for void visit(ICaseExpression)
     */
    public void testVisitICaseExpressionInteger() throws Exception {
        String expected = "CASE WHEN g1.e1='a' THEN 0 WHEN g1.e1='b' THEN 1 WHEN g1.e1='c' THEN 2 ELSE g1.e1 END"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCaseExpressionImpl.exampleInteger() ) );
    }
    
    /*
     * Test for void visit(ICompareCriteria)
     */
    public void testVisitICompareCriteria() throws Exception {
        String expected = "200 = 100"; //$NON-NLS-1$
        assertEquals(expected, getString(TestCompareCriteriaImpl.example(CompareCriteria.EQ, 200, 100)));
    }

    /*
     * Test for void visit(ICompoundCriteria)
     */
    public void testVisitICompoundCriteria() throws Exception {
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
    public void testVisitIDelete() throws Exception {
        String expected = "DELETE FROM g1 WHERE (100 >= 200) AND (500 < 600)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestDeleteImpl.example()));
    }
    
    /*
     * Test for void visit(IElement)
     */
    public void testVisitIElement() throws Exception {
        String expected = "g1.e1"; //$NON-NLS-1$
        assertEquals(expected, getString(TestElementImpl.example("vm1.g1", "e1"))); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /*
     * Test for void visit(IExecute)
     */
//    public void testVisitIExecute() throws Exception {
//        String expected = "EXEC pm1.sq3('x', 1)"; //$NON-NLS-1$
//        assertEquals(expected, getString(TestProcedureImpl.example()));
//    }

    /*
     * Test for void visit(IExistsCriteria)
     */
    public void testVisitIExistsCriteria() throws Exception {
        String expected = "EXISTS (SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE (100 >= 200) AND (500 < 600) GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING (100 >= 200) AND (500 < 600) ORDER BY e1, e2 DESC, e3, e4 DESC)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestExistsCriteriaImpl.example()));
    }

    /*
     * Test for void visit(IFrom)
     */
    public void testVisitIFrom() throws Exception {
        String expected = "FROM g1, g2 AS myAlias, g3, g4"; //$NON-NLS-1$
        assertEquals(expected, getString(TestFromImpl.example()));
    }

    /*
     * Test for void visit(IFunction)
     */
    public void testVisitIFunction() throws Exception {
        // TODO more thorough testing needed for built-in operators
        String expected = "testName(100, 200)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestFunctionImpl.example("testName"))); //$NON-NLS-1$
    }
    
    public void testVisitConvertFunctionOracleStyleWithNIS() throws Exception {
        
        FakeMetadataFacade facade = new FakeMetadataFacade(exampleMetadataStore());
        RuntimeMetadata metadata = exampleRuntimeMetadata(facade);
        MetadataFactory metadataFactory = new MetadataFactory(facade);

        IExpression [] params = null;
        params = new IExpression[2];
        IGroup g = new GroupImpl("SmallA", null, metadataFactory.createMetadataID(facade.getStore().findObject("BQT1.SmallA", FakeMetadataObject.GROUP), MetadataID.TYPE_GROUP)); //$NON-NLS-1$
        IElement e = new ElementImpl(g, "DoubleNum", metadataFactory.createMetadataID(facade.getStore().findObject("DoubleNum", FakeMetadataObject.ELEMENT), MetadataID.TYPE_ELEMENT), Double.class); //$NON-NLS-1$ //$NON-NLS-2$
        params[0] = e;
        params[1] = new LiteralImpl("integer", String.class); //$NON-NLS-1$

        
        
        final String expected = "convert(SmallishA.doublishNum, integer)"; //$NON-NLS-1$
        IFunction test = new FunctionImpl("convert", params, Integer.class); //$NON-NLS-1$
        
        assertEquals(expected, getString(test, metadata  )); 
    }
    
    public void testVisitConvertFunctionOracleStyle() throws Exception {
        String expected = "convert(columnA, integer)"; //$NON-NLS-1$
        
        IExpression [] params = null;
        params = new IExpression[2];
        params[0] = new ElementImpl(null, "columnA", null, String.class); //$NON-NLS-1$
        params[1] = new LiteralImpl("integer", String.class); //$NON-NLS-1$
        IFunction test = new FunctionImpl("convert", params, Integer.class); //$NON-NLS-1$
        
        assertEquals(expected, getString(test)); 
    }

    public void testVisitConvertFunctionSQLServerStyle() throws Exception {
        String expected = "convert(integer, columnA)"; //$NON-NLS-1$
        
        IExpression [] params = null;
        params = new IExpression[2];
        params[0] = new LiteralImpl("integer", String.class); //$NON-NLS-1$
        params[1] = new ElementImpl(null, "columnA", null, String.class); //$NON-NLS-1$
        IFunction test = new FunctionImpl("convert", params, Integer.class); //$NON-NLS-1$
        
        assertEquals(expected, getString(test)); 
        
    }

    /*
     * Test for void visit(IGroup)
     */
    public void testVisitIGroup() throws Exception {
        String expected = "g1 AS alias"; //$NON-NLS-1$
        assertEquals(expected, getString(TestGroupImpl.example("alias", "vm1.g1"))); //$NON-NLS-1$ //$NON-NLS-2$
        expected = "g1"; //$NON-NLS-1$
        assertEquals(expected, getString(TestGroupImpl.example("vm1.g1"))); //$NON-NLS-1$
    }

    /*
     * Test for void visit(IGroupBy)
     */
    public void testVisitIGroupBy() throws Exception {
        String expected = "GROUP BY g1.e1, g1.e2, g1.e3, g1.e4"; //$NON-NLS-1$
        assertEquals(expected, getString(TestGroupByImpl.example()));
    }

    /*
     * Test for void visit(IInCriteria)
     */
    public void testVisitIInCriteria() throws Exception {
        String expected = "300 IN (100, 200, 300, 400)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestInCriteriaImpl.example(false)));
        expected = "300 NOT IN (100, 200, 300, 400)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestInCriteriaImpl.example(true)));
    }

    /*
     * Test for void visit(IInsert)
     */
    public void testVisitIInsert() throws Exception {
        String expected = "INSERT INTO g1 (e1, e2, e3, e4) VALUES (1, 2, 3, 4)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestInsertImpl.example("g1"))); //$NON-NLS-1$
    }
  
    /*
     * Test for void visit(IIsNullCriteria)
     */
    public void testVisitIIsNullCriteria() throws Exception {
        String expected = "g1.e1 IS NULL"; //$NON-NLS-1$
        assertEquals(expected, getString(TestIsNullCriteriaImpl.example(false)));
        expected = "g1.e1 IS NOT NULL"; //$NON-NLS-1$
        assertEquals(expected, getString(TestIsNullCriteriaImpl.example(true)));
    }

    /*
     * Test for void visit(IJoin)
     */
    public void testVisitIJoin() throws Exception {
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
    public void testVisitILikeCriteria() throws Exception {
        String expected = "g1.e1 LIKE 'likeString' ESCAPE '\\'"; //$NON-NLS-1$
        assertEquals(expected, getString(TestLikeCriteriaImpl.example("likeString", '\\', false))); //$NON-NLS-1$
        expected = "g1.e1 NOT LIKE 'likeString' ESCAPE '\\'"; //$NON-NLS-1$
        assertEquals(expected, getString(TestLikeCriteriaImpl.example("likeString", '\\', true))); //$NON-NLS-1$
    }

    /*
     * Test for void visit(ILiteral)
     */
    public void testVisitILiteral() throws Exception {
        String expected = "'string''Literal'"; //$NON-NLS-1$
        assertEquals(expected, getString(TestLiteralImpl.example("string'Literal"))); //$NON-NLS-1$
        expected = "1000"; //$NON-NLS-1$
        assertEquals(expected, getString(TestLiteralImpl.example(new Integer(1000))));
        expected = "{b'true'}"; //$NON-NLS-1$
        assertEquals(expected, getString(TestLiteralImpl.example(Boolean.TRUE)));
        long now = System.currentTimeMillis();
        Date date = new Date(now);
        expected = "{d'" + date.toString() + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(expected, getString(TestLiteralImpl.example(date)));
        Timestamp ts = new Timestamp(now);
        expected = "{ts'" + ts.toString() + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(expected, getString(TestLiteralImpl.example(ts)));
        Time t = new Time(now);
        expected = "{t'" + t.toString() + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(expected, getString(TestLiteralImpl.example(t)));
    }

    /*
     * Test for void visit(INotCriteria)
     */
    public void testVisitINotCriteria() throws Exception {
        String expected = "NOT (100 >= 200)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestNotCriteriaImpl.example()));
    }

    /*
     * Test for void visit(IOrderBy)
     */
    public void testVisitIOrderBy() throws Exception {
        String expected = "ORDER BY e1, e2 DESC, e3, e4 DESC"; //$NON-NLS-1$
        assertEquals(expected, getString(TestOrderByImpl.example()));
    }

    /*
     * Test for void visit(IParameter)
     */
//    public void testVisitIParameter() throws Exception {
//        String expected = "x"; //$NON-NLS-1$
//        assertEquals(expected, getString(TestParameterImpl.example(1)));
//    }

    /*
     * Test for void visit(IQuery)
     */
    public void testVisitIQuery() throws Exception {
        String expected = "SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE (100 >= 200) AND (500 < 600) GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING (100 >= 200) AND (500 < 600) ORDER BY e1, e2 DESC, e3, e4 DESC"; //$NON-NLS-1$
        assertEquals(expected, getString(TestQueryImpl.example()));
    }

    /*
     * Test for void visit(IScalarSubquery)
     */
    public void testVisitIScalarSubquery() throws Exception {
        String expected = "(SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE (100 >= 200) AND (500 < 600) GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING (100 >= 200) AND (500 < 600) ORDER BY e1, e2 DESC, e3, e4 DESC)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestScalarSubqueryImpl.example()));
    }

    /*
     * Test for void visit(ISearchedCaseExpression)
     */
    public void testVisitISearchedCaseExpression() throws Exception {
        String expected = "CASE WHEN g1.e1 = 0 THEN 0 WHEN g1.e1 = 1 THEN 1 WHEN g1.e1 = 2 THEN 2 ELSE 9999 END"; //$NON-NLS-1$
        assertEquals(expected, getString(TestSearchedCaseExpressionImpl.example()));
    }

    /*
     * Test for void visit(ISelect)
     */
    public void testVisitISelect() throws Exception {
        String expected = "SELECT g1.e1, g1.e2, g1.e3, g1.e4"; //$NON-NLS-1$
        assertEquals(expected, getString(TestSelectImpl.example(false)));
        expected = "SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4"; //$NON-NLS-1$
        assertEquals(expected, getString(TestSelectImpl.example(true)));
    }

    
    /*
     * Test for void visit(ISelectSymbol)
     */
    public void testVisitISelectSymbol() throws Exception {
        String expected = "g1.e1"; //$NON-NLS-1$
        assertEquals(expected, getString(TestSelectSymbolImpl.example("e1", null))); //$NON-NLS-1$
        expected = "g1.e1 AS alias"; //$NON-NLS-1$
        assertEquals(expected, getString(TestSelectSymbolImpl.example("e1", "alias"))); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /*
     * Test for void visit(ISubqueryCompareCriteria)
     */
    public void testVisitISubqueryCompareCriteria() throws Exception {
        String expected = "g1.e1 > SOME (SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE (100 >= 200) AND (500 < 600) GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING (100 >= 200) AND (500 < 600) ORDER BY e1, e2 DESC, e3, e4 DESC)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestSubqueryCompareCriteriaImpl.example())); 
    }

    /*
     * Test for void visit(ISubqueryInCriteria)
     */
    public void testVisitISubqueryInCriteria() throws Exception {
        String expected = "g1.e1 NOT IN (SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE (100 >= 200) AND (500 < 600) GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING (100 >= 200) AND (500 < 600) ORDER BY e1, e2 DESC, e3, e4 DESC)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestSubqueryInCriteriaImpl.example())); 
    }

    public void testVisitIUnion1() throws Exception {
        String expected = "SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE (100 >= 200) AND (500 < 600) GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING (100 >= 200) AND (500 < 600) ORDER BY e1, e2 DESC, e3, e4 DESC UNION SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE (100 >= 200) AND (500 < 600) GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING (100 >= 200) AND (500 < 600) ORDER BY e1, e2 DESC, e3, e4 DESC ORDER BY e1, e2 DESC, e3, e4 DESC";//$NON-NLS-1$
        assertEquals(expected, getString(TestSetQueryImpl.example()));
    }
    
    public void testVisitIUnion2() throws Exception {
        String expected = "SELECT ted.nugent FROM ted UNION ALL SELECT dave.barry FROM dave";//$NON-NLS-1$
        String actual = getString(TestSetQueryImpl.example2());
        assertEquals(expected, actual);
    }

    public void testVisitIUnion3() throws Exception {
        String expected = "SELECT ted.nugent FROM ted UNION ALL SELECT dave.barry FROM dave ORDER BY nugent";//$NON-NLS-1$
        String actual = getString(TestSetQueryImpl.example3());
        assertEquals(expected, actual);
    }
    
    /*
     * Test for void visit(IUpdate)
     */
    public void testVisitIUpdate() throws Exception {
        String expected = "UPDATE g1 SET e1 = 1, e2 = 1, e3 = 1, e4 = 1 WHERE 1 = 1"; //$NON-NLS-1$
        assertEquals(expected, getString(TestUpdateImpl.example()));
    }
    

    public void testVisitProcedure() throws Exception {
        String expected = "EXEC sq3(, x, 1)"; //$NON-NLS-1$
        assertEquals(expected, getString(TestProcedureImpl.example()));
    }
}
