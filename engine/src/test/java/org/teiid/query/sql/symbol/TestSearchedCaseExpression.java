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

package org.teiid.query.sql.symbol;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.lang.CompareCriteria;

public class TestSearchedCaseExpression extends TestCase {

    /**
     * Constructor for TestSearchedCaseExpression.
     * @param name
     */
    public TestSearchedCaseExpression(String name) {
        super(name);
    }

    public static List getWhenCriteria(int criteria) {
        ArrayList list = new ArrayList();
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        for (int i = 0; i < criteria; i++) {
            list.add(new CompareCriteria(x, CompareCriteria.EQ, new Constant(new Integer(i))));
        }
        return list;
    }

    public static List getAlphaWhenCriteria(int criteria) {
        ArrayList list = new ArrayList();
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        for (int i = 0; i < criteria; i++) {
            list.add(new CompareCriteria(x, CompareCriteria.EQ, new Constant(String.valueOf((char)('a' + i)))));
        }
        return list;
    }

    public static SearchedCaseExpression example(int whens) {
        SearchedCaseExpression caseExpr = new SearchedCaseExpression(getWhenCriteria(whens), TestCaseExpression.getThenExpressions(whens));
        caseExpr.setElseExpression(new Constant(new Integer(9999)));
        return caseExpr;
    }

    public static SearchedCaseExpression example2(int whens) {
        SearchedCaseExpression caseExpr = new SearchedCaseExpression(getAlphaWhenCriteria(whens), TestCaseExpression.getThenExpressions(whens));
        caseExpr.setElseExpression(new Constant(new Integer(9999)));
        return caseExpr;
    }

    public static void helpTestWhenCriteria(SearchedCaseExpression caseExpr, int expectedWhens) {
        assertEquals(expectedWhens, caseExpr.getWhenCount());
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        for (int i = 0; i < expectedWhens; i++) {
            assertEquals(new CompareCriteria(x, CompareCriteria.EQ, new Constant(new Integer(i))),
                         caseExpr.getWhenCriteria(i));
        }
    }

    public void testGetWhenCount() {
        assertEquals(1, example(1).getWhenCount());
        assertEquals(2, example(2).getWhenCount());
        assertEquals(3, example(3).getWhenCount());
        assertEquals(4, example(4).getWhenCount());
    }

    public void testGetWhen() {
        SearchedCaseExpression expr = example(3);
        assertNotNull(expr.getWhen());
        assertEquals(3, expr.getWhen().size());
        try {
            expr.getWhen().add(new Object());
            fail("Should not be modifiable"); //$NON-NLS-1$
        } catch (UnsupportedOperationException e) {

        }
    }

    public void testGetThen() {
        SearchedCaseExpression expr = example(3);
        assertNotNull(expr.getThen());
        assertEquals(3, expr.getThen().size());
        try {
            expr.getThen().add(new Object());
            fail("Should not be modifiable"); //$NON-NLS-1$
        } catch (UnsupportedOperationException e) {

        }
    }

    /*
     * Test for Object clone()
     */
    public void testClone() {
        CompareCriteria c1 = new CompareCriteria(new ElementSymbol("abc"), CompareCriteria.EQ, new Constant(new Integer(20000))); //$NON-NLS-1$
        CompareCriteria c2 = new CompareCriteria(new ElementSymbol("xyz"), CompareCriteria.EQ, new Constant(new Integer(30000))); //$NON-NLS-1$
        ArrayList whens = new ArrayList();
        whens.add(c1);
        whens.add(c2);
        Constant const1 = new Constant("a"); //$NON-NLS-1$
        Constant const2 = new Constant("b"); //$NON-NLS-1$
        ArrayList thens = new ArrayList();
        thens.add(const1);
        thens.add(const2);
        Expression elseExpression = new Constant("c"); //$NON-NLS-1$
        SearchedCaseExpression expr = new SearchedCaseExpression(whens, thens);
        expr.setElseExpression(elseExpression);
        expr.setType(DataTypeManager.DefaultDataClasses.STRING);

        SearchedCaseExpression clone = (SearchedCaseExpression)expr.clone();

        assertTrue(expr != clone);
        assertEquals(2, clone.getWhenCount());


        TestCaseExpression.helpTestStrictEquivalence(c1, clone.getWhenCriteria(0));
        TestCaseExpression.helpTestStrictEquivalence(expr.getWhenCriteria(0), clone.getWhenCriteria(0));
        TestCaseExpression.helpTestStrictEquivalence(c2, clone.getWhenCriteria(1));
        TestCaseExpression.helpTestStrictEquivalence(expr.getWhenCriteria(1), clone.getWhenCriteria(1));

        TestCaseExpression.helpTestStrictEquivalence(const1, clone.getThenExpression(0));
        TestCaseExpression.helpTestStrictEquivalence(expr.getThenExpression(0), clone.getThenExpression(0));
        TestCaseExpression.helpTestStrictEquivalence(const2, clone.getThenExpression(1));
        TestCaseExpression.helpTestStrictEquivalence(expr.getThenExpression(1), clone.getThenExpression(1));

        TestCaseExpression.helpTestStrictEquivalence(expr.getElseExpression(), clone.getElseExpression());
        assertEquals(expr.getType(), clone.getType());
    }

    public void testGetWhenCriteria() {
        helpTestWhenCriteria(example(4), 4);
    }

    public void testSetWhen() {
        SearchedCaseExpression caseExpr = example(4);
        // Both are nulls
        try {
            caseExpr.setWhen(null, null);
            fail("Setting WHEN and THEN to null should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // There should be no side-effects of an illegal argument
            helpTestWhenCriteria(caseExpr, 4);
            TestCaseExpression.helpTestThenExpressions(caseExpr, 4);
        }
        try {
            caseExpr.setWhen(getWhenCriteria(2), null);
            fail("Setting THEN to null should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // There should be no side-effects of an illegal argument
            helpTestWhenCriteria(caseExpr, 4);
            TestCaseExpression.helpTestThenExpressions(caseExpr, 4);
        }
        try {
            caseExpr.setWhen(null, TestCaseExpression.getThenExpressions(2));
            fail("Setting WHEN to null should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // There should be no side-effects of an illegal argument
            helpTestWhenCriteria(caseExpr, 4);
           TestCaseExpression.helpTestThenExpressions(caseExpr, 4);
        }
        try {
            caseExpr.setWhen(getWhenCriteria(0), TestCaseExpression.getThenExpressions(0));
            fail("Setting WHEN and THEN to empty lists should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // There should be no side-effects of an illegal argument
            helpTestWhenCriteria(caseExpr, 4);
            TestCaseExpression.helpTestThenExpressions(caseExpr, 4);
        }
        try {
            caseExpr.setWhen(TestCaseExpression.getWhenExpressions(3), TestCaseExpression.getThenExpressions(3));
            fail("Setting WHEN non Criteria types should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // There should be no side-effects of an illegal argument
            helpTestWhenCriteria(caseExpr, 4);
            TestCaseExpression.helpTestThenExpressions(caseExpr, 4);
        }
        caseExpr.setWhen(getWhenCriteria(3), getWhenCriteria(3));
        ArrayList whens = new ArrayList();
        whens.add(new CompareCriteria(new ElementSymbol("abc"), CompareCriteria.EQ, new Constant(new Integer(20000)))); //$NON-NLS-1$
        whens.add(new CompareCriteria(new ElementSymbol("xyz"), CompareCriteria.EQ, new Constant(new Integer(30000)))); //$NON-NLS-1$
        ArrayList thens = new ArrayList();
        thens.add(new Constant(new Integer(20000)));
        thens.add(new Constant(new Integer(30000)));
        caseExpr.setWhen(whens, thens);
        assertEquals(2, caseExpr.getWhenCount());
        assertEquals(new CompareCriteria(new ElementSymbol("abc"), CompareCriteria.EQ, new Constant(new Integer(20000))), caseExpr.getWhenCriteria(0)); //$NON-NLS-1$
        assertEquals(new CompareCriteria(new ElementSymbol("xyz"), CompareCriteria.EQ, new Constant(new Integer(30000))), caseExpr.getWhenCriteria(1)); //$NON-NLS-1$
        assertEquals(new Constant(new Integer(20000)), caseExpr.getThenExpression(0));
        assertEquals(new Constant(new Integer(30000)), caseExpr.getThenExpression(1));
    }

    public void testGetThenExpression() {
        TestCaseExpression.helpTestThenExpressions(example(3), 3);
    }

    public void testGetElseExpression() {
        SearchedCaseExpression expr = example(3);
        assertEquals(new Constant(new Integer(9999)), expr.getElseExpression());
    }

    public void testSetElseExpression() {
        SearchedCaseExpression expr = example(3);
        expr.setElseExpression(new Constant(new Integer(1000)));
        assertEquals(new Constant(new Integer(1000)), expr.getElseExpression());
        expr.setElseExpression(null);
        assertNull(expr.getElseExpression());
    }

    public void testGetType() {
        SearchedCaseExpression expr = example(4);
        assertNull(expr.getType());
        expr.setType(Integer.class);
        assertEquals(Integer.class, expr.getType());
    }

    public void testSetType() {
        SearchedCaseExpression expr = example(4);
        expr.setType(DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
        assertEquals(DataTypeManager.DefaultDataClasses.BIG_DECIMAL, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.BIG_INTEGER);
        assertEquals(DataTypeManager.DefaultDataClasses.BIG_INTEGER, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.BLOB);
        assertEquals(DataTypeManager.DefaultDataClasses.BLOB, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
        assertEquals(DataTypeManager.DefaultDataClasses.BOOLEAN, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.BYTE);
        assertEquals(DataTypeManager.DefaultDataClasses.BYTE, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.CHAR);
        assertEquals(DataTypeManager.DefaultDataClasses.CHAR, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.CLOB);
        assertEquals(DataTypeManager.DefaultDataClasses.CLOB, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.DATE);
        assertEquals(DataTypeManager.DefaultDataClasses.DATE, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.DOUBLE);
        assertEquals(DataTypeManager.DefaultDataClasses.DOUBLE, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.FLOAT);
        assertEquals(DataTypeManager.DefaultDataClasses.FLOAT, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.LONG);
        assertEquals(DataTypeManager.DefaultDataClasses.LONG, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.NULL);
        assertEquals(DataTypeManager.DefaultDataClasses.NULL, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.OBJECT);
        assertEquals(DataTypeManager.DefaultDataClasses.OBJECT, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.SHORT);
        assertEquals(DataTypeManager.DefaultDataClasses.SHORT, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.STRING);
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.TIME);
        assertEquals(DataTypeManager.DefaultDataClasses.TIME, expr.getType());
        expr.setType(DataTypeManager.DefaultDataClasses.TIMESTAMP);
        assertEquals(DataTypeManager.DefaultDataClasses.TIMESTAMP, expr.getType());
        expr.setType(null);
        assertNull(expr.getType());
    }

    public void testEquals() {
        SearchedCaseExpression sc1 = example(3);
        assertTrue(sc1.equals(sc1));
        assertTrue(sc1.equals(sc1.clone()));
        assertTrue(sc1.clone().equals(sc1));
        assertTrue(sc1.equals(example(3)));

        SearchedCaseExpression sc2 = example(4);

        assertFalse(sc1.equals(sc2));
        assertFalse(sc2.equals(sc1));

        SearchedCaseExpression sc3 = example(3);
        sc3.setElseExpression(new ElementSymbol("y")); //$NON-NLS-1$
        assertFalse(sc1.equals(sc3));
        assertFalse(sc3.equals(sc1));
    }
}
