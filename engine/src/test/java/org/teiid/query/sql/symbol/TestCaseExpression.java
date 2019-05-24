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
import org.teiid.core.util.ArgCheck;

public class TestCaseExpression extends TestCase {

    /**
     * Constructor for TestCaseExpression.
     * @param name
     */
    public TestCaseExpression(String name) {
        super(name);
    }

    public void testGetWhen() {
        CaseExpression expr = example(3);
        assertNotNull(expr.getWhen());
        assertEquals(3, expr.getWhen().size());
        try {
            expr.getWhen().add(new Object());
            fail("Should not be modifiable"); //$NON-NLS-1$
        } catch (UnsupportedOperationException e) {

        }
    }

    public void testGetThen() {
        CaseExpression expr = example(3);
        assertNotNull(expr.getThen());
        assertEquals(3, expr.getThen().size());
        try {
            expr.getThen().add(new Object());
            fail("Should not be modifiable"); //$NON-NLS-1$
        } catch (UnsupportedOperationException e) {

        }
    }

    public static List getWhenExpressions(int expressions) {
        return getWhenExpressions(expressions, -1, false);
    }

    public static List getWhenExpressions(int expressions, int nullIndex, boolean includeNull) {
        ArrayList list = new ArrayList();
        for (int i = 0; i < expressions; i++) {
            if(includeNull && i == nullIndex) {
                list.add(new Constant(null) );
            }else {
                list.add(new Constant(String.valueOf((char)('a' + i))));
            }
        }
        return list;
    }

    public static List getThenExpressions(int expressions) {
        ArrayList list = new ArrayList();
        for (int i = 0; i < expressions; i++) {
            list.add(new Constant(new Integer(i)));
        }
        return list;
    }

    public static CaseExpression example(int whens) {
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        CaseExpression caseExpr = new CaseExpression(x, getWhenExpressions(whens), getThenExpressions(whens));
        caseExpr.setElseExpression(new Constant(new Integer(9999)));
        return caseExpr;
    }

    public static CaseExpression example(int whens, int nullIndex, boolean includeNull) {
        ArgCheck.isTrue(nullIndex < whens, "Null Index must be less than the number of When expressions"); //$NON-NLS-1$
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        CaseExpression caseExpr = new CaseExpression(x, getWhenExpressions(whens, nullIndex, includeNull), getThenExpressions(whens));
        caseExpr.setElseExpression(new Constant(new Integer(9999)));
        return caseExpr;
    }

    public static void helpTestWhenExpressions(CaseExpression caseExpr, int expectedWhens) {
        assertEquals(expectedWhens, caseExpr.getWhenCount());
        for (int i = 0; i < expectedWhens; i++) {
            assertEquals(new Constant(String.valueOf((char)('a' + i))), caseExpr.getWhenExpression(i));
        }
    }

    public static void helpTestThenExpressions(AbstractCaseExpression caseExpr, int expectedThens) {
        assertEquals(expectedThens, caseExpr.getWhenCount());
        for (int i = 0; i < expectedThens; i++) {
            assertEquals(new Constant(new Integer(i)), caseExpr.getThenExpression(i));
        }
    }

    /**
     * Test that the Object references are not equal, but that the result of
     * the equals() method is true.
     * @param obj1
     * @param obj2
     */
    public static void helpTestStrictEquivalence(Object obj1, Object obj2) {
        assertTrue(obj1 != obj2);
        assertTrue(obj1 != null);
        assertTrue(obj2 != null);
        assertTrue(obj1.equals(obj2));
        assertTrue(obj2.equals(obj1));
    }

    public void testGetWhenCount() {
        assertEquals(1, example(1).getWhenCount());
        assertEquals(2, example(2).getWhenCount());
        assertEquals(3, example(3).getWhenCount());
        assertEquals(4, example(4).getWhenCount());
    }

    public void testGetExpression() {
        assertNotNull(example(1).getExpression());
        assertEquals(new ElementSymbol("x"), example(1).getExpression()); //$NON-NLS-1$
    }

    public void testSetExpression() {
        CaseExpression caseExpr = example(1);
        ElementSymbol y = new ElementSymbol("y"); //$NON-NLS-1$
        caseExpr.setExpression(y);
        assertEquals(y, caseExpr.getExpression());

        try {
            caseExpr.setExpression(null);
            fail("Setting the expression to null should fail."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // There should be no side-effects of an illegal argument
            assertEquals(y, caseExpr.getExpression());
        }
    }

    /*
     * Test for Object clone()
     */
    public void testClone() {
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        Expression e1 = new Constant("abc"); //$NON-NLS-1$
        Expression e2 = new Constant("xyz"); //$NON-NLS-1$
        ArrayList whens = new ArrayList();
        whens.add(e1);
        whens.add(e2);
        Constant const1 = new Constant("a"); //$NON-NLS-1$
        Constant const2 = new Constant("b"); //$NON-NLS-1$
        ArrayList thens = new ArrayList();
        thens.add(const1);
        thens.add(const2);
        Expression elseExpression = new Constant("c"); //$NON-NLS-1$
        CaseExpression expr = new CaseExpression(x, whens, thens);
        expr.setElseExpression(elseExpression);
        expr.setType(DataTypeManager.DefaultDataClasses.STRING);

        CaseExpression clone = (CaseExpression)expr.clone();

        assertTrue(expr != clone);

        helpTestStrictEquivalence(x, clone.getExpression());
        helpTestStrictEquivalence(expr.getExpression(), clone.getExpression());

        assertEquals(2, clone.getWhenCount());

        helpTestStrictEquivalence(e1, clone.getWhenExpression(0));
        helpTestStrictEquivalence(expr.getWhenExpression(0), clone.getWhenExpression(0));
        helpTestStrictEquivalence(e2, clone.getWhenExpression(1));
        helpTestStrictEquivalence(expr.getWhenExpression(1), clone.getWhenExpression(1));

        helpTestStrictEquivalence(const1, clone.getThenExpression(0));
        helpTestStrictEquivalence(expr.getThenExpression(0), clone.getThenExpression(0));
        helpTestStrictEquivalence(const2, clone.getThenExpression(1));
        helpTestStrictEquivalence(expr.getThenExpression(1), clone.getThenExpression(1));

        helpTestStrictEquivalence(expr.getElseExpression(), clone.getElseExpression());
        assertEquals(expr.getType(), clone.getType());
    }

    public void testGetWhenExpression() {
        helpTestWhenExpressions(example(3), 3);
    }

    public void testSetWhen() {
        CaseExpression caseExpr = example(4);
        // Both are nulls
        try {
            caseExpr.setWhen(null, null);
            fail("Setting WHEN and THEN to null should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // There should be no side-effects of an illegal argument
            helpTestWhenExpressions(caseExpr, 4);
            helpTestThenExpressions(caseExpr, 4);
        }
        try {
            caseExpr.setWhen(getWhenExpressions(2), null);
            fail("Setting THEN to null should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // There should be no side-effects of an illegal argument
            helpTestWhenExpressions(caseExpr, 4);
            helpTestThenExpressions(caseExpr, 4);
        }
        try {
            caseExpr.setWhen(null, getThenExpressions(2));
            fail("Setting WHEN to null should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // There should be no side-effects of an illegal argument
            helpTestWhenExpressions(caseExpr, 4);
            helpTestThenExpressions(caseExpr, 4);
        }
        try {
            caseExpr.setWhen(getWhenExpressions(0), getThenExpressions(0));
            fail("Setting WHEN and THEN to empty lists should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // There should be no side-effects of an illegal argument
            helpTestWhenExpressions(caseExpr, 4);
            helpTestThenExpressions(caseExpr, 4);
        }
        caseExpr.setWhen(TestSearchedCaseExpression.getWhenCriteria(3), getThenExpressions(3));
        caseExpr.setWhen(getWhenExpressions(3), TestSearchedCaseExpression.getWhenCriteria(3));
        ArrayList whens = new ArrayList();
        whens.add(new Constant("abc")); //$NON-NLS-1$
        whens.add(new Constant("xyz")); //$NON-NLS-1$
        ArrayList thens = new ArrayList();
        thens.add(new Constant(new Integer(20000)));
        thens.add(new Constant(new Integer(30000)));
        caseExpr.setWhen(whens, thens);
        assertEquals(2, caseExpr.getWhenCount());
        assertEquals(new Constant("abc"), caseExpr.getWhenExpression(0)); //$NON-NLS-1$
        assertEquals(new Constant("xyz"), caseExpr.getWhenExpression(1)); //$NON-NLS-1$
        assertEquals(new Constant(new Integer(20000)), caseExpr.getThenExpression(0));
        assertEquals(new Constant(new Integer(30000)), caseExpr.getThenExpression(1));
    }

    public void testGetThenExpression() {
        helpTestThenExpressions(example(3), 3);
    }

    public void testGetElseExpression() {
        CaseExpression expr = example(3);
        assertEquals(new Constant(new Integer(9999)), expr.getElseExpression());
    }

    public void testSetElseExpression() {
        CaseExpression expr = example(3);
        expr.setElseExpression(new Constant(new Integer(1000)));
        assertEquals(new Constant(new Integer(1000)), expr.getElseExpression());
        expr.setElseExpression(null);
        assertNull(expr.getElseExpression());
    }

    public void testGetType() {
        CaseExpression expr = example(4);
        assertNull(expr.getType());
        expr.setType(Integer.class);
        assertEquals(Integer.class, expr.getType());
    }

    public void testSetType() {
        CaseExpression expr = example(4);
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
        CaseExpression sc1 = example(3);
        assertTrue(sc1.equals(sc1));
        assertTrue(sc1.equals(sc1.clone()));
        assertTrue(sc1.clone().equals(sc1));
        assertTrue(sc1.equals(example(3)));

        CaseExpression sc2 = example(4);

        assertFalse(sc1.equals(sc2));
        assertFalse(sc2.equals(sc1));

        CaseExpression sc3 = example(3);
        sc3.setElseExpression(new ElementSymbol("y")); //$NON-NLS-1$
        assertFalse(sc1.equals(sc3));
        assertFalse(sc3.equals(sc1));

        CaseExpression sc4 = example(3);
        sc4.setExpression(new ElementSymbol("y")); //$NON-NLS-1$
        assertFalse(sc1.equals(sc4));
        assertFalse(sc4.equals(sc1));
    }
}
