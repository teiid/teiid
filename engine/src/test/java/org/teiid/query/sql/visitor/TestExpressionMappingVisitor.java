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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.symbol.*;
import org.teiid.translator.SourceSystemFunctions;


@SuppressWarnings("nls")
public class TestExpressionMappingVisitor {

    public void helpTest(LanguageObject original, Map<Expression, Expression> map, LanguageObject expected) {
        ExpressionMappingVisitor.mapExpressions(original, map);

        assertEquals("Did not get expected mapped expression", expected, original);     //$NON-NLS-1$
    }

    @Test public void testCompareCriteria1() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        Function f = new Function("+", new Expression[] { new Constant(new Integer(2)), new Constant(new Integer(5)) }); //$NON-NLS-1$
        Map<Expression, Expression> map = new HashMap<Expression, Expression>();
        map.put(e1, f);
        CompareCriteria before = new CompareCriteria(e1, CompareCriteria.EQ, new Constant("xyz")); //$NON-NLS-1$
        CompareCriteria after = new CompareCriteria(f, CompareCriteria.EQ, new Constant("xyz")); //$NON-NLS-1$
        helpTest(before, map, after);
    }

    @Test public void testCompareCriteria2() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        Function f = new Function("+", new Expression[] { new Constant(new Integer(2)), new Constant(new Integer(5)) }); //$NON-NLS-1$
        Map<Expression, Expression> map = new HashMap<Expression, Expression>();
        map.put(e1, f);
        CompareCriteria before = new CompareCriteria(new Constant("xyz"), CompareCriteria.EQ, e1); //$NON-NLS-1$
        CompareCriteria after = new CompareCriteria(new Constant("xyz"), CompareCriteria.EQ, f); //$NON-NLS-1$
        helpTest(before, map, after);
    }

    @Test public void testFunction1() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$
        ElementSymbol e3 = new ElementSymbol("e3"); //$NON-NLS-1$
        ElementSymbol e4 = new ElementSymbol("e4"); //$NON-NLS-1$
        Function f1 = new Function("+", new Expression[] { e1, e2 }); //$NON-NLS-1$
        Function f2 = new Function("+", new Expression[] { f1, e3 }); //$NON-NLS-1$
        Function f3 = new Function("+", new Expression[] { f2, e4 }); //$NON-NLS-1$

        ElementSymbol e5 = new ElementSymbol("e5");                 //$NON-NLS-1$
        ElementSymbol e6 = new ElementSymbol("e6"); //$NON-NLS-1$
        ElementSymbol e7 = new ElementSymbol("e7"); //$NON-NLS-1$
        Function f4 = new Function("*", new Expression[] { e5, e6 }); //$NON-NLS-1$

        Map<Expression, Expression> map = new HashMap<Expression, Expression>();
        map.put(e3, f4);
        map.put(e2, e7);

        Function f5 = new Function("+", new Expression[] { e1, e7 }); //$NON-NLS-1$
        Function f6 = new Function("+", new Expression[] { f5, f4 }); //$NON-NLS-1$
        Function f7 = new Function("+", new Expression[] { f6, e4 }); //$NON-NLS-1$
        helpTest(f3, map, f7);
    }

    @Test public void testSetCriteria() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$
        Constant c1 = new Constant("xyz"); //$NON-NLS-1$
        Constant c2 = new Constant("abc"); //$NON-NLS-1$
        Constant c3 = new Constant("def"); //$NON-NLS-1$
        List<Expression> values = new ArrayList<Expression>();
        values.add(c1);
        values.add(c2);
        List<Expression> mappedValues = new ArrayList<Expression>();
        mappedValues.add(c1);
        mappedValues.add(c3);

        Map<Expression, Expression> map = new HashMap<Expression, Expression>();
        map.put(e1, e2);
        map.put(c2, c3);

        SetCriteria before = new SetCriteria(e1, values);
        SetCriteria after = new SetCriteria(e2, mappedValues);
        helpTest(before, map, after);
    }

    @Test public void testCaseExpression1() {
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        ElementSymbol y = new ElementSymbol("y"); //$NON-NLS-1$
        Constant a = new Constant(String.valueOf('a'));
        Constant z = new Constant(String.valueOf('z'));

        HashMap<Expression, Expression> map = new HashMap<Expression, Expression>();
        map.put(x, y);
        map.put(a, z);

        ArrayList<Expression> whens = new ArrayList<Expression>(), thens = new ArrayList<Expression>();
        whens.add(new Constant(String.valueOf('z')));
        thens.add(new Constant(new Integer(0)));
        whens.add(new Constant(String.valueOf('b')));
        thens.add(new Constant(new Integer(1)));
        whens.add(new Constant(String.valueOf('c')));
        thens.add(new Constant(new Integer(2)));
        CaseExpression mapped = new CaseExpression(y, whens, thens);
        mapped.setElseExpression(new Constant(new Integer(9999)));

        helpTest(TestCaseExpression.example(3), map, mapped);
    }

    @Test public void testCaseExpression2() {
        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        ElementSymbol y = new ElementSymbol("y"); //$NON-NLS-1$

        HashMap<Expression, Expression> map = new HashMap<Expression, Expression>();
        map.put(x, y);

        ArrayList<Expression> whens = new ArrayList<Expression>(), thens = new ArrayList<Expression>();
        whens.add(new CompareCriteria(y, CompareCriteria.EQ, new Constant(new Integer(0))));
        thens.add(new Constant(new Integer(0)));
        whens.add(new CompareCriteria(y, CompareCriteria.EQ, new Constant(new Integer(1))));
        thens.add(new Constant(new Integer(1)));
        whens.add(new CompareCriteria(y, CompareCriteria.EQ, new Constant(new Integer(2))));
        thens.add(new Constant(new Integer(2)));
        SearchedCaseExpression mapped = new SearchedCaseExpression(whens, thens);
        mapped.setElseExpression(new Constant(new Integer(9999)));

        helpTest(TestSearchedCaseExpression.example(3), map, mapped);
    }

    @Test public void testSelectAlias() {
        ElementSymbol x = new ElementSymbol("y.x"); //$NON-NLS-1$
        ElementSymbol y = new ElementSymbol("z.X"); //$NON-NLS-1$

        HashMap<Expression, Expression> map = new HashMap<Expression, Expression>();
        map.put(x, y);

        LanguageObject toMap = new Select(Arrays.asList(x));

        ExpressionMappingVisitor.mapExpressions(toMap, map);

        assertEquals("Did not get expected mapped expression", " z.X AS x", toMap.toString());     //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSelectAlias1() {
        ElementSymbol x = new ElementSymbol("y.x"); //$NON-NLS-1$
        ElementSymbol y = new ElementSymbol("z.x"); //$NON-NLS-1$

        HashMap<ElementSymbol, ElementSymbol> map = new HashMap<ElementSymbol, ElementSymbol>();
        map.put(x, y);

        LanguageObject toMap = new Select(Arrays.asList(x));

        ExpressionMappingVisitor.mapExpressions(toMap, map);

        assertEquals("Did not get expected mapped expression", " z.x", toMap.toString());     //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * we are not careful about ensuring that that every symbol is
     * unique in a plan, so there's a chance mapping the expression in an
     * aggregate in a project node will cause the same symbol to be
     * updated in a sort node.  to ensure that we don't get into
     * recursion trouble we detect if we're replacing an expression
     * that already exists as a mapping.
     *
     * we simulate that situation here using the same aggregate twice in
     * a function.
     */
    @Test public void testRecursionDetection() {
        ElementSymbol e1 = new ElementSymbol("g1.e1"); //$NON-NLS-1$
        AggregateSymbol a1 = new AggregateSymbol(NonReserved.SUM, false, e1);
        Function f = new Function(SourceSystemFunctions.ADD_OP, new Expression[] {a1, a1});
        HashMap<AggregateSymbol, AggregateSymbol> map = new HashMap<AggregateSymbol, AggregateSymbol>();
        map.put(a1, new AggregateSymbol(NonReserved.SUM, false, a1));
        ExpressionMappingVisitor.mapExpressions(f, map);
        assertEquals("(SUM(SUM(g1.e1)) + SUM(SUM(g1.e1)))", f.toString()); //$NON-NLS-1$
    }

    @Test public void testArray() {
        Expression e1 = new ElementSymbol("g1.e1"); //$NON-NLS-1$
        Expression e2 = new ElementSymbol("g1.e2"); //$NON-NLS-1$
        Map<Expression, ElementSymbol> map = new HashMap<Expression, ElementSymbol>();
        map.put(e1, new ElementSymbol("foo"));
        Array a = new Array(DataTypeManager.DefaultDataClasses.OBJECT, Arrays.asList(e1, e2));
        ExpressionMappingVisitor.mapExpressions(a, map);
        assertEquals("(foo, g1.e2)", a.toString()); //$NON-NLS-1$
    }

}
