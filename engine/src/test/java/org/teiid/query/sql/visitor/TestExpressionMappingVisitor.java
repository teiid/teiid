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

package org.teiid.query.sql.visitor;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.CaseExpression;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.TestCaseExpression;
import org.teiid.query.sql.symbol.TestSearchedCaseExpression;
import org.teiid.translator.SourceSystemFunctions;



public class TestExpressionMappingVisitor {

    public void helpTest(LanguageObject original, Map map, LanguageObject expected) {
        ExpressionMappingVisitor.mapExpressions(original, map);
        
        assertEquals("Did not get expected mapped expression", expected, original);     //$NON-NLS-1$
    }
    
    @Test public void testCompareCriteria1() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        Function f = new Function("+", new Expression[] { new Constant(new Integer(2)), new Constant(new Integer(5)) }); //$NON-NLS-1$
        Map map = new HashMap();
        map.put(e1, f);
        CompareCriteria before = new CompareCriteria(e1, CompareCriteria.EQ, new Constant("xyz")); //$NON-NLS-1$
        CompareCriteria after = new CompareCriteria(f, CompareCriteria.EQ, new Constant("xyz")); //$NON-NLS-1$
        helpTest(before, map, after);
    }
    
    @Test public void testCompareCriteria2() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        Function f = new Function("+", new Expression[] { new Constant(new Integer(2)), new Constant(new Integer(5)) }); //$NON-NLS-1$
        Map map = new HashMap();
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
                        
        Map map = new HashMap();
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
        List values = new ArrayList();
        values.add(c1);
        values.add(c2);
        List mappedValues = new ArrayList();
        mappedValues.add(c1);
        mappedValues.add(c3);
        
        Map map = new HashMap();
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
        
        HashMap map = new HashMap();
        map.put(x, y);
        map.put(a, z);
        
        ArrayList whens = new ArrayList(), thens = new ArrayList();
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
        
        HashMap map = new HashMap();
        map.put(x, y);
        
        ArrayList whens = new ArrayList(), thens = new ArrayList();
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
    
    /**
     * We do not need to create an alias if the canonical short names match
     */
    @Test public void testSelectAlias() {
        ElementSymbol x = new ElementSymbol("y.x"); //$NON-NLS-1$
        ElementSymbol y = new ElementSymbol("z.X"); //$NON-NLS-1$
        
        HashMap map = new HashMap();
        map.put(x, y);
        
        LanguageObject toMap = new Select(Arrays.asList(x));
        
        ExpressionMappingVisitor.mapExpressions(toMap, map);
        
        assertEquals("Did not get expected mapped expression", "SELECT z.X", toMap.toString());     //$NON-NLS-1$ //$NON-NLS-2$
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
    	AggregateSymbol a1 = new AggregateSymbol("x", NonReserved.SUM, false, e1); //$NON-NLS-1$
    	Function f = new Function(SourceSystemFunctions.ADD_OP, new Expression[] {a1, a1});
    	HashMap<AggregateSymbol, AggregateSymbol> map = new HashMap<AggregateSymbol, AggregateSymbol>();
    	map.put(a1, new AggregateSymbol("x", NonReserved.SUM, false, a1)); //$NON-NLS-1$
    	ExpressionMappingVisitor.mapExpressions(f, map);
        assertEquals("(SUM(SUM(g1.e1)) + SUM(SUM(g1.e1)))", f.toString()); //$NON-NLS-1$
    }
    
}
