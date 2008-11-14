/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.query.sql.visitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.symbol.CaseExpression;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.sql.symbol.TestCaseExpression;
import com.metamatrix.query.sql.symbol.TestSearchedCaseExpression;


public class TestExpressionMappingVisitor extends TestCase {

    public TestExpressionMappingVisitor(String arg0) {
        super(arg0);
    }

    public void helpTest(LanguageObject original, Map map, LanguageObject expected) {
        ExpressionMappingVisitor.mapExpressions(original, map);
        
        assertEquals("Did not get expected mapped expression", expected, original);     //$NON-NLS-1$
    }
    
    public void testCompareCriteria1() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        Function f = new Function("+", new Expression[] { new Constant(new Integer(2)), new Constant(new Integer(5)) }); //$NON-NLS-1$
        Map map = new HashMap();
        map.put(e1, f);
        CompareCriteria before = new CompareCriteria(e1, CompareCriteria.EQ, new Constant("xyz")); //$NON-NLS-1$
        CompareCriteria after = new CompareCriteria(f, CompareCriteria.EQ, new Constant("xyz")); //$NON-NLS-1$
        helpTest(before, map, after);
    }
    
    public void testCompareCriteria2() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        Function f = new Function("+", new Expression[] { new Constant(new Integer(2)), new Constant(new Integer(5)) }); //$NON-NLS-1$
        Map map = new HashMap();
        map.put(e1, f);
        CompareCriteria before = new CompareCriteria(new Constant("xyz"), CompareCriteria.EQ, e1); //$NON-NLS-1$
        CompareCriteria after = new CompareCriteria(new Constant("xyz"), CompareCriteria.EQ, f); //$NON-NLS-1$
        helpTest(before, map, after);
    }

    public void testFunction1() {
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

    public void testSetCriteria() {
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
    
    public void testCaseExpression1() {
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
    
    public void testCaseExpression2() {
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
    
}
