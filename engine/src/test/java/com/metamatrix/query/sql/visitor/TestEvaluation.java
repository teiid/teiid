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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Reference;

import junit.framework.TestCase;

public class TestEvaluation extends TestCase {

    private Reference getNestedReferences() {
        ElementSymbol foo = new ElementSymbol("foo");//$NON-NLS-1$
        foo.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        
        Reference reference = new Reference(0, foo); 
        
        Reference wrapper = new Reference(1, reference);
        return wrapper;
    }
    
    public void testNestedReferences() {
        Reference wrapper = getNestedReferences();
        
        //references are never considered evaluatable during planning
        assertFalse(EvaluateExpressionVisitor.isFullyEvaluatable(wrapper, true));

        //lacks data
        assertFalse(EvaluateExpressionVisitor.isFullyEvaluatable(wrapper, false));

        //should have data at runtime
        assertTrue(EvaluateExpressionVisitor.willBecomeConstant(wrapper));

        //should have data at runtime
        assertTrue(EvaluateExpressionVisitor.willBecomeConstant(wrapper));
    }

    public void testNestedReferences1() {
        Reference wrapper = getNestedReferences();
        
        ((Reference)wrapper.getExpression()).setData(Collections.EMPTY_MAP, Collections.EMPTY_LIST);
        
        //references are never considered evaluatable during planning
        assertFalse(EvaluateExpressionVisitor.isFullyEvaluatable(wrapper, true));

        //still lacks data
        assertTrue(EvaluateExpressionVisitor.isFullyEvaluatable(wrapper, false));

        //should have data at runtime
        assertTrue(EvaluateExpressionVisitor.willBecomeConstant(wrapper));

        //should have data at runtime
        assertTrue(EvaluateExpressionVisitor.willBecomeConstant(wrapper));
    }

    public void testNestedReferencesEvaluation() throws Exception {
        Reference wrapper = getNestedReferences();
        
        HashMap index = new HashMap();
        index.put(new ElementSymbol("foo"), new Integer(0)); //$NON-NLS-1$
        List data = new ArrayList();
        data.add(new Integer(1));
        
        ((Reference)wrapper.getExpression()).setData(index, data);
        
        CompareCriteria crit = new CompareCriteria(new ElementSymbol("bar"), CompareCriteria.EQ, wrapper); //$NON-NLS-1$
        
        EvaluateExpressionVisitor.replaceExpressions(crit, true, null, null);
        
        assertEquals("bar = 1", crit.toString()); //$NON-NLS-1$
        
    }    
}
