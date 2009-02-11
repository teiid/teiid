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

package com.metamatrix.dqp.internal.datamgr.language;

import com.metamatrix.connector.language.ISubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.*;
import com.metamatrix.query.sql.symbol.ElementSymbol;

import junit.framework.TestCase;

/**
 */
public class TestSubqueryCompareCriteriaImpl extends TestCase {

    /**
     * Constructor for TestSubqueryCompareCriteriaImpl.
     * @param name
     */
    public TestSubqueryCompareCriteriaImpl(String name) {
        super(name);
    }

    public static SubqueryCompareCriteria helpExample() {
        ElementSymbol element = TestElementImpl.helpExample("g1", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Query query = TestQueryImpl.helpExample();
        SubqueryCompareCriteria scc = new SubqueryCompareCriteria(element, query, CompareCriteria.GT, SubqueryCompareCriteria.ANY);
        return scc;
    }
    
    public static SubqueryCompareCriteriaImpl example() throws Exception {
        return (SubqueryCompareCriteriaImpl)TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetExpression() throws Exception {
        assertNotNull(example().getLeftExpression());
    }

    public void testGetQuery() throws Exception {
        assertNotNull(example().getQuery());
    }
    
    public void testOperator() throws Exception {
        assertEquals("Wrong operator", ISubqueryCompareCriteria.GT, example().getOperator()); //$NON-NLS-1$
    }

    public void testQuantifier() throws Exception {
        assertEquals("Wrong quantifier", ISubqueryCompareCriteria.SOME, example().getQuantifier()); //$NON-NLS-1$
    }

}
