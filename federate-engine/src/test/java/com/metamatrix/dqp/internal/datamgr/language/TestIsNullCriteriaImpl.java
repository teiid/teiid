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

import com.metamatrix.query.sql.lang.IsNullCriteria;

import junit.framework.TestCase;

public class TestIsNullCriteriaImpl extends TestCase {

    /**
     * Constructor for TestIsNullCriteriaImpl.
     * @param name
     */
    public TestIsNullCriteriaImpl(String name) {
        super(name);
    }

    public static IsNullCriteria helpExample(boolean negated) {
        IsNullCriteria crit = new IsNullCriteria(TestElementImpl.helpExample("vm1.g1", "e1")); //$NON-NLS-1$ //$NON-NLS-2$
        crit.setNegated(negated);
        return crit;
    }
    
    public static IsNullCriteriaImpl example(boolean negated) throws Exception {
        return (IsNullCriteriaImpl)TstLanguageBridgeFactory.factory.translate(helpExample(negated));
    }

    public void testGetExpression() throws Exception {
        assertNotNull(example(false).getExpression());
    }

    public void testIsNegated() throws Exception {
        assertTrue(example(true).isNegated());
        assertFalse(example(false).isNegated());
    }

}
