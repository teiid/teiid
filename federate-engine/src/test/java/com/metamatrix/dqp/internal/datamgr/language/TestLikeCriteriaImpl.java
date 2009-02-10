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

import com.metamatrix.connector.language.ILiteral;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;

import junit.framework.TestCase;

public class TestLikeCriteriaImpl extends TestCase {

    /**
     * Constructor for TestLikeCriteriaImpl.
     * @param name
     */
    public TestLikeCriteriaImpl(String name) {
        super(name);
    }

    public static MatchCriteria helpExample(String right, char escape, boolean negated) {
        ElementSymbol e1 = TestElementImpl.helpExample("vm1.g1", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        MatchCriteria match = new MatchCriteria(e1, new Constant(right), escape);
        match.setNegated(negated);
        return match;
    }
    
    public static LikeCriteriaImpl example(String right, char escape, boolean negated) throws Exception {
        return (LikeCriteriaImpl)TstLanguageBridgeFactory.factory.translate(helpExample(right, escape, negated));
    }

    public void testGetLeftExpression() throws Exception {
        assertNotNull(example("abc", '.', false).getLeftExpression()); //$NON-NLS-1$
    }

    public void testGetRightExpression() throws Exception {
        LikeCriteriaImpl like = example("abc", '.', false); //$NON-NLS-1$
        assertNotNull(like.getRightExpression());
        assertTrue(like.getRightExpression() instanceof ILiteral);
        assertEquals("abc", ((ILiteral)like.getRightExpression()).getValue()); //$NON-NLS-1$
    }

    public void testGetEscapeCharacter() throws Exception {
        assertEquals(new Character('.'), example("abc", '.', false).getEscapeCharacter()); //$NON-NLS-1$
    }

    public void testIsNegated() throws Exception {
        assertTrue(example("abc", '.', true).isNegated()); //$NON-NLS-1$
        assertFalse(example("abc", '.', false).isNegated()); //$NON-NLS-1$
    }

}
