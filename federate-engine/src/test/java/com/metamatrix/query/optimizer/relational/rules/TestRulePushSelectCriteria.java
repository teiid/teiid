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

package com.metamatrix.query.optimizer.relational.rules;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.sql.symbol.ElementSymbol;

public class TestRulePushSelectCriteria extends TestCase {
    
    public void testElementsInCritieria() throws Exception {
        String criteria = "e1 = '1' OR ((e1 = '2' OR e1 = '4') AND e2 = 3)"; //$NON-NLS-1$
        Set<ElementSymbol> expected = new HashSet<ElementSymbol>(Arrays.asList(new ElementSymbol("e1"))); //$NON-NLS-1$
        assertEquals(expected, RulePushSelectCriteria.getElementsIncriteria(QueryParser.getQueryParser().parseCriteria(criteria)));
    }
    
    public void testElementsInCritieria1() throws Exception {
        String criteria = "e1 = '1' and ((e1 = '2' OR e1 = '4') AND e2 = 3) or e2 is null"; //$NON-NLS-1$
        Set<ElementSymbol> expected = new HashSet<ElementSymbol>(Arrays.asList(new ElementSymbol("e2"))); //$NON-NLS-1$
        assertEquals(expected, RulePushSelectCriteria.getElementsIncriteria(QueryParser.getQueryParser().parseCriteria(criteria)));
    }

}
