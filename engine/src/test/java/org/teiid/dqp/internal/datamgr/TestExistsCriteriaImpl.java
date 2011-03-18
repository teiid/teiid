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

package org.teiid.dqp.internal.datamgr;


import junit.framework.TestCase;

import org.teiid.language.Exists;
import org.teiid.language.Not;
import org.teiid.query.sql.lang.ExistsCriteria;

/**
 */
public class TestExistsCriteriaImpl extends TestCase {

    /**
     * Constructor for TestExistsCriteriaImpl.
     * @param name
     */
    public TestExistsCriteriaImpl(String name) {
        super(name);
    }

    public static ExistsCriteria helpExample(boolean negated) {
        ExistsCriteria crit = new ExistsCriteria(TestQueryImpl.helpExample(true));
        crit.setNegated(negated);
        return crit;
    }
    
    public static Exists example() throws Exception {
        return (Exists)TstLanguageBridgeFactory.factory.translate(helpExample(false));
    }

    public void testGetQuery() throws Exception {
        assertNotNull(example().getSubquery());    
    }
    
    public void testNegated() throws Exception {
        assertTrue(TstLanguageBridgeFactory.factory.translate(helpExample(true)) instanceof Not);    
    }
    
}
