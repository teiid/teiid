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

package org.teiid.dqp.internal.datamgr.language;

import org.teiid.dqp.internal.datamgr.language.SubqueryInCriteriaImpl;

import junit.framework.TestCase;

import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.symbol.ElementSymbol;

/**
 */
public class TestSubqueryInCriteriaImpl extends TestCase {

    /**
     * Constructor for TestSubqueryInCriteriaImpl.
     * @param name
     */
    public TestSubqueryInCriteriaImpl(String name) {
        super(name);
    }

    public static SubquerySetCriteria helpExample() {
        ElementSymbol element = TestElementImpl.helpExample("g1", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Query query = TestQueryImpl.helpExample();
        SubquerySetCriteria ssc = new SubquerySetCriteria(element, query);
        ssc.setNegated(true);
        return ssc;
    }
    
    public static SubqueryInCriteriaImpl example() throws Exception {
        return (SubqueryInCriteriaImpl)TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetExpression() throws Exception {
        assertNotNull(example().getLeftExpression());
    }

    public void testGetQuery() throws Exception {
        assertNotNull(example().getQuery());
    }
    
    public void testIsNegated() throws Exception {
        assertEquals("Wrong negation", true, example().isNegated()); //$NON-NLS-1$
    }
}
