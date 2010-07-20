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


import org.teiid.language.IsNull;
import org.teiid.query.sql.lang.IsNullCriteria;


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
    
    public static IsNull example(boolean negated) throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample(negated));
    }

    public void testGetExpression() throws Exception {
        assertNotNull(example(false).getExpression());
    }

    public void testIsNegated() throws Exception {
        assertTrue(example(true).isNegated());
        assertFalse(example(false).isNegated());
    }

}
