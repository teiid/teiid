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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.ILiteral;
import org.teiid.dqp.internal.datamgr.language.InCriteriaImpl;

import com.metamatrix.query.sql.lang.SetCriteria;

import junit.framework.TestCase;

public class TestInCriteriaImpl extends TestCase {

    /**
     * Constructor for TestInCriteriaImpl.
     * @param name
     */
    public TestInCriteriaImpl(String name) {
        super(name);
    }
    
    public static SetCriteria helpExample(boolean negated) {
        ArrayList values = new ArrayList();
        values.add(TestLiteralImpl.helpExample(100));
        values.add(TestLiteralImpl.helpExample(200));
        values.add(TestLiteralImpl.helpExample(300));
        values.add(TestLiteralImpl.helpExample(400));
        SetCriteria crit = new SetCriteria(TestLiteralImpl.helpExample(300), values);
        crit.setNegated(negated);
        return crit;
    }
    
    public static InCriteriaImpl example(boolean negated) throws Exception {
        return (InCriteriaImpl)TstLanguageBridgeFactory.factory.translate(helpExample(negated));
    }

    public void testGetLeftExpression() throws Exception {
        InCriteriaImpl inCriteria = example(false);
        assertNotNull(inCriteria.getLeftExpression());
        assertTrue(inCriteria.getLeftExpression() instanceof ILiteral);
        assertEquals(new Integer(300), ((ILiteral)inCriteria.getLeftExpression()).getValue());
    }

    public void testGetRightExpressions() throws Exception {
        List values = example(false).getRightExpressions();
        assertNotNull(values);
        assertEquals(4, values.size());
        for (Iterator i = values.iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof IExpression);
        }
        
    }

    public void testIsNegated() throws Exception {
        assertTrue(example(true).isNegated());
        assertFalse(example(false).isNegated());
    }

}
