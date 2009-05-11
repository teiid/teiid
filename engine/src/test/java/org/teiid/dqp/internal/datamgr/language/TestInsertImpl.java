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

import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IInsertExpressionValueSource;
import org.teiid.dqp.internal.datamgr.language.InsertImpl;

import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.symbol.GroupSymbol;

import junit.framework.TestCase;

public class TestInsertImpl extends TestCase {

    /**
     * Constructor for TestInsertImpl.
     * @param name
     */
    public TestInsertImpl(String name) {
        super(name);
    }

    public static Insert helpExample(String groupName) {
        GroupSymbol group = TestGroupImpl.helpExample(groupName);
        ArrayList elements = new ArrayList();
        elements.add(TestElementImpl.helpExample(groupName, "e1")); //$NON-NLS-1$
        elements.add(TestElementImpl.helpExample(groupName, "e2")); //$NON-NLS-1$
        elements.add(TestElementImpl.helpExample(groupName, "e3")); //$NON-NLS-1$
        elements.add(TestElementImpl.helpExample(groupName, "e4")); //$NON-NLS-1$
        
        ArrayList values = new ArrayList();
        values.add(TestLiteralImpl.helpExample(1));
        values.add(TestLiteralImpl.helpExample(2));
        values.add(TestLiteralImpl.helpExample(3));
        values.add(TestLiteralImpl.helpExample(4));
        
        return new Insert(group,
                          elements,
                          values);
    }
    
    public static Insert helpExample2(String groupName) {
        GroupSymbol group = TestGroupImpl.helpExample(groupName);
        ArrayList elements = new ArrayList();
        elements.add(TestElementImpl.helpExample(groupName, "e1")); //$NON-NLS-1$
        
        ArrayList values = new ArrayList();
        values.add(TestSearchedCaseExpressionImpl.helpExample());
        
        return new Insert(group,
                          elements,
                          values);
    }
  
    public static InsertImpl example(String groupName) throws Exception {
        return (InsertImpl)TstLanguageBridgeFactory.factory.translate(helpExample(groupName));
        
    }
    public static InsertImpl example2(String groupName) throws Exception {
        return (InsertImpl)TstLanguageBridgeFactory.factory.translate(helpExample2(groupName));
        
    }
    public void testGetGroup() throws Exception {
        assertNotNull(example("a.b").getGroup()); //$NON-NLS-1$
    }

    public void testGetElements() throws Exception {
        InsertImpl insert = example("a.b"); //$NON-NLS-1$
        assertNotNull(insert.getElements());
        assertEquals(4, insert.getElements().size());
        for (Iterator i = insert.getElements().iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof IElement);
        }

        // verify that elements are not qualified by group
        String sInsertSQL = insert.toString();
        assertTrue(sInsertSQL.indexOf( '.') == -1 );                        
    }

    public void testGetValues() throws Exception {
        InsertImpl insert = example("a.b"); //$NON-NLS-1$
        assertNotNull(insert.getValueSource());
        assertEquals(4, ((IInsertExpressionValueSource)insert.getValueSource()).getValues().size());
        for (Iterator i = ((IInsertExpressionValueSource)insert.getValueSource()).getValues().iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof IExpression);
        }
    }
    
    public void testExpressionsInInsert() throws Exception {
        InsertImpl insert = example2("a.b"); //$NON-NLS-1$
        assertNotNull(insert.getElements());
        assertEquals(1, insert.getElements().size());
        for (Iterator i = insert.getElements().iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof IElement);
        }
        assertNotNull(insert.getValueSource());
        assertEquals(1, ((IInsertExpressionValueSource)insert.getValueSource()).getValues().size());
        for (Iterator i = ((IInsertExpressionValueSource)insert.getValueSource()).getValues().iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof IExpression);
        }
    }

}
