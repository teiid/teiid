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

package com.metamatrix.dqp.internal.datamgr.language;

import java.util.ArrayList;
import java.util.Iterator;

import org.teiid.connector.language.IElement;

import junit.framework.TestCase;

import com.metamatrix.query.sql.lang.BulkInsert;
import com.metamatrix.query.sql.symbol.GroupSymbol;

/**
 * A Testcase for Bulk Insert, this is a simple extension of regular insert
 */
public class TestBulkInsertImpl extends TestCase {

    /**
     * Constructor for TestBulkInsert.
     * @param name
     */
    public TestBulkInsertImpl(String name) {
        super(name);
    }
    
    public static BulkInsert helpExample(String groupName) {
        GroupSymbol group = TestGroupImpl.helpExample(groupName);
        ArrayList elements = new ArrayList();
        elements.add(TestElementImpl.helpExample(groupName, "e1")); //$NON-NLS-1$
        elements.add(TestElementImpl.helpExample(groupName, "e2")); //$NON-NLS-1$
        elements.add(TestElementImpl.helpExample(groupName, "e3")); //$NON-NLS-1$
        elements.add(TestElementImpl.helpExample(groupName, "e4")); //$NON-NLS-1$
        
        ArrayList rows = new ArrayList();
        for (int i =0; i < 4; i++) {
	        ArrayList values = new ArrayList();
	        values.add(TestLiteralImpl.helpExample(1));
	        values.add(TestLiteralImpl.helpExample(2));
	        values.add(TestLiteralImpl.helpExample(3));
	        values.add(TestLiteralImpl.helpExample(4));
	        rows.add(values);
        }
        
        return new BulkInsert(group,elements,rows);
    }
    

    public static BulkInsertImpl example(String groupName) throws Exception {
        return (BulkInsertImpl)TstLanguageBridgeFactory.factory.translate(helpExample(groupName));
        
    }

    public void testGetGroup() throws Exception {
        assertNotNull(example("a.b").getGroup()); //$NON-NLS-1$
    }

    public void testGetElements() throws Exception {
        BulkInsertImpl insert = example("a.b"); //$NON-NLS-1$
        assertNotNull(insert.getElements());
        assertEquals(4, insert.getElements().size());
        for (Iterator i = insert.getElements().iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof IElement);
        }
    }

    public void testGetRows() throws Exception {
        BulkInsertImpl insert = example("a.b"); //$NON-NLS-1$
        assertNotNull(insert.getRows());
        assertEquals(4, insert.getRows().size());
//        for (Iterator iterRow = insert.getRows().iterator(); iterRow.hasNext();) {
//            List row = (List) iterRow.next();
//            for (Iterator i = row.iterator(); i.hasNext();) {
//                assertTrue(i.next() instanceof IExpression);
//            }            
//        }
    }
}
