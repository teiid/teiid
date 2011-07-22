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

import org.teiid.language.ColumnReference;
import org.teiid.language.NamedTable;
import org.teiid.query.sql.symbol.ElementSymbol;


public class TestElementImpl extends TestCase {

    /**
     * Constructor for TestElementImpl.
     * @param name
     */
    public TestElementImpl(String name) {
        super(name);
    }

    public static ElementSymbol helpExample(String groupName, String elementName) {
        ElementSymbol symbol = new ElementSymbol(elementName);
        symbol.setType(String.class);
        symbol.setGroupSymbol(TestGroupImpl.helpExample(groupName));
        return symbol;
        
    }
    
    public static ElementSymbol helpIntExample(String groupName, String elementName) {
        ElementSymbol symbol = new ElementSymbol(elementName);
        symbol.setType(Integer.class);
        symbol.setGroupSymbol(TestGroupImpl.helpExample(groupName));
        return symbol;
    }
    
    public static ElementSymbol helpExample(String groupName, String elementName, Object metadataID) {
        ElementSymbol symbol = new ElementSymbol(elementName);
        symbol.setGroupSymbol(TestGroupImpl.helpExample(groupName));
        symbol.setType(Integer.class);
        symbol.setMetadataID(metadataID);
        return symbol;
    }
    
    public static ColumnReference example(String groupName, String elementName) throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample(groupName, elementName));
    }
    
    public static ColumnReference example(String groupName, String elementName, Object metadataID) throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample(groupName, elementName, metadataID));
    }
    
    public void testGetName() throws Exception {
        Object metadataID = TstLanguageBridgeFactory.metadata.getElementID("pm1.g1.e1"); //$NON-NLS-1$
        assertEquals("e1", example("pm1.g1", "e1", metadataID).getName()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testGetGroup() throws Exception {
        Object metadataID = TstLanguageBridgeFactory.metadata.getElementID("pm1.g1.e1"); //$NON-NLS-1$
        assertNotNull(example("pm1.g1", "e1", metadataID).getTable()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testGetType() throws Exception {
        Object metadataID = TstLanguageBridgeFactory.metadata.getElementID("pm1.g1.e2"); //$NON-NLS-1$
        assertTrue(example("pm1.g1", "e2", metadataID).getType().equals(Integer.class)); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void helpTestEquals(ColumnReference e1, ColumnReference e2, boolean equal) {
        boolean actual = e1.equals(e2);
        boolean actual2 = e2.equals(e1);
        
        assertEquals("e1.equals(e2) != e2.equals(e1)", actual, actual2); //$NON-NLS-1$
        assertEquals("Did not get expected equal value", equal, actual); //$NON-NLS-1$
    }
    
    public NamedTable createGroup(String context, String definition) {
        return new NamedTable(context, definition, null);
    }
    
    public ColumnReference createElement(NamedTable group, String name) {
        return new ColumnReference(group, name, null, String.class);
    }
   
}
