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

import org.teiid.dqp.internal.datamgr.language.GroupImpl;

import junit.framework.TestCase;

import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.unittest.FakeMetadataObject;

public class TestGroupImpl extends TestCase {

    /**
     * Constructor for TestGroupImpl.
     * @param name
     */
    public TestGroupImpl(String name) {
        super(name);
    }

    public static GroupSymbol helpExample(String groupName) {
        return helpExample(groupName, null);
    }
    
    public static GroupSymbol helpExample(String groupName, String definition) {
        String name = groupName;
        if (definition != null) {
            name = definition;
        }
        Object obj = new FakeMetadataObject(name, FakeMetadataObject.GROUP);
        return helpExample(groupName, definition, obj);
    }
    
    public static GroupSymbol helpExample(String groupName, String definition, Object metadataID) {
        GroupSymbol symbol = new GroupSymbol(groupName, definition);
        symbol.setMetadataID(metadataID);
        return symbol;
    }
    
    public static GroupImpl example(String groupName) throws Exception {
        return example(groupName, null);
    }

    public static GroupImpl example(String groupName, String definition) throws Exception {
        return (GroupImpl)TstLanguageBridgeFactory.factory.translate(helpExample(groupName, definition));
    }

    public static GroupImpl example(String groupName, String definition, Object metadataID) throws Exception {
        return (GroupImpl)TstLanguageBridgeFactory.factory.translate(helpExample(groupName, definition, metadataID));
    }

    public void testGetContext() throws Exception {
        Object metadataID = TstLanguageBridgeFactory.metadata.getGroupID("pm1.g1"); //$NON-NLS-1$
        assertEquals("x", example("x", "pm1.g1", metadataID).getContext()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testGetDefinition() throws Exception {
        Object metadataID = TstLanguageBridgeFactory.metadata.getGroupID("pm1.g1"); //$NON-NLS-1$
        assertEquals("pm1.g1", example("x", "pm1.g1", metadataID).getDefinition()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testEquals1() throws Exception {
        GroupImpl g1 = new GroupImpl("alias", "pm1.g1", null); //$NON-NLS-1$ //$NON-NLS-2$
        GroupImpl g2 = new GroupImpl("alias", "pm1.g1", null); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals(g1,g2);
    }

    public void testEquals2() throws Exception {
        GroupImpl g1 = new GroupImpl("alias", "model.group1", null); //$NON-NLS-1$ //$NON-NLS-2$
        GroupImpl g2 = new GroupImpl("alias", "model.group2", null); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals(g1,g2);
    }

    public void testEquals3() {
        GroupImpl g1 = new GroupImpl("alias1", "model.group", null); //$NON-NLS-1$ //$NON-NLS-2$
        GroupImpl g2 = new GroupImpl("alias2", "model.group", null); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertTrue(! g1.equals(g2));
    }

    public void testEquals4() {
        GroupImpl g1 = new GroupImpl("alias1", "model.group", null);   //$NON-NLS-1$ //$NON-NLS-2$
              
        assertEquals(g1, g1);
    }

    public void testEquals5() {
        GroupImpl g1 = new GroupImpl("model.group", null, null); //$NON-NLS-1$
        GroupImpl g2 = new GroupImpl("model.group", null, null); //$NON-NLS-1$
        
        assertEquals(g1, g2);
    }

    public void testEquals6() {
        GroupImpl g1 = new GroupImpl("model.gRoUp", null, null); //$NON-NLS-1$
        GroupImpl g2 = new GroupImpl("MoDeL.group", null, null); //$NON-NLS-1$
        
        assertEquals(g1, g2);
    }

    public void testEquals7() {
        GroupImpl g1 = new GroupImpl("model.gRoUp", "alias", null); //$NON-NLS-1$ //$NON-NLS-2$
        GroupImpl g2 = new GroupImpl("MoDeL.group", null, null); //$NON-NLS-1$
        
        assertTrue(! g1.equals(g2));
    }

}
