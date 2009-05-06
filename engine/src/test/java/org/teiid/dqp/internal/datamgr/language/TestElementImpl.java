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

import junit.framework.TestCase;

import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.metadata.runtime.Element;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;

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
        FakeMetadataObject obj = new FakeMetadataObject(groupName + "." + elementName, FakeMetadataObject.ELEMENT); //$NON-NLS-1$
        obj.putProperty(FakeMetadataObject.Props.GROUP, new FakeMetadataObject(groupName, FakeMetadataObject.GROUP));
        obj.putProperty(FakeMetadataObject.Props.LENGTH, "3"); //$NON-NLS-1$
        symbol.setMetadataID(obj);
        return symbol;
        
    }
    
    public static ElementSymbol helpIntExample(String groupName, String elementName) {
        ElementSymbol symbol = new ElementSymbol(elementName);
        symbol.setType(Integer.class);
        symbol.setGroupSymbol(TestGroupImpl.helpExample(groupName));
        FakeMetadataObject obj = new FakeMetadataObject(groupName + "." + elementName, FakeMetadataObject.ELEMENT); //$NON-NLS-1$
        obj.putProperty(FakeMetadataObject.Props.GROUP, new FakeMetadataObject(groupName, FakeMetadataObject.GROUP));
        obj.putProperty(FakeMetadataObject.Props.LENGTH, "3"); //$NON-NLS-1$
        symbol.setMetadataID(obj);
        return symbol;
        
    }
    
    public static ElementSymbol helpExample(String groupName, String elementName, Object metadataID) {
        ElementSymbol symbol = new ElementSymbol(elementName);
        symbol.setGroupSymbol(TestGroupImpl.helpExample(groupName));
        symbol.setType(Integer.class);
        symbol.setMetadataID(metadataID);
        return symbol;
    }
    
    public static ElementImpl example(String groupName, String elementName) throws Exception {
        return (ElementImpl)TstLanguageBridgeFactory.factory.translate(helpExample(groupName, elementName));
    }
    
    public static ElementImpl example(String groupName, String elementName, Object metadataID) throws Exception {
        return (ElementImpl)TstLanguageBridgeFactory.factory.translate(helpExample(groupName, elementName, metadataID));
    }
    
    public void testGetName() throws Exception {
        Object metadataID = TstLanguageBridgeFactory.metadata.getElementID("pm1.g1.e1"); //$NON-NLS-1$
        assertEquals("e1", example("pm1.g1", "e1", metadataID).getName()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testGetGroup() throws Exception {
        Object metadataID = TstLanguageBridgeFactory.metadata.getElementID("pm1.g1.e1"); //$NON-NLS-1$
        assertNotNull(example("pm1.g1", "e1", metadataID).getGroup()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testGetMetadataID() throws Exception {
        FakeMetadataObject group = FakeMetadataFactory.createPhysicalGroup("pm1.g1", FakeMetadataFactory.createPhysicalModel("pm1.g1")); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject metadataID = FakeMetadataFactory.createElement("e", group, DataTypeManager.DefaultDataTypes.STRING, 0); //$NON-NLS-1$
        assertNotNull(example("pm1.g1", "e", metadataID).getMetadataObject()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testGetType() throws Exception {
        Object metadataID = TstLanguageBridgeFactory.metadata.getElementID("pm1.g1.e2"); //$NON-NLS-1$
        assertTrue(example("pm1.g1", "e2", metadataID).getType().equals(Integer.class)); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testSetMetadataID() throws Exception {
        Object metadataID = TstLanguageBridgeFactory.metadata.getElementID("pm1.g1.e1"); //$NON-NLS-1$
        ElementImpl element = example("pm1.g1", "e1", metadataID); //$NON-NLS-1$ //$NON-NLS-2$
        Element elem = TstLanguageBridgeFactory.metadataFactory.getElement(metadataID);
        element.setMetadataObject(elem);
        assertNotNull(element.getMetadataObject());
        assertEquals(elem, element.getMetadataObject());
    }
    
    public void helpTestEquals(IElement e1, IElement e2, boolean equal) {
        boolean actual = e1.equals(e2);
        boolean actual2 = e2.equals(e1);
        
        assertEquals("e1.equals(e2) != e2.equals(e1)", actual, actual2); //$NON-NLS-1$
        assertEquals("Did not get expected equal value", equal, actual); //$NON-NLS-1$
    }
    
    public IGroup createGroup(String context, String definition) {
        return new GroupImpl(context, definition, null);
    }
    
    public IElement createElement(IGroup group, String name) {
        return new ElementImpl(group, name, null, String.class);
    }
    
    public void testEquals1() {
        IElement e1 = createElement(createGroup("a", "m.g"), "e1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        helpTestEquals(e1, e1, true);
    }

    public void testEquals2() {
        IElement e1 = createElement(createGroup("a", "m.g"), "e1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        IElement e2 = createElement(createGroup("a", "m.g"), "e2"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        helpTestEquals(e1, e2, false);
    }

    public void testEquals3() {
        IElement e1 = createElement(createGroup("a", "m.g1"), "e1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        IElement e2 = createElement(createGroup("a", "m.g2"), "e2"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        helpTestEquals(e1, e2, false);
    }

    public void testEquals4() {
        IElement e1 = createElement(createGroup("a", "m.g"), "e1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        IElement e2 = createElement(createGroup("b", "m.g"), "e2"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        helpTestEquals(e1, e2, false);
    }

    public void testEquals5() {
        IElement e1 = createElement(createGroup("m.g1", null), "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        IElement e2 = createElement(createGroup("m.g1", null), "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTestEquals(e1, e2, true);
    }

    public void testEquals6() {
        IElement e1 = createElement(createGroup("a", "M.g"), "e1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        IElement e2 = createElement(createGroup("a", "m.G"), "E1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        helpTestEquals(e1, e2, true);
    }

    public void testEquals7() {
        IElement e1 = createElement(createGroup("a", "m.g"), "m.g.e1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        IElement e2 = createElement(createGroup("a", "m.g"), "e1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        helpTestEquals(e1, e2, true);
    }

}
