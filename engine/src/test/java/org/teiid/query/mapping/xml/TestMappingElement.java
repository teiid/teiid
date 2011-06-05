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

package org.teiid.query.mapping.xml;

import java.util.List;

import org.teiid.query.mapping.xml.MappingAllNode;
import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingChoiceNode;
import org.teiid.query.mapping.xml.MappingCommentNode;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingNodeConstants;
import org.teiid.query.mapping.xml.MappingRecursiveElement;
import org.teiid.query.mapping.xml.MappingSequenceNode;
import org.teiid.query.mapping.xml.Namespace;

import junit.framework.TestCase;


/** 
 * Test the Mapping Element 
 */
public class TestMappingElement extends TestCase {
   
    public void testRecusrive() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertFalse(element.isRecursive());
    }
    
    public void testNamespace() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertEquals("Test", element.getName()); //$NON-NLS-1$
        assertNull(element.getNamespacePrefix());
        
        Namespace namespace = new Namespace("foo", "protocol://someuri"); //$NON-NLS-1$ //$NON-NLS-2$
        element = new MappingElement("Test", namespace); //$NON-NLS-1$
        assertEquals("foo", element.getNamespacePrefix()); //$NON-NLS-1$
        assertEquals(element.getNamespaces().length, 0);
        
        element.addNamespace(namespace);
        assertEquals(element.getNamespaces().length, 1);
        
        element.addNamespace(new Namespace("ns1", "http://mm.com")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(element.getNamespaces().length, 2);
        
        // they may be switch some times, need a better way to test this?
        assertEquals("ns1", element.getNamespaces()[0].getPrefix()); //$NON-NLS-1$
        assertEquals("foo", element.getNamespaces()[1].getPrefix()); //$NON-NLS-1$
    }
    
    
    public void testNameInSource() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertNull(element.getNameInSource());
        
        element.setNameInSource(null);        
        assertNull(element.getNameInSource());
        
        element.setNameInSource("sourceName"); //$NON-NLS-1$
        assertEquals("sourceName", element.getNameInSource()); //$NON-NLS-1$
        
        element = new MappingElement("Test", "sourceName"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("sourceName", element.getNameInSource()); //$NON-NLS-1$        
    }
    
    public void testNillable() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertFalse(element.isNillable());

        element.setNillable(true);
        assertTrue(element.isNillable());
    }
    
    public void testDefaultValue() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertNull(element.getDefaultValue());

        element.setDefaultValue("foo"); //$NON-NLS-1$
        assertEquals("foo", element.getDefaultValue()); //$NON-NLS-1$
    }
    
    public void testFixedValue() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertNull(element.getValue());

        element.setValue("foo"); //$NON-NLS-1$
        assertEquals("foo", element.getValue()); //$NON-NLS-1$
    }    
    
    public void testOptional() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertFalse(element.isOptional());

        element.setOptional(true);
        assertTrue(element.isOptional());
    }
    
    public void testNormalizeText() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertEquals(MappingNodeConstants.Defaults.DEFAULT_NORMALIZE_TEXT, element.getNormalizeText());

        element.setNormalizeText("foo"); //$NON-NLS-1$
        assertEquals("foo", element.getNormalizeText()); //$NON-NLS-1$
    }    

    public void testType() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertNull(element.getType());

        element.setType("int"); //$NON-NLS-1$
        assertEquals("int", element.getType());//$NON-NLS-1$        
    }
    
    public void testInclude() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertTrue(element.isAlwaysInclude());

        element.setAlwaysInclude(false);
        assertFalse(element.isAlwaysInclude());
    }    
    
    public void testMinOccurence() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertEquals(1, element.getMinOccurence());

        element.setMinOccurrs(2);
        assertEquals(2, element.getMinOccurence());
    }    
    
    public void testMaxOccurence() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertEquals(1, element.getMaxOccurence());

        element.setMaxOccurrs(2);
        assertEquals(2, element.getMaxOccurence());
    }     
    
    public void testExclude() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertFalse(element.isExcluded());

        element.setExclude(true);
        assertTrue(element.isExcluded());
    }
    
    public void testSource() {        
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        assertNull(element.getSource());
    }     

    public void testChildren() {
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        
        element.addCommentNode(new MappingCommentNode("this is comment text")); //$NON-NLS-1$
        element.addAttribute(new MappingAttribute("foo", "fooReal")); //$NON-NLS-1$ //$NON-NLS-2$
        element.addChildElement(new MappingElement("child1")); //$NON-NLS-1$
        element.addChoiceNode(new MappingChoiceNode());
        element.addSequenceNode(new MappingSequenceNode());
        element.addAllNode(new MappingAllNode());
        
        assertEquals(5, element.getNodeChildren().size());
        assertEquals(1, element.getAttributes().size());
        
        List list = element.getNodeChildren();
        assertTrue(list.get(0) instanceof MappingCommentNode);
        assertTrue(list.get(1) instanceof MappingElement);
        assertTrue(list.get(2) instanceof MappingChoiceNode);
        assertTrue(list.get(3) instanceof MappingSequenceNode);
        assertTrue(list.get(4) instanceof MappingAllNode);
    }
    
    public void testAddRecursiveElement() {
        MappingDocument doc = new MappingDocument(false);
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$
        
        MappingElement nodeA = root.addChildElement(new MappingElement("NodeA", "test.nodea")); //$NON-NLS-1$ //$NON-NLS-2$
        nodeA.setSource("recursive.test"); //$NON-NLS-1$

        MappingSequenceNode seq = nodeA.addSequenceNode(new MappingSequenceNode());
        MappingElement nodeB = seq.addChildElement(new MappingElement("NodeB", "test.nodeb")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertFalse(nodeA.isRootRecursiveNode());
        
        MappingRecursiveElement rElem = (MappingRecursiveElement)nodeB.addChildElement(new MappingRecursiveElement("RecursiveNodeA", "recursive.test")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertTrue(rElem.isRecursive());
        
        assertTrue(nodeA.isRootRecursiveNode());
    }
    
    public void testAddBADRecursiveElement() {
        MappingDocument doc = new MappingDocument(false);
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$
        
        MappingElement nodeA = root.addChildElement(new MappingElement("NodeA", "test.nodea")); //$NON-NLS-1$ //$NON-NLS-2$
        nodeA.setSource("recursive.test"); //$NON-NLS-1$
        
        MappingSequenceNode seq = nodeA.addSequenceNode(new MappingSequenceNode());
        MappingElement nodeB = seq.addChildElement(new MappingElement("NodeB", "test.nodeb")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertFalse(nodeA.isRootRecursiveNode());
        
        try {
            nodeB.addChildElement(new MappingRecursiveElement("RecursiveNodeA", "unknown mapping")); //$NON-NLS-1$ //$NON-NLS-2$
            fail("should have failed to add a node with unknown mapping class"); //$NON-NLS-1$
        } catch (RuntimeException e) {
            //e.printStackTrace();
        }        
    }  
    
    public void testAddNullStagingTable() {        
        MappingElement element = new MappingElement("Test"); //$NON-NLS-1$
        element.addStagingTable(null);
        List<String> stagingTables = element.getStagingTables();
        assertTrue(stagingTables.isEmpty());
    } 
}
