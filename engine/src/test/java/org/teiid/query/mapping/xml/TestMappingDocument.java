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

import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingNodeConstants;
import org.teiid.query.mapping.xml.MappingSourceNode;

import junit.framework.TestCase;


/** 
 * Test class for MappingDocument
 */
public class TestMappingDocument extends TestCase {

    public void testDoc1() {
        MappingDocument doc = new MappingDocument(true);
        assertNull(doc.getRootNode());
        
        assertTrue(doc.isFormatted());
        assertEquals(MappingNodeConstants.Defaults.DEFAULT_DOCUMENT_ENCODING, doc.getDocumentEncoding());
    }
    
    public void testDoc2() {
        MappingDocument doc = new MappingDocument("UTF-16", false); //$NON-NLS-1$
        MappingElement root = new MappingElement("test"); //$NON-NLS-1$
        doc.addChildElement(root);
        assertNotNull(doc.getRootNode());
        
        assertFalse(doc.isFormatted());
        assertEquals("UTF-16", doc.getDocumentEncoding()); //$NON-NLS-1$
    }

    public void testDoc3() {
        MappingDocument doc = new MappingDocument("UTF-16", false); //$NON-NLS-1$
        MappingElement root = new MappingElement("test"); //$NON-NLS-1$
        root.setMinOccurrs(2);
        root.setMaxOccurrs(-1);
        
        doc.addChildElement(root);
        
        assertNotNull(doc.getRootNode());
        
        assertFalse(doc.isFormatted());
        assertEquals("UTF-16", doc.getDocumentEncoding()); //$NON-NLS-1$
        
        // the root can not re-occur more than once..
        root = (MappingElement)doc.getRootNode();
        assertEquals(1, root.getMinOccurence());
        assertEquals(1, root.getMaxOccurence());
    }
    
    public void testTagRoot() {
        MappingDocument doc = new MappingDocument("UTF-16", false); //$NON-NLS-1$
        MappingSourceNode source = new MappingSourceNode("source"); //$NON-NLS-1$
        doc.addSourceNode(source);
        MappingElement tagroot = new MappingElement("test"); //$NON-NLS-1$
        source.addChildElement(tagroot);
        
        assertTrue(doc.getRootNode() == source);
        assertTrue(doc.getTagRootElement() == tagroot);
        
    }    
    
}
