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

package org.teiid.query.optimizer.xml;

import java.util.Properties;

import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.optimizer.xml.HandleNillableVisitor;

import junit.framework.TestCase;



/** 
 * 
 */
public class TestHandleNillableNode extends TestCase {
    
    public void testNillableNode() {
        MappingDocument doc = FakeXMLMetadata.docWithExcluded();
        MappingElement root = (MappingElement)doc.getRootNode();
        
        Properties names = root.getNamespacesAsProperties();
        assertFalse(names.containsKey("xsi"));//$NON-NLS-1$
        
        HandleNillableVisitor.execute(doc);
        
        names = root.getNamespacesAsProperties();
        assertTrue(names.containsKey("xsi"));//$NON-NLS-1$
    }
}
