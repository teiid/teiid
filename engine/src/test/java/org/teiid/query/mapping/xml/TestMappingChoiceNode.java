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

import org.teiid.query.mapping.xml.MappingAllNode;
import org.teiid.query.mapping.xml.MappingChoiceNode;
import org.teiid.query.mapping.xml.MappingCriteriaNode;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingSequenceNode;

import junit.framework.TestCase;


/** 
 * Test class for MappingChoiceNode
 */
public class TestMappingChoiceNode extends TestCase {

    public void testChoice() {
        MappingChoiceNode choice = new MappingChoiceNode();
        assertFalse(choice.throwExceptionOnDefault());
        
        choice = new MappingChoiceNode(true);
        assertTrue(choice.throwExceptionOnDefault());        
    }
    
    public void testAddWrongChoice() {
        
        MappingChoiceNode choice = new MappingChoiceNode();
        
        try {
            choice.addAllNode(new MappingAllNode());
            fail("must have failed to add"); //$NON-NLS-1$
        }catch(RuntimeException e) {            
        }
        
        try {
            choice.addSequenceNode(new MappingSequenceNode());
            fail("must have failed to add"); //$NON-NLS-1$
        }catch(RuntimeException e) {            
        }

        try {
            choice.addChildElement(new MappingElement("foo")); //$NON-NLS-1$
            fail("must have failed to add"); //$NON-NLS-1$
        }catch(RuntimeException e) {            
        }

        try {
            choice.addChoiceNode(new MappingChoiceNode());
            fail("must have failed to add"); //$NON-NLS-1$
        }catch(RuntimeException e) {            
        }            
    }
    
    
    public void testAddCriteria() {
        MappingChoiceNode choice = new MappingChoiceNode();
        
        choice.addCriteriaNode(new MappingCriteriaNode("chooseme > ?", false)); //$NON-NLS-1$ 
        
        // default node
        MappingCriteriaNode c2 = new MappingCriteriaNode(); 
        choice.addCriteriaNode(c2); 
        
       assertTrue(choice.getDefaultNode() == c2);
    }
    
    public void testWithoutCriteria() {
        MappingChoiceNode choice = new MappingChoiceNode();
        
        try {
            choice.addCriteriaNode(new MappingCriteriaNode().setAsDefault(false));  
            fail("must have failed to add"); //$NON-NLS-1$
        }catch(RuntimeException e) {
        }
        
    }
    
}
