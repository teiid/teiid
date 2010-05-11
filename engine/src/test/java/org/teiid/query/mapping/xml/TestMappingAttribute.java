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

import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingNodeConstants;

import junit.framework.TestCase;


/** 
 * Test class for MappingAttribute
 */
public class TestMappingAttribute extends TestCase {
    
    public void testNamespace() {
        MappingAttribute attribute = new MappingAttribute("Test"); //$NON-NLS-1$
        assertEquals("Test", attribute.getName()); //$NON-NLS-1$
        assertNull(attribute.getNamespacePrefix());        
    }
    
    public void testNameInSource() {
        MappingAttribute attribute = new MappingAttribute("Test"); //$NON-NLS-1$
        assertNull(attribute.getNameInSource());
        
        attribute.setNameInSource(null);        
        assertNull(attribute.getNameInSource());
        
        attribute.setNameInSource("sourceName"); //$NON-NLS-1$
        assertEquals("sourceName", attribute.getNameInSource()); //$NON-NLS-1$
        
        attribute = new MappingAttribute("Test", "sourceName"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("sourceName", attribute.getNameInSource()); //$NON-NLS-1$        
    }
        
    public void testDefaultValue() {
        MappingAttribute attribute = new MappingAttribute("Test"); //$NON-NLS-1$
        assertNull(attribute.getDefaultValue());

        attribute.setDefaultValue("foo"); //$NON-NLS-1$
        assertEquals("foo", attribute.getDefaultValue()); //$NON-NLS-1$
    }
    
    public void testFixedValue() {
        MappingAttribute attribute = new MappingAttribute("Test"); //$NON-NLS-1$
        assertNull(attribute.getValue());

        attribute.setValue("foo"); //$NON-NLS-1$
        assertEquals("foo", attribute.getValue()); //$NON-NLS-1$
    }    
    
    public void testOptional() {
        MappingAttribute attribute = new MappingAttribute("Test"); //$NON-NLS-1$
        assertFalse(attribute.isOptional());

        attribute.setOptional(true);
        assertTrue(attribute.isOptional());
    }
    
    public void testNormalizeText() {
        MappingAttribute attribute = new MappingAttribute("Test"); //$NON-NLS-1$
        assertEquals(MappingNodeConstants.Defaults.DEFAULT_NORMALIZE_TEXT, attribute.getNormalizeText());

        attribute.setNormalizeText("foo"); //$NON-NLS-1$
        assertEquals("foo", attribute.getNormalizeText()); //$NON-NLS-1$
    }    
 
    public void testInclude() {
        MappingAttribute attribute = new MappingAttribute("Test"); //$NON-NLS-1$
        assertFalse(attribute.isAlwaysInclude());

        attribute.setAlwaysInclude(true);
        assertTrue(attribute.isAlwaysInclude());
    }    
    
    
    public void testExclude() {
        MappingAttribute attribute = new MappingAttribute("Test"); //$NON-NLS-1$
        assertFalse(attribute.isExcluded());

        attribute.setExclude(true);
        assertTrue(attribute.isExcluded());
    }           
}
