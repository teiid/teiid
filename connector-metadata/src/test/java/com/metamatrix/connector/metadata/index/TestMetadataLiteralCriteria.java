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

package com.metamatrix.connector.metadata.index;

import org.teiid.connector.metadata.MetadataLiteralCriteria;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestMetadataLiteralCriteria extends TestCase {
    // =========================================================================
    //                        F R A M E W O R K
    // =========================================================================

    /**
     * Constructor for TestMetadataLiteralCriteria.
     * @param name
     */
    public TestMetadataLiteralCriteria(String name) {
        super(name);
    }
    
    public void testEvaluatedValue1() {
        MetadataLiteralCriteria criteria = new MetadataLiteralCriteria("field", "value"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(criteria.getFieldValue(), criteria.getEvaluatedValue()); 
    }
    
    public void testEvaluatedValue2() {
        MetadataLiteralCriteria criteria = new MetadataLiteralCriteria("field", "value"); //$NON-NLS-1$ //$NON-NLS-2$
        criteria.setValueFunction("UPPER"); //$NON-NLS-1$
        assertEquals(criteria.getFieldValue().toString().toUpperCase(), criteria.getEvaluatedValue()); 
    }
    
    public void testFalseCriteria1() {
        MetadataLiteralCriteria criteria = new MetadataLiteralCriteria("field", "value"); //$NON-NLS-1$ //$NON-NLS-2$
        criteria.setFieldFunction("UPPER"); //$NON-NLS-1$
        assertTrue(criteria.isFalseCriteria());        
    }

    public void testFalseCriteria2() {
        MetadataLiteralCriteria criteria = new MetadataLiteralCriteria("field", "value".toUpperCase()); //$NON-NLS-1$ //$NON-NLS-2$
        criteria.setFieldFunction("UPPER"); //$NON-NLS-1$
        assertTrue(!criteria.isFalseCriteria());        
    }
    
    public void testFieldCaseFunction() {
        MetadataLiteralCriteria criteria = new MetadataLiteralCriteria("field", "value"); //$NON-NLS-1$ //$NON-NLS-2$
        criteria.setFieldFunction("UPPER"); //$NON-NLS-1$
        assertTrue(criteria.hasFieldWithCaseFunctions());        
    }    
}
