/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.Arrays;
import java.util.Collection;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestMetadataInCriteria extends TestCase {
    // =========================================================================
    //                        F R A M E W O R K
    // =========================================================================

    /**
     * Constructor for TestMetadataInCriteria.
     * @param name
     */
    public TestMetadataInCriteria(String name) {
        super(name);
    }

    public void testFieldCaseFunction1() {
        Collection values = Arrays.asList(new String[]{"value1", "value2", "value3"}); //$NON-NLS-1$ //$NON-NLS-2$  //$NON-NLS-3$
        MetadataInCriteria criteria = new MetadataInCriteria("field", values); //$NON-NLS-1$
        criteria.setFieldFunction("UPPER"); //$NON-NLS-1$
        assertTrue(criteria.hasFieldWithCaseFunctions());        
    }
    
    public void testFieldCaseFunction2() {
        Collection values = Arrays.asList(new String[]{"value1", "value2", "value3"}); //$NON-NLS-1$ //$NON-NLS-2$  //$NON-NLS-3$
        MetadataInCriteria criteria = new MetadataInCriteria("field", values); //$NON-NLS-1$
        criteria.setFieldFunction("XYZ"); //$NON-NLS-1$
        assertTrue(!criteria.hasFieldWithCaseFunctions());        
    }    

    public void testGetLiteralCriteria() {
        Collection values = Arrays.asList(new String[]{"value1", "value2", "value3"}); //$NON-NLS-1$ //$NON-NLS-2$  //$NON-NLS-3$
        MetadataInCriteria criteria = new MetadataInCriteria("field", values); //$NON-NLS-1$
        assertEquals(3, criteria.getLiteralCriteria().size());
    }
}
