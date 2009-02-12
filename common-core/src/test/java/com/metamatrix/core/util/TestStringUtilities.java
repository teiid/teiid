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

//################################################################################################################################
package com.metamatrix.core.util;

import junit.framework.TestCase;

/**
 * @author Lance Phillips
 *
 * @since 3.1
 */
public class TestStringUtilities extends TestCase {

    /**
     * Constructor for TestStringUtilities.
     * @param name
     */
    public TestStringUtilities(String name) {
        super(name);
    }
    
    /**
     * Constructor for TestStringUtilities.
     * @param name
     */
    public TestStringUtilities() {
        this("TestStringUtilities"); //$NON-NLS-1$
    }
    
    public void testGetLastUpperCharToken(){
        String testString = "getSuperDuperTypes"; //$NON-NLS-1$
        String result = StringUtilities.getLastUpperCharToken(testString);
        
        if(!result.equals("Types") ){ //$NON-NLS-1$
            fail("Expected \"Types\" but got " + result); //$NON-NLS-1$
        }
    }
    
    public void testGetLastUpperCharTokenComplex(){
        String testString = "getSuperDuperTypes"; //$NON-NLS-1$
        String result = StringUtilities.getLastUpperCharToken(testString);
        
        result = StringUtilities.getLastUpperCharToken(testString, result);
        if(!result.equals("DuperTypes") ){ //$NON-NLS-1$
            fail("Expected \"DuperTypes\" but got " + result); //$NON-NLS-1$
        }
        
        result = StringUtilities.getLastUpperCharToken(testString, result);
        if(!result.equals("SuperDuperTypes") ){ //$NON-NLS-1$
            fail("Expected \"SuperDuperTypes\" but got " + result); //$NON-NLS-1$
        }
    }

}
