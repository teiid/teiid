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

package com.metamatrix.core.util;

import org.teiid.metadata.index.CharOperation;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestCharOperation extends TestCase {

    /**
     * Constructor for TestCharOperation.
     * @param name
     */
    public TestCharOperation(String name) {
        super(name);
    }
        
    public void testCaseSentivieMatchWithoutWildCards1() {
        String pattern = "MyStringWithOutWildCards"; //$NON-NLS-1$
        String name = "MyStringWithOutWildCards"; //$NON-NLS-1$
        assertTrue(CharOperation.match(pattern.toCharArray(), name.toCharArray(), true));
    }
    
    public void testCaseSentivieMatchWithoutWildCards2() {
        String pattern = "MyStringWithOutWildCards".toUpperCase(); //$NON-NLS-1$
        String name = "MyStringWithOutWildCards"; //$NON-NLS-1$
        assertTrue(!CharOperation.match(pattern.toCharArray(), name.toCharArray(), true));
    }
    
    public void testCaseSentivieMatchWithWildCards1() {
        String pattern = "MyString*Wild*"; //$NON-NLS-1$
        String name = "MyStringWithWildCards"; //$NON-NLS-1$
        assertTrue(CharOperation.match(pattern.toCharArray(), name.toCharArray(), true));
    }
    
    public void testCaseSentivieMatchWithWildCards2() {
        String pattern = "MyString????Wil?Cards"; //$NON-NLS-1$
        String name = "MyStringWithWildCards"; //$NON-NLS-1$
        assertTrue(CharOperation.match(pattern.toCharArray(), name.toCharArray(), true));
    }
    
    public void testCaseSentivieMatchWithWildCards3() {
        String pattern = "MyString*WildCards".toUpperCase(); //$NON-NLS-1$
        String name = "MyStringWithWildCards"; //$NON-NLS-1$
        assertTrue(!CharOperation.match(pattern.toCharArray(), name.toCharArray(), true));
    }
    
    public void testCaseSentivieMatchWithWildCards4() {
        String pattern = "MyString????WildCards".toUpperCase(); //$NON-NLS-1$
        String name = "MyStringWithWildCards"; //$NON-NLS-1$
        assertTrue(!CharOperation.match(pattern.toCharArray(), name.toCharArray(), true));
    }
    
    public void testCaseSentivieMatchWithWildCards5() {
        String pattern = "*Supplie?"; //$NON-NLS-1$
        String name = "PartsSupplier"; //$NON-NLS-1$
        assertTrue(CharOperation.match(pattern.toCharArray(), name.toCharArray(), true));
    }
    
    public void testCaseSentivieMatchWithWildCards6() {
        String pattern = "*Supplie*"; //$NON-NLS-1$
        String name = "PartsSupplier"; //$NON-NLS-1$
        assertTrue(CharOperation.match(pattern.toCharArray(), name.toCharArray(), true));
    }
    
    public void testCaseInSentivieMatchWithoutWildCards() {
        String pattern = "MyStringWithOutWildCards".toUpperCase(); //$NON-NLS-1$
        String name = "MyStringWithOutWildCards"; //$NON-NLS-1$
        assertTrue(CharOperation.match(pattern.toCharArray(), name.toCharArray(), false));
    }

    public void testCaseInSentivieMatchWithWildCards1() {
        String pattern = "MyString*WildCards".toUpperCase(); //$NON-NLS-1$
        String name = "MyStringWithWildCards"; //$NON-NLS-1$
        assertTrue(CharOperation.match(pattern.toCharArray(), name.toCharArray(), false));
    }
    
    public void testCaseInSentivieMatchWithWildCards2() {
        String pattern = "MyString????WildCards".toUpperCase(); //$NON-NLS-1$
        String name = "MyStringWithWildCards"; //$NON-NLS-1$
        assertTrue(CharOperation.match(pattern.toCharArray(), name.toCharArray(), false));
    }
}