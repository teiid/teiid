/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.core.util;

import org.teiid.internal.core.index.CharOperation;

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