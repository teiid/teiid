/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
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