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

package com.metamatrix.common.util;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestVDBNameValidator extends TestCase {

    public TestVDBNameValidator(String name) {
        super(name);
    }

    public final void testValidName1() {
        assertTrue(VDBNameValidator.isValid("ValidName")); //$NON-NLS-1$
    }

    public final void testValidName2() {
        assertTrue(VDBNameValidator.isValid("System_1")); //$NON-NLS-1$
    }

    public final void testValidName3() {
        assertTrue(VDBNameValidator.isValid("Admin__")); //$NON-NLS-1$
    }

    public final void testValidNameWithUnderscore() {
        assertTrue(VDBNameValidator.isValid("Valid_Name")); //$NON-NLS-1$
    }
    
    public final void testAllValidCharacters() {
        assertTrue(VDBNameValidator.isValid("abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")); //$NON-NLS-1$
    }

    public final void testInvalidName1() {
        assertFalse(VDBNameValidator.isValid("InvalidName`")); //$NON-NLS-1$
    }

    public final void testInvalidName2() {
        assertFalse(VDBNameValidator.isValid("Inalid/Name")); //$NON-NLS-1$
    }

    public final void testInvalidName3() {
        assertFalse(VDBNameValidator.isValid("Invalid-Name")); //$NON-NLS-1$
    }

    public final void testInvalidName4() {
        assertFalse(VDBNameValidator.isValid("Invalid?Name")); //$NON-NLS-1$
    }

    public final void testNameThatStartWithDigit() {
        assertFalse(VDBNameValidator.isValid("1VDBName")); //$NON-NLS-1$
    }

    public final void testNameThatStartWithUnderscore() {
        assertFalse(VDBNameValidator.isValid("_VDBName")); //$NON-NLS-1$
    }

    public final void testEmptyVDBName() {
        assertFalse(VDBNameValidator.isValid("")); //$NON-NLS-1$
    }

    public final void testNullName() {
        assertFalse(VDBNameValidator.isValid(null)); 
    }

    public final void testReservedSystemName() {
        assertFalse(VDBNameValidator.isValid("System")); //$NON-NLS-1$
    }

    public final void testReservedHelpName() {
        assertFalse(VDBNameValidator.isValid("System")); //$NON-NLS-1$
    }

    public final void testReservedAdminName() {
        assertFalse(VDBNameValidator.isValid("System")); //$NON-NLS-1$
    }
   
    
}
