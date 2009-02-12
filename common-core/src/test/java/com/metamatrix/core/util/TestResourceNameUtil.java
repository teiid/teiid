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

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestResourceNameUtil extends TestCase {
    
    private static final String NON_RESERVED_VDB_NAME               = "SomeVdb";                //$NON-NLS-1$
    private static final String NON_RESERVED_VDB_NAME_WITH_EXT      = "SomeVdb.vdb";            //$NON-NLS-1$
    private static final String RESERVED_VDB_NAME                   = "System";                 //$NON-NLS-1$
    private static final String RESERVED_VDB_NAME_WITH_EXT          = "System.vdb";             //$NON-NLS-1$
    private static final String ILLEGAL_VDB_EXTENSION_NAME          = "SomeVdb.someExtension";  //$NON-NLS-1$
    private static final String RESERVED_VDB_NAME_WITH_EXT_WITH_DOTS = "System.a.b.vdb";  //$NON-NLS-1$
    
    private static final String NON_RESERVED_XMI_NAME               = "SomeModel";                  //$NON-NLS-1$
    private static final String NON_RESERVED_XMI_NAME_WITH_EXT      = "SomeModel.xmi";              //$NON-NLS-1$
    private static final String RESERVED_XMI_NAME                   = "System";                     //$NON-NLS-1$
    private static final String RESERVED_XMI_NAME_WITH_EXT          = "System.xmi";                 //$NON-NLS-1$
    private static final String ILLEGAL_XMI_EXTENSION_NAME          = "SomeModel.someExtension";    //$NON-NLS-1$
    private static final String RESERVED_XMI_NAME_WITH_EXT_WITH_DOTS = "System.a.b.xmi";  //$NON-NLS-1$
 
    private static final String NON_RESERVED_XSD_NAME               = "SomeSchema";                  //$NON-NLS-1$
    private static final String NON_RESERVED_XSD_NAME_WITH_EXT      = "SomeSchema.xsd";              //$NON-NLS-1$
    private static final String RESERVED_XSD_NAME                   = "SystemSchema";                //$NON-NLS-1$
    private static final String RESERVED_XSD_NAME_WITH_EXT          = "SystemSchema.xsd";            //$NON-NLS-1$
    private static final String ILLEGAL_XSD_EXTENSION_NAME          = "SomeSchema.someExtension";    //$NON-NLS-1$
    private static final String RESERVED_XSD_NAME_WITH_EXT_WITH_DOTS = "SystemSchema.a.b.xsd";  //$NON-NLS-1$
    
    private static final String MSG_EXPECTED = "Expected: "; //$NON-NLS-1$
    private static final String MSG_VDB_NAME = "VDB Name ["; //$NON-NLS-1$
    private static final String MSG_XSD_NAME = "XSD Name ["; //$NON-NLS-1$
    private static final String MSG_XMI_NAME = "Xmi Model Name ["; //$NON-NLS-1$
    private static final String MSG_RESOURCE_NAME = "XSD Name ["; //$NON-NLS-1$
    private static final String MSG_IS_NOT_RESERVED = "] is NOT RESERVED"; //$NON-NLS-1$
    private static final String MSG_IS_RESERVED = "] IS RESERVED"; //$NON-NLS-1$
    private static final String MSG_THROW_ILLEGAL_ARG_EXC = "] to throw IllegalArgumentException"; //$NON-NLS-1$
    
    /** 
     * 
     * @since 4.3
     */
    public TestResourceNameUtil() {
        super();
    }

    /** 
     * @param theName
     * @since 4.3
     */
    public TestResourceNameUtil(String theName) {
        super(theName);
    }
      
    // ------------------------------------------------
    // Test for VDB names
    // ------------------------------------------------
    public void testNonReservedVdbNameWithoutExtension() {
        assertFalse(MSG_EXPECTED + MSG_VDB_NAME + 
                                                      NON_RESERVED_VDB_NAME + 
                   MSG_IS_NOT_RESERVED,
                   ResourceNameUtil.isReservedVdbName(NON_RESERVED_VDB_NAME));
    }
    
    public void testNonReservedVdbNameWithExtension() {
        assertFalse(MSG_EXPECTED + MSG_VDB_NAME + 
                                                      NON_RESERVED_VDB_NAME_WITH_EXT + 
                   MSG_IS_NOT_RESERVED,
                   ResourceNameUtil.isReservedVdbName(NON_RESERVED_VDB_NAME_WITH_EXT));
    }
    
    public void testReservedVdbNameWithoutExtension() {
        assertTrue(MSG_EXPECTED + MSG_VDB_NAME + 
                                                      RESERVED_VDB_NAME + 
                   MSG_IS_RESERVED,
                   ResourceNameUtil.isReservedVdbName(RESERVED_VDB_NAME));
    }
    
    public void testReservedVdbNameWithExtension() {
        assertTrue(MSG_EXPECTED + MSG_VDB_NAME + 
                                                      RESERVED_VDB_NAME_WITH_EXT + 
                   MSG_IS_RESERVED,
                   ResourceNameUtil.isReservedVdbName(RESERVED_VDB_NAME_WITH_EXT));
    }
    
    public void testFailIllegalVdbName() {
        try {
            ResourceNameUtil.isReservedVdbName(ILLEGAL_VDB_EXTENSION_NAME);
            fail(MSG_EXPECTED + MSG_VDB_NAME + ILLEGAL_VDB_EXTENSION_NAME + MSG_THROW_ILLEGAL_ARG_EXC);
        } catch (IllegalArgumentException e) {
        }
    }
    
    public void testReservedVdbNameWithExtensionAndDots() {
        assertFalse(MSG_EXPECTED + MSG_VDB_NAME + 
                                                      RESERVED_VDB_NAME_WITH_EXT_WITH_DOTS + 
                   MSG_IS_NOT_RESERVED,
                   ResourceNameUtil.isReservedVdbName(RESERVED_VDB_NAME_WITH_EXT_WITH_DOTS));
    }
    
    // ------------------------------------------------
    // Test for XMI model names
    // ------------------------------------------------
    public void testNonReservedXmiNameWithoutExtension() {
        assertFalse(MSG_EXPECTED + MSG_XMI_NAME + 
                                                      NON_RESERVED_XMI_NAME + 
                   MSG_IS_NOT_RESERVED,
                   ResourceNameUtil.isReservedModelName(NON_RESERVED_XMI_NAME));
    }
    
    public void testNonReservedXmiNameWithExtension() {
        assertFalse(MSG_EXPECTED + MSG_XMI_NAME + 
                                                      NON_RESERVED_XMI_NAME_WITH_EXT + 
                   MSG_IS_NOT_RESERVED,
                   ResourceNameUtil.isReservedModelName(NON_RESERVED_XMI_NAME_WITH_EXT));
    }
    
    public void testReservedXmiNameWithoutExtension() {
        assertTrue(MSG_EXPECTED + MSG_XMI_NAME + 
                                                      RESERVED_XMI_NAME + 
                   MSG_IS_RESERVED,
                   ResourceNameUtil.isReservedModelName(RESERVED_XMI_NAME));
    }
    
    public void testReservedXmiNameWithExtension() {
        assertTrue(MSG_EXPECTED + MSG_XMI_NAME + 
                                                      RESERVED_XMI_NAME_WITH_EXT + 
                   MSG_IS_RESERVED,
                   ResourceNameUtil.isReservedModelName(RESERVED_XMI_NAME_WITH_EXT));
    }
    
    public void testFailIllegalXmiName() {
        try {
            ResourceNameUtil.isReservedModelName(ILLEGAL_XMI_EXTENSION_NAME);
            fail(MSG_EXPECTED + MSG_XSD_NAME + ILLEGAL_XMI_EXTENSION_NAME + MSG_THROW_ILLEGAL_ARG_EXC);
        } catch (IllegalArgumentException e) {
        }
    }
    
    public void testReservedXmiNameWithExtensionAndDots() {
        assertFalse(MSG_EXPECTED + MSG_VDB_NAME + 
                                                      RESERVED_XMI_NAME_WITH_EXT_WITH_DOTS + 
                   MSG_IS_NOT_RESERVED,
                   ResourceNameUtil.isReservedModelName(RESERVED_XMI_NAME_WITH_EXT_WITH_DOTS));
    }
    
    // ------------------------------------------------
    // Test for XSD model names
    // ------------------------------------------------
    public void testNonReservedXsdNameWithoutExtension() {
        assertFalse(MSG_EXPECTED + MSG_XSD_NAME + 
                                                      NON_RESERVED_XSD_NAME + 
                   MSG_IS_NOT_RESERVED,
                   ResourceNameUtil.isReservedSchemaName(NON_RESERVED_XSD_NAME));
    }
    
    public void testNonReservedXsdNameWithExtension() {
        assertFalse(MSG_EXPECTED + MSG_XSD_NAME + 
                                                      NON_RESERVED_XSD_NAME_WITH_EXT + 
                   MSG_IS_NOT_RESERVED,
                   ResourceNameUtil.isReservedSchemaName(NON_RESERVED_XSD_NAME_WITH_EXT));
    }
    
    public void testReservedXsdNameWithoutExtension() {
        assertTrue(MSG_EXPECTED + MSG_XSD_NAME + 
                                                      RESERVED_XSD_NAME + 
                   MSG_IS_RESERVED,
                   ResourceNameUtil.isReservedSchemaName(RESERVED_XSD_NAME));
    }
    
    public void testReservedXsdNameWithExtension() {
        assertTrue(MSG_EXPECTED + MSG_XSD_NAME + 
                                                      RESERVED_XSD_NAME_WITH_EXT + 
                   MSG_IS_RESERVED,
                   ResourceNameUtil.isReservedSchemaName(RESERVED_XSD_NAME_WITH_EXT));
    }
    
    public void testFailIllegalXsdName() {
        try {
            ResourceNameUtil.isReservedSchemaName(ILLEGAL_XSD_EXTENSION_NAME);
            fail(MSG_EXPECTED + MSG_XSD_NAME + ILLEGAL_XSD_EXTENSION_NAME + MSG_THROW_ILLEGAL_ARG_EXC);
        } catch (IllegalArgumentException e) {
        }
    }
    
    public void testReservedXsdNameWithExtensionAndDots() {
        assertFalse(MSG_EXPECTED + MSG_VDB_NAME + 
                                                      RESERVED_XSD_NAME_WITH_EXT_WITH_DOTS + 
                   MSG_IS_NOT_RESERVED,
                   ResourceNameUtil.isReservedSchemaName(RESERVED_XSD_NAME_WITH_EXT_WITH_DOTS));
    }
    
    
    // ------------------------------------------------
    // Test for RESOURCE names
    // ------------------------------------------------
    public void testNonReservedResourceNameWithoutExtension() {
        assertFalse(MSG_EXPECTED + MSG_RESOURCE_NAME + 
                    NON_RESERVED_VDB_NAME + 
                    MSG_IS_NOT_RESERVED,
                    ResourceNameUtil.isReservedResourceName(NON_RESERVED_VDB_NAME));
    }
    
    public void testNonReservedResourceNameWithExtension() {
        assertFalse(MSG_EXPECTED + MSG_RESOURCE_NAME + 
                    NON_RESERVED_VDB_NAME_WITH_EXT + 
                    MSG_IS_NOT_RESERVED,
                    ResourceNameUtil.isReservedResourceName(NON_RESERVED_VDB_NAME_WITH_EXT));
    }
    
    public void testReservedResourceNameWithoutExtension() {
        assertTrue(MSG_EXPECTED + MSG_RESOURCE_NAME + 
                    RESERVED_XSD_NAME + 
                    MSG_IS_RESERVED,
                    ResourceNameUtil.isReservedResourceName(RESERVED_XSD_NAME));
    }
    
    public void testReservedResourceNameWithExtension() {
        assertTrue(MSG_EXPECTED + MSG_RESOURCE_NAME + 
                    RESERVED_VDB_NAME_WITH_EXT + 
                    MSG_IS_RESERVED,
                    ResourceNameUtil.isReservedResourceName(RESERVED_VDB_NAME_WITH_EXT));
    }
    
    public void testFailIllegalResourceName() {
        try {
            ResourceNameUtil.isReservedResourceName(ILLEGAL_VDB_EXTENSION_NAME);
            fail(MSG_EXPECTED + MSG_RESOURCE_NAME + ILLEGAL_VDB_EXTENSION_NAME + MSG_THROW_ILLEGAL_ARG_EXC);
        } catch (IllegalArgumentException e) {
        }
    }
    
    public void testReservedResourceNameWithExtensionAndDots() {
        assertFalse(MSG_EXPECTED + MSG_VDB_NAME + 
                                                      RESERVED_XSD_NAME_WITH_EXT_WITH_DOTS + 
                   MSG_IS_NOT_RESERVED,
                   ResourceNameUtil.isReservedResourceName(RESERVED_XSD_NAME_WITH_EXT_WITH_DOTS));
    }
}
