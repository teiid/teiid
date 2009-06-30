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

package com.metamatrix.admin.api.objects;

import org.teiid.adminapi.AdminOptions;

import junit.framework.TestCase;



/** 
 * @since 4.3
 */
public class TestAdminOptions extends TestCase {

    /**
     * Constructor for TestAdminOptions.
     * @param name
     */
    public TestAdminOptions(String name) {
        super(name);
    }

    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
    }

    /*
     * @see TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test method for 'com.metamatrix.admin.api.objects.AdminOptions.AdminOptions(int)'
     */
    public void testMMAdminOptions_1_Option() {
        int theOptions = AdminOptions.OnConflict.EXCEPTION;
        AdminOptions opts = new AdminOptions(theOptions);
        assertTrue(opts.containsOption(theOptions));
    }

    /**
     * Test method for 'com.metamatrix.admin.api.objects.AdminOptions.AdminOptions(int)'
     */
    public void testMMAdminOptions_2_Options() {
        int option_1 = AdminOptions.OnConflict.EXCEPTION;
        int option_2 = AdminOptions.OnConflict.IGNORE;

        AdminOptions opts = new AdminOptions(option_1);
        
        opts.addOption(option_2);
        
        assertTrue(opts.containsOption(option_1));
        assertTrue(opts.containsOption(option_2));
    }

    /**
     * Test method for 'com.metamatrix.admin.api.objects.AdminOptions.AdminOptions(int)'
     */
    public void testMMAdminOptions_3_Options() {
        int option_1 = AdminOptions.OnConflict.EXCEPTION;
        int option_2 = AdminOptions.OnConflict.IGNORE;
        int option_3 = AdminOptions.OnConflict.OVERWRITE;

        AdminOptions opts = new AdminOptions(option_1);
        
        opts.addOption(option_2);
        opts.addOption(option_3);
        
        assertTrue(opts.containsOption(option_1));
        assertTrue(opts.containsOption(option_2));
        assertTrue(opts.containsOption(option_3));
    }

    /**
     * Test method for 'com.metamatrix.admin.api.objects.AdminOptions.AdminOptions(int)'
     */
    public void testMMAdminOptions_4_Options() {
        int option_1 = AdminOptions.OnConflict.EXCEPTION;
        int option_2 = AdminOptions.OnConflict.IGNORE;
        int option_3 = AdminOptions.OnConflict.OVERWRITE;
        int option_4 = AdminOptions.BINDINGS_IGNORE_DECRYPT_ERROR;

        AdminOptions opts = new AdminOptions(option_1);
        
        opts.addOption(option_2);
        opts.addOption(option_3);
        opts.addOption(option_4);
        
        assertTrue(opts.containsOption(option_1));
        assertTrue(opts.containsOption(option_2));
        assertTrue(opts.containsOption(option_3));
        assertTrue(opts.containsOption(option_4));
   }

    /**
     * Test method for 'com.metamatrix.admin.api.objects.AdminOptions.AdminOptions(int)'
     */
    public void testMMAdminOptions_Bogus_Added() {
        int option_1 = AdminOptions.OnConflict.EXCEPTION;
        int bogus = 0;

        AdminOptions opts = new AdminOptions(option_1);
        
        try {
            opts.addOption(bogus);
            fail("AdminOptions addOptions method took an invalid option: " + bogus); //$NON-NLS-1$
        } catch (Exception err) {
        }
   }

    /**
     * Test method for 'com.metamatrix.admin.api.objects.AdminOptions.AdminOptions(int)'
     */
    public void testMMAdminOptions_1_Option_DoesNotContain_2() {
        int option_1 = AdminOptions.OnConflict.EXCEPTION;
        int option_2 = AdminOptions.OnConflict.IGNORE;
        int option_3 = AdminOptions.OnConflict.OVERWRITE;

        AdminOptions opts = new AdminOptions(option_1);
        
        assertTrue(opts.containsOption(option_1));
        
        assertFalse(opts.containsOption(option_2));
        assertFalse(opts.containsOption(option_3));
    }

    /**
     * Test method for 'com.metamatrix.admin.api.objects.AdminOptions.AdminOptions(int)'
     */
    public void testMMAdminOptions_2_Option_DoesNotContain_1() {
        int option_1 = AdminOptions.OnConflict.EXCEPTION;
        int option_2 = AdminOptions.OnConflict.IGNORE;
        int option_3 = AdminOptions.OnConflict.OVERWRITE;

        AdminOptions opts = new AdminOptions(option_1);
        
        opts.addOption(option_2);
        
        assertTrue(opts.containsOption(option_1));
        assertTrue(opts.containsOption(option_2));
        
        assertFalse(opts.containsOption(option_3));
    }

    /**
     * Test method for 'com.metamatrix.admin.api.objects.AdminOptions.AdminOptions(int)'
     */
    public void testMMAdminOptions_ORed() {
        int option_1 = AdminOptions.OnConflict.EXCEPTION;
        int option_2 = AdminOptions.OnConflict.IGNORE;

        AdminOptions opts = new AdminOptions(option_1 | option_2);
        
        assertTrue(opts.containsOption(option_1));
        assertTrue(opts.containsOption(option_2));
    }

    /**
     * Test method for 'com.metamatrix.admin.api.objects.AdminOptions.AdminOptions(int)'
     */
    public void testMMAdminOptions_Bogus_0() {
        int bogus = 0;

        try {
            new AdminOptions(bogus);
            fail("AdminOptions ctor took an invalid option: " + bogus); //$NON-NLS-1$
        } catch (Exception err) {
        }
    }

    /**
     * Test method for 'com.metamatrix.admin.api.objects.AdminOptions.AdminOptions(int)'
     */
    public void testMMAdminOptions_ORed_Bogus_Added() {
        int option_1 = AdminOptions.OnConflict.EXCEPTION;
        int option_2 = AdminOptions.OnConflict.IGNORE;
        int bogus = -1;

        AdminOptions opts = new AdminOptions(option_1 | option_2);
        try {
            opts.addOption(bogus);
            fail("AdminOptions addOptions method took an invalid option: " + bogus); //$NON-NLS-1$
        } catch (Exception err) {
        }
    }

    /**
     * Test method for 'com.metamatrix.admin.objects.MMAdminOptions.toString()'
     */
    public void testToStringOne() {
        int option_1 = AdminOptions.OnConflict.EXCEPTION;

        AdminOptions opts = new AdminOptions(option_1);
        
        assertEquals("[OnConflict_EXCEPTION]", opts.toString()); //$NON-NLS-1$
    }

    /**
     * Test method for 'com.metamatrix.admin.objects.MMAdminOptions.toString()'
     */
    public void testToStringTwo() {
        int option_1 = AdminOptions.OnConflict.EXCEPTION;
        int option_4 = AdminOptions.BINDINGS_IGNORE_DECRYPT_ERROR;

        AdminOptions opts = new AdminOptions(option_1);
        opts.addOption(option_4);
        
        assertEquals("[OnConflict_EXCEPTION, BINDINGS_IGNORE_DECRYPT_ERROR]", opts.toString()); //$NON-NLS-1$
    }

    /**
     * Test method for 'com.metamatrix.admin.objects.MMAdminOptions.toString()'
     */
    public void testToStringAll() {
        int option_1 = AdminOptions.OnConflict.EXCEPTION;
        int option_2 = AdminOptions.OnConflict.IGNORE;
        int option_3 = AdminOptions.OnConflict.OVERWRITE;
        int option_4 = AdminOptions.BINDINGS_IGNORE_DECRYPT_ERROR;

        AdminOptions opts = new AdminOptions(option_1);
        
        opts.addOption(option_2);
        opts.addOption(option_3);
        opts.addOption(option_4);
        
        assertEquals("[OnConflict_OVERWRITE, OnConflict_IGNORE, OnConflict_EXCEPTION, BINDINGS_IGNORE_DECRYPT_ERROR]", opts.toString()); //$NON-NLS-1$
    }
}
