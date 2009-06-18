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

package com.metamatrix.platform.security.membership.spi.file;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import junit.framework.TestCase;

import com.metamatrix.api.exception.security.InvalidUserException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.api.exception.security.UnsupportedCredentialException;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SuccessfulAuthenticationToken;
import com.metamatrix.platform.security.membership.spi.MembershipSourceException;

public class TestFileMembershipDomain extends TestCase {
    
    public static final String TEST_DOMAIN_NAME = "testFile"; //$NON-NLS-1$
    
    /** 
     * testInvalidInit1 - tests invalid init - no properties supplied.
     */
    public void testInvalidInit1() throws Exception {
        FileMembershipDomain domain = new FileMembershipDomain();
        
        // Empty properties
        Properties p = new Properties();
        
        // Initialize the domain with empty properties
        try {
            domain.initialize(p);
            fail("expected exception"); //$NON-NLS-1$
        } catch (MembershipSourceException e) {
            assertEquals("Required property usersFile was missing.", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    /** 
     * testInvalidInit2 - tests invalid init - only users file supplied
     */
    public void testInvalidInit2() throws Exception {
        FileMembershipDomain domain = new FileMembershipDomain();
        
        // Empty properties
        Properties p = new Properties();
        File usersFile = UnitTestUtil.getTestDataFile("users.properties"); //$NON-NLS-1$
        
        p.setProperty(FileMembershipDomain.USERS_FILE, usersFile.getAbsolutePath()); 
        
        // Initialize the domain with empty properties
        try {
            domain.initialize(p);
            fail("expected exception"); //$NON-NLS-1$
        } catch (MembershipSourceException e) {
            assertEquals("Required property groupsFile was missing.", e.getMessage()); //$NON-NLS-1$
        }
    }

    /** 
     * testInvalidInit3 - tests invalid init - only groups file supplied
     */
    public void testInvalidInit3() throws Exception {
        FileMembershipDomain domain = new FileMembershipDomain();
        
        // Empty properties
        Properties p = new Properties();
        File groupsFile = UnitTestUtil.getTestDataFile("groups.properties"); //$NON-NLS-1$
        
        p.setProperty(FileMembershipDomain.GROUPS_FILE, groupsFile.getAbsolutePath()); 
        
        // Initialize the domain with empty properties
        try {
            domain.initialize(p);
            fail("expected exception"); //$NON-NLS-1$
        } catch (MembershipSourceException e) {
            assertEquals("Required property usersFile was missing.", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    /** 
     * testBadUsersFile - tests invalid init - bad usersfile supplied
     */
    public void testBadUsersFile() throws Exception {
        FileMembershipDomain domain = new FileMembershipDomain();
        
        // Empty properties
        Properties p = new Properties();
        File usersFile = UnitTestUtil.getTestDataFile("ohCrap"); //$NON-NLS-1$
        File groupsFile = UnitTestUtil.getTestDataFile("groups.properties"); //$NON-NLS-1$
        
        p.setProperty(FileMembershipDomain.USERS_FILE, usersFile.getAbsolutePath()); 
        p.setProperty(FileMembershipDomain.GROUPS_FILE, groupsFile.getAbsolutePath()); 
        
        // Initialize the domain with empty properties
        try {
            domain.initialize(p);
            fail("expected exception"); //$NON-NLS-1$
        } catch (MembershipSourceException e) {
            assertTrue(e.getMessage().startsWith("Could not load file")); //$NON-NLS-1$
        }
    }
    
    /** 
     * testBadGroupsFile - tests invalid init - bad groupsfile supplied
     */
    public void testBadGroupsFile() throws Exception {
        FileMembershipDomain domain = new FileMembershipDomain();
        
        // Empty properties
        Properties p = new Properties();
        File usersFile = UnitTestUtil.getTestDataFile("users.properties"); //$NON-NLS-1$
        File groupsFile = UnitTestUtil.getTestDataFile("bad"); //$NON-NLS-1$
        
        p.setProperty(FileMembershipDomain.USERS_FILE, usersFile.getAbsolutePath()); 
        p.setProperty(FileMembershipDomain.GROUPS_FILE, groupsFile.getAbsolutePath()); 
        
        // Initialize the domain with empty properties
        try {
            domain.initialize(p);
            fail("expected exception"); //$NON-NLS-1$
        } catch (MembershipSourceException e) {
            assertTrue(e.getMessage().startsWith("Could not load file")); //$NON-NLS-1$
        }
    }

    /** 
     * testValidInit - tests valid init - good files
     */
    public void testValidInit() throws Exception {
        FileMembershipDomain domain = new FileMembershipDomain();
        
        // Empty properties
        Properties p = new Properties();
        File usersFile = UnitTestUtil.getTestDataFile("users.properties"); //$NON-NLS-1$
        File groupsFile = UnitTestUtil.getTestDataFile("groups.properties"); //$NON-NLS-1$
        
        p.setProperty(FileMembershipDomain.USERS_FILE, usersFile.getAbsolutePath()); 
        p.setProperty(FileMembershipDomain.GROUPS_FILE, groupsFile.getAbsolutePath()); 
        
        // Initialize the domain with empty properties
        try {
            domain.initialize(p);
        } catch (MembershipSourceException e) {
            fail("unexpected exception"); //$NON-NLS-1$
        }
    }
    
    /** 
     * testValidUserAuthentication - tests valid user
     */
    public void testValidUserAuthentication() throws Exception {
        FileMembershipDomain domain = createFileMembershipDomain();

        SuccessfulAuthenticationToken sat = domain.authenticateUser("user1", new Credentials("pass1".toCharArray()), null, null); //$NON-NLS-1$ //$NON-NLS-2$

        assertNull(sat.getPayload());
    }
    
    public void testInValidUserAuthentication() throws Exception {
        FileMembershipDomain domain = createFileMembershipDomain();

        try {
            domain.authenticateUser("user1", null, null, null); //$NON-NLS-1$
            fail("Expected exception"); //$NON-NLS-1$
        } catch (UnsupportedCredentialException uce) {
            //expected
        }
    }


    public static FileMembershipDomain createFileMembershipDomain() throws MembershipSourceException {
        return createFileMembershipDomain(true);
    }

    public static FileMembershipDomain createFileMembershipDomain(boolean checkPassword) throws MembershipSourceException {
        FileMembershipDomain domain = new FileMembershipDomain();
        
        // Empty properties
        Properties p = new Properties();
        File usersFile = UnitTestUtil.getTestDataFile("users.properties"); //$NON-NLS-1$
        File groupsFile = UnitTestUtil.getTestDataFile("groups.properties"); //$NON-NLS-1$
        
        p.setProperty(FileMembershipDomain.USERS_FILE, usersFile.getAbsolutePath()); 
        p.setProperty(FileMembershipDomain.GROUPS_FILE, groupsFile.getAbsolutePath()); 
        p.setProperty(FileMembershipDomain.CHECK_PASSWORD, Boolean.toString(checkPassword)); 
        p.setProperty(MembershipServiceInterface.DOMAIN_NAME, TEST_DOMAIN_NAME); 
        
        domain.initialize(p);
        return domain;
    }
    
    /** 
     * testInvalidUserAuthentication - tests invalid user
     */
    public void testInvalidUserAuthentication() throws Exception {
        FileMembershipDomain domain = createFileMembershipDomain();

        try {
        	domain.authenticateUser("joe", new Credentials("pass1".toCharArray()), null, null); //$NON-NLS-1$ //$NON-NLS-2$
        	fail("expected exception"); //$NON-NLS-1$
        } catch (InvalidUserException e) {
        	assertEquals(e.getMessage(),"user joe is invalid"); //$NON-NLS-1$
        }
    }
    
    /** 
     * testInvalidPasswordAuthentication - tests invalid password
     */
    public void testInvalidPasswordAuthentication() throws Exception {
        FileMembershipDomain domain = createFileMembershipDomain();

        try {
        	domain.authenticateUser("user1", new Credentials("pass2".toCharArray()), null, null); //$NON-NLS-1$ //$NON-NLS-2$
        	fail("expected exception"); //$NON-NLS-1$
        } catch (LogonException e) {
        	assertEquals(e.getMessage(),"user user1 could not be authenticated"); //$NON-NLS-1$
        }
    }

    /** 
     * testInvalidUserWithCheckingFalse - tests invalid user with checking turned off
     */
    public void testInvalidUserWithCheckingFalse() throws Exception {
        FileMembershipDomain domain = createFileMembershipDomain();

        try {
        	domain.authenticateUser("joe", new Credentials("pass1".toCharArray()), null, null); //$NON-NLS-1$ //$NON-NLS-2$
        	fail("expected exception"); //$NON-NLS-1$
        } catch (InvalidUserException e) {
        	assertEquals(e.getMessage(),"user joe is invalid"); //$NON-NLS-1$
        }
    }
    
    /** 
     * testInvalidPasswordWithCheckingFalse - tests valid user but invalid password with checking turned off
     */
    public void testInvalidPasswordWithCheckingFalse() throws Exception {
        FileMembershipDomain domain = createFileMembershipDomain(false);

        SuccessfulAuthenticationToken sat = domain.authenticateUser("user1", new Credentials("pass2".toCharArray()), null, null); //$NON-NLS-1$ //$NON-NLS-2$

        assertNull(sat.getPayload());
    }


    /** 
     * testGetGroupNames - tests get groupNames.
     */
    public void testGetGroupNames() throws Exception {
        FileMembershipDomain domain = createFileMembershipDomain();
        
        Set groupNames = domain.getGroupNames();
        
        assertEquals(new HashSet(Arrays.asList(new Object[] {"group1", "group2", "group3"})), groupNames); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        
    }
    
    /** 
     * testGetGroupNamesForUser - tests get groupNames for a user.
     */
    public void testGetGroupNamesForUser() throws Exception {
        FileMembershipDomain domain = createFileMembershipDomain();
                
        Set groupNames = domain.getGroupNamesForUser("user2"); //$NON-NLS-1$
        
        assertEquals(new HashSet(Arrays.asList(new Object[] {"group1", "group2"})), groupNames); //$NON-NLS-1$ //$NON-NLS-2$
        
    }
    
    /** 
     * testGetGroupNamesForUser - tests get groupNames for an invalid user.
     */
    public void testGetGroupNamesForInvalidUser() throws Exception {
        FileMembershipDomain domain = createFileMembershipDomain();
        
        try {
            domain.getGroupNamesForUser("markyMark"); //$NON-NLS-1$
            fail("expected exception"); //$NON-NLS-1$
        } catch (Exception e) {
            assertTrue(e instanceof InvalidUserException); 
        }
        
    }
    
    public void testInitializeWithClasspathFiles() throws Exception {
        FileMembershipDomain domain = new FileMembershipDomain();
        
        // Empty properties
        Properties p = new Properties();
        String usersFile = "classpath:users.properties"; //$NON-NLS-1$
        String groupsFile = "classpath:groups.properties"; //$NON-NLS-1$
        
        p.setProperty(FileMembershipDomain.USERS_FILE, usersFile); 
        p.setProperty(FileMembershipDomain.GROUPS_FILE, groupsFile); 
        p.setProperty(MembershipServiceInterface.DOMAIN_NAME, TEST_DOMAIN_NAME); 
        
        domain.initialize(p);
        
        assertEquals(3, domain.getUsers().size());
    }

}
