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

package com.metamatrix.platform.security.membership.spi.ldap;

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.platform.security.membership.service.MembershipServiceImpl;
import com.metamatrix.platform.security.membership.spi.ldap.LDAPMembershipDomain.LdapContext;
import com.metamatrix.platform.service.api.exception.ServiceStateException;

public class TestLDAPMembershipDomainConfiguration extends TestCase {

    private LDAPMembershipDomain getLdapMembershipDomainWithMultipleContexts() throws ServiceStateException {
        LDAPMembershipDomain domain = new LDAPMembershipDomain();
        
        Properties p = new Properties();
        //ldap url, usersRootContext, groupsRootContext
        p.setProperty(LDAPMembershipDomain.LDAP_URL, "ldap://sluxtech09:389"); //$NON-NLS-1$
        p.setProperty(LDAPMembershipDomain.USERS_ROOT_CONTEXT, "ou=people,dc=metamatrix,dc=com?ou=people,dc=quadrian,dc=com"); //$NON-NLS-1$
        p.setProperty(LDAPMembershipDomain.GROUPS_ROOT_CONTEXT, "ou=groups,dc=metamatrix,dc=com?ou=groups,dc=quadrian,dc=com"); //$NON-NLS-1$

        //properties for Apache DS
        p.setProperty(LDAPMembershipDomain.GROUPS_GROUP_MEMBER_ATTRIBUTE, "uniquemember"); //$NON-NLS-1$
        
        //credentials
        p.setProperty(LDAPMembershipDomain.LDAP_ADMIN_DN, "cn=Directory Manager"); //$NON-NLS-1$
        p.setProperty(LDAPMembershipDomain.LDAP_ADMIN_PASSWORD, "stladmin"); //$NON-NLS-1$
        p.setProperty(MembershipServiceImpl.DOMAIN_NAME, "testDomain"); //$NON-NLS-1$
        
        
        domain.initialize(p);
        return domain;
    }
    
    /** 
     * testInvalidInit1 - tests invalid init - no properties supplied.
     */
    public void testInvalidInit1() throws Exception {
        LDAPMembershipDomain domain = new LDAPMembershipDomain();
        
        // Empty properties
        Properties p = new Properties();
        
        // Initialize the domain with empty properties
        try {
            domain.initialize(p);
            fail("expected exception"); //$NON-NLS-1$
        } catch (ServiceStateException e) {
            assertEquals("Required property ldapURL was missing.", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    /** 
     * testInvalidInit2 - tests invalid init - only the ldap URL is supplied.
     */
    public void testInvalidInit2() throws Exception {
        LDAPMembershipDomain domain = new LDAPMembershipDomain();

        // Properties containing ldap URL only
        Properties p = new Properties();
        p.setProperty(LDAPMembershipDomain.LDAP_URL, "ldap://sluxtech09:389"); //$NON-NLS-1$

        try {
            domain.initialize(p);
            fail("expected exception"); //$NON-NLS-1$
        } catch (ServiceStateException e) {
            assertEquals("Required property users.rootContext was missing.", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    /** 
     * testInvalidInit3 - tests invalid init - ldap URL and users rootContext are supplied.
     */
    public void testInvalidInit3() throws Exception {
        LDAPMembershipDomain domain = new LDAPMembershipDomain();
        
        Properties p = new Properties();
        p.setProperty(LDAPMembershipDomain.LDAP_URL, "ldap://sluxtech09:389"); //$NON-NLS-1$
        p.setProperty(LDAPMembershipDomain.USERS_ROOT_CONTEXT, "ou=people,dc=metamatrix,dc=com"); //$NON-NLS-1$

        try {
            domain.initialize(p);
            fail("expected exception"); //$NON-NLS-1$
        } catch (ServiceStateException e) {
            assertEquals("Required property groups.rootContext was missing.", e.getMessage()); //$NON-NLS-1$
        }
    }
    
//    public void testInvalidInit4() throws Exception {
//        LDAPMembershipDomain domain = new LDAPMembershipDomain();
//        
//        Properties p = new Properties();
//        p.setProperty(LDAPMembershipDomain.LDAP_URL, "ldap://sluxtech09:389"); //$NON-NLS-1$
//        p.setProperty(LDAPMembershipDomain.USERS_ROOT_CONTEXT, "ou=people,dc=metamatrix,dc=com"); //$NON-NLS-1$
//        p.setProperty(LDAPMembershipDomain.GROUPS_ROOT_CONTEXT, "ou=groups,dc=metamatrix,dc=com"); //$NON-NLS-1$
//        
//        try {
//            domain.initialize(p);
//            fail("expected exception"); //$NON-NLS-1$
//        } catch (ServiceStateException e) {
//            assertEquals("No users will appear as members of any group since user's memberOf and group's group memberOf attributes are both unspecified.", e.getMessage()); //$NON-NLS-1$
//        }
//    }
    
    /** 
     * testValidInit - tests valid init - all required properties supplied.
     */
    public void testValidInit() throws Exception {
        LDAPMembershipDomain domain = new LDAPMembershipDomain();
        
        Properties p = new Properties();
        p.setProperty(LDAPMembershipDomain.LDAP_URL, "ldap://sluxtech09:389"); //$NON-NLS-1$
        p.setProperty(LDAPMembershipDomain.USERS_ROOT_CONTEXT, "ou=people,dc=metamatrix,dc=com"); //$NON-NLS-1$
        p.setProperty(LDAPMembershipDomain.GROUPS_ROOT_CONTEXT, "ou=groups,dc=metamatrix,dc=com"); //$NON-NLS-1$
        p.setProperty(LDAPMembershipDomain.USERS_MEMBER_OF_ATTRIBUTE, "memberOf"); //$NON-NLS-1$
        try {
            domain.initialize(p);
        } catch (ServiceStateException e) {
            fail("Encountered initialization exception"); //$NON-NLS-1$
        }
    }

    public void testUsernameEscaping() {
        assertEquals("\\2a", LDAPMembershipDomain.escapeLDAPSearchFilter("*")); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testMultipleContexts() throws Exception {
        LDAPMembershipDomain domain = getLdapMembershipDomainWithMultipleContexts();
        assertEquals(2, domain.getUsersRootContexts().size());
        LdapContext context = (LdapContext)domain.getUsersRootContexts().get(1);
        assertEquals("ou=people,dc=quadrian,dc=com", context.context); //$NON-NLS-1$
    }

}
