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

package com.metamatrix.platform.security.membership.service;

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.api.exception.security.InvalidPrincipalException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.membership.spi.file.TestFileMembershipDomain;
import com.metamatrix.platform.service.api.exception.ServiceStateException;

public class TestMembershipServiceImpl extends TestCase {
    
    public void testInitialization() throws Exception {
        Properties p = new Properties();
        p.setProperty(MembershipServiceInterface.ADMIN_USERNAME, "metamatrixadmin"); //$NON-NLS-1$
        p.setProperty(MembershipServiceInterface.ADMIN_PASSWORD, CryptoUtil.getCryptor().encrypt("mm")); //$NON-NLS-1$
        p.setProperty(MembershipServiceInterface.SECURITY_ENABLED, Boolean.TRUE.toString());
        MembershipServiceImpl membershipServiceImpl = new MembershipServiceImpl();
        
        membershipServiceImpl.initService(p);
        
        assertEquals(0, membershipServiceImpl.getDomains().size());        
        assertTrue(membershipServiceImpl.isSecurityEnabled());
    }
    
    public void testInitialization1() throws Exception {
        Properties p = new Properties();
        p.setProperty(MembershipServiceInterface.ADMIN_USERNAME, "metamatrixadmin"); //$NON-NLS-1$
        p.setProperty(MembershipServiceInterface.ADMIN_PASSWORD, CryptoUtil.getCryptor().encrypt("mm")); //$NON-NLS-1$
        p.setProperty(MembershipServiceInterface.SECURITY_ENABLED, Boolean.FALSE.toString());
        MembershipServiceImpl membershipServiceImpl = new MembershipServiceImpl();
        
        membershipServiceImpl.initService(p);
        
        assertEquals(0, membershipServiceImpl.getDomains().size());
        assertFalse(membershipServiceImpl.isSecurityEnabled());
        
        assertTrue(membershipServiceImpl.authenticateUser("foo", new Credentials("bar".toCharArray()), null, null) instanceof SuccessfulAuthenticationToken); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testBaseUsername() throws Exception {
        
        assertEquals("foo@bar.com", MembershipServiceImpl.getBaseUsername("foo\\@bar.com@foo")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals("foo", MembershipServiceImpl.getDomainName("me\\@bar.com@foo")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals(null, MembershipServiceImpl.getDomainName("@")); //$NON-NLS-1$
        
        assertEquals("@", MembershipServiceImpl.getBaseUsername("@")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    private MembershipServiceImpl createMembershipService() throws ServiceStateException {
        MembershipServiceImpl membershipService = new MembershipServiceImpl();
        MembershipServiceImpl.MembershipDomainHolder membershipDomainHolder = new MembershipServiceImpl.MembershipDomainHolder(
                                                                               TestFileMembershipDomain
                                                                                                       .createFileMembershipDomain(),
                                                                               TestFileMembershipDomain.TEST_DOMAIN_NAME);
        membershipService.getDomains().add(membershipDomainHolder);
        return membershipService;
    }
    
    public void testGetPrincipal() throws Exception {
        MembershipServiceImpl membershipService = createMembershipService();
        
        MetaMatrixPrincipal principal = membershipService.getPrincipal(new MetaMatrixPrincipalName("user1@testFile", MetaMatrixPrincipal.TYPE_USER)); //$NON-NLS-1$
        
        assertEquals("user1@testFile", principal.getName()); //$NON-NLS-1$
    }
    
    public void testAuthenticate() throws Exception {
        MembershipServiceImpl membershipService = createMembershipService();
        
        AuthenticationToken at = (AuthenticationToken)membershipService.authenticateUser("user1", new Credentials("pass1".toCharArray()), null, null); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals("user1@testFile", at.getUserName()); //$NON-NLS-1$
    }
    
    public void testGetPrincipalForGroup() throws Exception {
        MembershipServiceImpl membershipService = createMembershipService();
        
        MetaMatrixPrincipal principal = membershipService.getPrincipal(new MetaMatrixPrincipalName("group1@testFile", MetaMatrixPrincipal.TYPE_GROUP)); //$NON-NLS-1$
        
        assertEquals("group1@testFile", principal.getName()); //$NON-NLS-1$
        assertEquals(MetaMatrixPrincipal.TYPE_GROUP, principal.getType()); 
    }

    public void testGetPrincipalForInvalidGroup() throws Exception {
        MembershipServiceImpl membershipService = createMembershipService();
        
        try {
        	membershipService.getPrincipal(new MetaMatrixPrincipalName("groupX@testFile", MetaMatrixPrincipal.TYPE_GROUP)); //$NON-NLS-1$
        } catch (InvalidPrincipalException e) {
        	assertEquals("The principal 'groupX@testFile' does not exist in domain 'testFile'", e.getMessage()); //$NON-NLS-1$
        }
    }

}
