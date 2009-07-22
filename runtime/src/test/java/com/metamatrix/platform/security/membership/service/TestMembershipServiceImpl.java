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

package com.metamatrix.platform.security.membership.service;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Properties;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.security.InvalidPrincipalException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.service.AuthenticationToken;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SuccessfulAuthenticationToken;
import com.metamatrix.platform.security.membership.spi.MembershipSourceException;
import com.metamatrix.platform.security.membership.spi.file.TestFileMembershipDomain;

public class TestMembershipServiceImpl extends TestCase {
    
    public void testInitialization() throws Exception {
        Properties p = new Properties();
        p.setProperty(MembershipServiceInterface.ADMIN_USERNAME, "metamatrixadmin"); //$NON-NLS-1$
        p.setProperty(MembershipServiceInterface.ADMIN_PASSWORD, CryptoUtil.getCryptor().encrypt("mm")); //$NON-NLS-1$
        p.setProperty(MembershipServiceInterface.SECURITY_ENABLED, Boolean.TRUE.toString());
        MembershipServiceImpl membershipServiceImpl = new MembershipServiceImpl(null, null);
        
        membershipServiceImpl.initialize(p);
        
        assertEquals(0, membershipServiceImpl.getDomains().size());        
        assertTrue(membershipServiceImpl.isSecurityEnabled());
    }
    
    public void testInitialization1() throws Exception {
        Properties p = new Properties();
        p.setProperty(MembershipServiceInterface.ADMIN_USERNAME, "metamatrixadmin"); //$NON-NLS-1$
        p.setProperty(MembershipServiceInterface.ADMIN_PASSWORD, CryptoUtil.getCryptor().encrypt("mm")); //$NON-NLS-1$
        p.setProperty(MembershipServiceInterface.SECURITY_ENABLED, Boolean.FALSE.toString());
        MembershipServiceImpl membershipServiceImpl = new MembershipServiceImpl(null,null);
        
        membershipServiceImpl.initialize(p);
        
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

    private MembershipServiceImpl createMembershipService() throws Exception {
        MembershipServiceImpl membershipService = new MembershipServiceImpl(null, InetAddress.getLocalHost());
        MembershipServiceImpl.MembershipDomainHolder membershipDomainHolder = new MembershipServiceImpl.MembershipDomainHolder(
                                                                               TestFileMembershipDomain
                                                                                                       .createFileMembershipDomain(),
                                                                               TestFileMembershipDomain.TEST_DOMAIN_NAME);
        membershipService.getDomains().add(membershipDomainHolder);
        return membershipService;
    }
    
    public void testSuperAuthenticate() throws Exception {
        MembershipServiceImpl membershipService = createMembershipService();
        membershipService.setAllowedAddresses(Pattern.compile("192[.]168[.]0[.]2")); //$NON-NLS-1$
        membershipService.setAdminCredentials("pass1"); //$NON-NLS-1$
        
        AuthenticationToken at = membershipService.authenticateUser(MembershipServiceImpl.DEFAULT_ADMIN_USERNAME, new Credentials("pass1".toCharArray()), null, null); //$NON-NLS-1$ 
        
        assertFalse(at.isAuthenticated()); 
        DQPWorkContext.getWorkContext().setClientAddress("192.168.0.1"); //$NON-NLS-1$
        at = membershipService.authenticateUser(MembershipServiceImpl.DEFAULT_ADMIN_USERNAME, new Credentials("pass1".toCharArray()), null, null); //$NON-NLS-1$ 
        
        assertFalse(at.isAuthenticated()); 
        DQPWorkContext.getWorkContext().setClientAddress("192.168.0.2"); //$NON-NLS-1$
        at = membershipService.authenticateUser(MembershipServiceImpl.DEFAULT_ADMIN_USERNAME, new Credentials("pass1".toCharArray()), null, null); //$NON-NLS-1$ 
        
        assertTrue(at.isAuthenticated()); 
    }
    
    public void testGetPrincipal() throws Exception {
        MembershipServiceImpl membershipService = createMembershipService();
        
        MetaMatrixPrincipal principal = membershipService.getPrincipal(new MetaMatrixPrincipalName("user1@testFile", MetaMatrixPrincipal.TYPE_USER)); //$NON-NLS-1$
        
        assertEquals("user1@testFile", principal.getName()); //$NON-NLS-1$
    }
    
    public void testAuthenticate() throws Exception {
        MembershipServiceImpl membershipService = createMembershipService();
        
        AuthenticationToken at = membershipService.authenticateUser("user1", new Credentials("pass1".toCharArray()), null, null); //$NON-NLS-1$ //$NON-NLS-2$
        
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
