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

package org.teiid.services;

import java.security.Principal;
import java.util.*;

import javax.security.auth.Subject;

import junit.framework.TestCase;

import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.SimplePrincipal;
import org.mockito.Mockito;
import org.teiid.security.Credentials;
import org.teiid.security.SecurityHelper;

@SuppressWarnings("nls")
public class TestMembershipServiceImpl extends TestCase {
    
   
    public void testBaseUsername() throws Exception {
        
        assertEquals("foo@bar.com", TeiidLoginContext.getBaseUsername("foo\\@bar.com@foo")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals("foo", TeiidLoginContext.getDomainName("me\\@bar.com@foo")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals(null, TeiidLoginContext.getDomainName("@")); //$NON-NLS-1$
        
        assertEquals("@", TeiidLoginContext.getBaseUsername("@")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    private TeiidLoginContext createMembershipService() throws Exception {
    	Principal p = Mockito.mock(Principal.class);
    	Mockito.stub(p.getName()).toReturn("alreadylogged"); //$NON-NLS-1$
    	HashSet<Principal> principals = new HashSet<Principal>();
    	principals.add(p);
    	
    	Subject subject = new Subject(false, principals, new HashSet(), new HashSet());
    	SecurityHelper sh = Mockito.mock(SecurityHelper.class);
    	Mockito.stub(sh.getSubjectInContext("passthrough")).toReturn(subject); //$NON-NLS-1$
    	
        TeiidLoginContext membershipService = new TeiidLoginContext(sh);
        return membershipService;
    }
    
       
    public void testAuthenticate() throws Exception {
    	Credentials credentials = new Credentials("pass1".toCharArray());
        TeiidLoginContext ms = createMembershipService();
        List<String> domains = new ArrayList<String>();
        domains.add("testFile"); //$NON-NLS-1$
        Map<String, SecurityDomainContext> securityDomainMap = new HashMap<String, SecurityDomainContext>();
        SecurityDomainContext securityContext = Mockito.mock(SecurityDomainContext.class);
        AuthenticationManager authManager = new AuthenticationManager() {
			public String getSecurityDomain() {
				return null;
			}
			public boolean isValid(Principal principal, Object credential, Subject activeSubject) {
				return true;
			}
			public boolean isValid(Principal principal, Object credential) {
				return true;
			}
			
			@Override
			public Principal getTargetPrincipal(Principal anotherDomainPrincipal, Map<String, Object> contextMap) {
				return null;
			}
			@Override
			public Subject getActiveSubject() {
				return null;
			}
		};
        
        Mockito.stub(securityContext.getAuthenticationManager()).toReturn(authManager);
        securityDomainMap.put("testFile", securityContext); //$NON-NLS-1$
        
        ms.authenticateUser("user1", credentials, null, domains,securityDomainMap, false); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("user1@testFile", ms.getUserName()); //$NON-NLS-1$
    }
    

    public void testPassThrough() throws Exception {
        TeiidLoginContext ms = createMembershipService();
        List<String> domains = new ArrayList<String>();
        domains.add("passthrough"); //$NON-NLS-1$
        Map<String, SecurityDomainContext> securityDomainMap = new HashMap<String, SecurityDomainContext>();
        ms.authenticateUser("user1", new Credentials("pass1".toCharArray()), null, domains, securityDomainMap, true); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals("alreadylogged@passthrough", ms.getUserName()); //$NON-NLS-1$
    }
}
