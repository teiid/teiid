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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.security.Credentials;
import org.teiid.security.SecurityHelper;


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
    	
    	final Subject subject = new Subject(false, principals, new HashSet(), new HashSet());
    	SecurityHelper sh = Mockito.mock(SecurityHelper.class);
    	Mockito.stub(sh.getSubjectInContext("passthrough")).toReturn(subject); //$NON-NLS-1$
    	
        TeiidLoginContext membershipService = new TeiidLoginContext(sh) {
			public LoginContext createLoginContext(String domain, CallbackHandler handler) throws LoginException {
        		LoginContext context =  Mockito.mock(LoginContext.class);
        		Mockito.stub(context.getSubject()).toReturn(subject);
        		return context;
        	}
			protected LoginContext createLoginContext(String domain, Subject subject) throws LoginException {
        		LoginContext context =  Mockito.mock(LoginContext.class);
        		Mockito.stub(context.getSubject()).toReturn(subject);
        		return context;
		    }			
        };
        return membershipService;
    }
    
       
    public void testAuthenticate() throws Exception {
        TeiidLoginContext ms = createMembershipService();
        List<String> domains = new ArrayList<String>();
        domains.add("testFile"); //$NON-NLS-1$
        ms.authenticateUser("user1", new Credentials("pass1".toCharArray()), null, domains, false); //$NON-NLS-1$ //$NON-NLS-2$
        
        Mockito.verify(ms.getLoginContext()).login();
        
        assertEquals("user1@testFile", ms.getUserName()); //$NON-NLS-1$
    }
    

    public void testPassThrough() throws Exception {
        TeiidLoginContext ms = createMembershipService();
        List<String> domains = new ArrayList<String>();
        domains.add("passthrough"); //$NON-NLS-1$
        ms.authenticateUser("user1", new Credentials("pass1".toCharArray()), null, domains, true); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals("alreadylogged@passthrough", ms.getUserName()); //$NON-NLS-1$
    }
}
