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

package com.metamatrix.platform.security.authorization.service;

import java.util.Collection;

import junit.framework.TestCase;

import org.jboss.cache.Cache;
import org.jboss.cache.CacheFactory;
import org.jboss.cache.DefaultCacheFactory;
import org.mockito.Mockito;

import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.api.exception.security.InvalidPrincipalException;
import com.metamatrix.cache.FakeCache;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.platform.security.api.BasicMetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.authorization.cache.AuthorizationCache;
import com.metamatrix.platform.security.authorization.spi.AuthorizationSourceTransaction;
import com.metamatrix.platform.security.authorization.spi.FakeAuthorizationSource;
import com.metamatrix.platform.security.authorization.spi.TestFakeAuthorizationSource;
import com.metamatrix.platform.service.api.exception.ServiceException;

public class TestAuthorizationServiceImpl extends TestCase {

	private static final String TEST_GROUP = "g1+p1"; //$NON-NLS-1$
	private static final String INVALID_GROUP = "foo"; //$NON-NLS-1$

	public class FakeAuthorizationService extends AuthorizationServiceImpl {
		
		public FakeAuthorizationService() throws Exception {
			this.authorizationCache = new AuthorizationCache(new FakeCache("1"), new FakeCache("2"), null); //$NON-NLS-1$ //$NON-NLS-2$
			this.membershipServiceProxy = Mockito.mock(MembershipServiceInterface.class);
			Mockito.stub(this.membershipServiceProxy.getPrincipal(new MetaMatrixPrincipalName(TEST_GROUP, MetaMatrixPrincipal.TYPE_GROUP))).toReturn(new BasicMetaMatrixPrincipal(TEST_GROUP, MetaMatrixPrincipal.TYPE_GROUP));
			Mockito.stub(this.membershipServiceProxy.getPrincipal(new MetaMatrixPrincipalName(INVALID_GROUP, MetaMatrixPrincipal.TYPE_GROUP))).toThrow(new InvalidPrincipalException());
		}
		
		@Override
		protected AuthorizationSourceTransaction getReadTransaction()
				throws ManagedConnectionException {
			FakeAuthorizationSource source = new FakeAuthorizationSource();
			TestFakeAuthorizationSource.helpPopulate(source);
			return source;
		}
		
		@Override
		protected boolean isEntitled(String principal)
				throws ServiceException {
			return false;
		}
	}
	
	private static final SessionToken TEST_SESSION_TOKEN = new SessionToken(); 

	public void testGetRolesForGroup() throws Exception {
		AuthorizationServiceImpl service = new FakeAuthorizationService();
		
		String groupName = TEST_GROUP; 
		
		Collection roles = service.getRoleNamesForPrincipal(TEST_SESSION_TOKEN, new MetaMatrixPrincipalName(groupName, MetaMatrixPrincipal.TYPE_GROUP));

		assertEquals(1, roles.size());
		assertEquals("Policy1", roles.iterator().next()); //$NON-NLS-1$		
	}
	
	public void testGetRolesForGroupWithInvalidGroup() throws Exception {
		AuthorizationServiceImpl service = new FakeAuthorizationService();
		
		String groupName = INVALID_GROUP;
		
		try {
			service.getRoleNamesForPrincipal(TEST_SESSION_TOKEN, new MetaMatrixPrincipalName(groupName, MetaMatrixPrincipal.TYPE_GROUP));
			fail("expected exception"); //$NON-NLS-1$
		} catch (AuthorizationMgmtException e) {
			
		}
	}
	
}
