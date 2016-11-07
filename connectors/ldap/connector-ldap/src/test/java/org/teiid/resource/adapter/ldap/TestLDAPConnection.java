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

package org.teiid.resource.adapter.ldap;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("nls")
public class TestLDAPConnection {
	
	public static class FakeFactory implements InitialContextFactory {
		@Override
		public Context getInitialContext(Hashtable<?, ?> environment)
				throws NamingException {
			return Mockito.mock(Context.class);
		}
	}

	@Test public void testInitialization() throws Exception {
		
		LDAPManagedConnectionFactory config = new LDAPManagedConnectionFactory();
		config.setLdapUrl("ldap://foo");
		config.setLdapAdminUserDN("admin");
		config.setLdapAdminUserPassword("password");
		config.setLdapContextFactory(FakeFactory.class.getName());
		
        
		new LDAPConnectionImpl(config);
	}
	
}
