/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
