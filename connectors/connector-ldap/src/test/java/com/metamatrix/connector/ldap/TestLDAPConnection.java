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

package com.metamatrix.connector.ldap;

import java.util.Hashtable;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.junit.Test;
import org.mockito.Mockito;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.SysLogger;

public class TestLDAPConnection {
	
	public static class FakeFactory implements InitialContextFactory {
		@Override
		public Context getInitialContext(Hashtable<?, ?> environment)
				throws NamingException {
			return Mockito.mock(Context.class);
		}
	}

	@Test public void testInitialization() throws Exception {
		Properties p = new Properties();
		p.setProperty(LDAPConnectorPropertyNames.LDAP_URL, "ldap://foo"); //$NON-NLS-1$
		p.setProperty(LDAPConnectorPropertyNames.LDAP_ADMIN_USER_DN, "admin"); //$NON-NLS-1$
		p.setProperty(LDAPConnectorPropertyNames.LDAP_ADMIN_USER_PASSWORD, "password"); //$NON-NLS-1$
		new LDAPConnection(EnvironmentUtility.createExecutionContext("1", "1"), p, EnvironmentUtility.createStdoutLogger(SysLogger.ERROR), FakeFactory.class.getName()); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
}
