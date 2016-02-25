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
package org.teiid.jdbc;

import static org.junit.Assert.*;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;

@SuppressWarnings("nls")
public class TestPassthroughAuthentication {

	static FakeServer server = new FakeServer(false);
	static TestableSecurityHelper securityHelper = new TestableSecurityHelper(); 
	
	@AfterClass public static void oneTimeTearDown() {
		server.stop();
	}
	
	@BeforeClass public static void oneTimeSetup() throws Exception {
    	server.setUseCallingThread(true);
    	server.start(new EmbeddedConfiguration() {
    		@Override
            public SecurityHelper getSecurityHelper() {
    			return securityHelper;
    		}  		
    	}, false);
	}
	
	@Test
	public void test() throws Exception {
		try {
			server.deployVDB("not_there", UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
			server.getSessionService().setTrustAllLocal(false);
			try {
				server.createConnection("jdbc:teiid:not_there.1;passthroughAuthentication=true");
				fail();
			} catch (Exception e) {
			}

			server.getSessionService().setTrustAllLocal(true);
			server.createConnection("jdbc:teiid:not_there.1;passthroughAuthentication=true");
			
			securityHelper.associateSecurityContext("testSC");
			try {
				server.createConnection("jdbc:teiid:not_there.1;passthroughAuthentication=true");
			} catch (Exception e) {
				fail();
			}			
		} finally {
			server.undeployVDB("not_there");
		}
	}

	private static class TestableSecurityHelper implements SecurityHelper {
		Object ctx;
		@Override
		public Object associateSecurityContext(Object context) {
			return ctx = context;
		}
		@Override
		public void clearSecurityContext() {
			ctx = null;
		}
		@Override
		public Object getSecurityContext() {
			return this.ctx;
		}

		@Override
		public Subject getSubjectInContext(String securityDomain) {
			if (securityDomain.equals("teiid-security") && getSecurityContext() != null && getSecurityContext().equals("testSC")) {
				Subject s = new Subject();
				return s;
			}
			return null;
		}
		
		@Override
		public Subject getSubjectInContext(Object context) {
			if (context != null) {
				return new Subject();
			}
			return null;
		}
		
		@Override
		public Object authenticate(String securityDomain, String baseUserName,
				Credentials credentials, String applicationName) throws LoginException {
            return ctx;
        }
        @Override
        public GSSResult negotiateGssLogin(String securityDomain, byte[] serviceTicket) throws LoginException {
            return null;
        }
	};
}
