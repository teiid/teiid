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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;


public class TestDQPWorkContext {

	public static DQPWorkContext example() {
		DQPWorkContext message = new DQPWorkContext();
		message.getSession().setVDBName("vdbName"); //$NON-NLS-1$
		message.getSession().setVDBVersion(1); 
		message.getSession().setApplicationName("querybuilder"); //$NON-NLS-1$
		message.getSession().setSessionId(String.valueOf(5));
		message.getSession().setUserName("userName"); //$NON-NLS-1$
		return message;
	}

	@Test public void testSerialize() throws Exception {
		DQPWorkContext copy = UnitTestUtil.helpSerialize(example());

		assertEquals("5", copy.getSessionId()); //$NON-NLS-1$
		assertEquals("userName", copy.getUserName()); //$NON-NLS-1$
		assertEquals("vdbName", copy.getVdbName()); //$NON-NLS-1$
		assertEquals(1, copy.getVdbVersion());
		assertEquals("querybuilder", copy.getAppName()); //$NON-NLS-1$
	}
	
	@Test public void testClearPolicies() {
		DQPWorkContext message = new DQPWorkContext();
		message.setSession(Mockito.mock(SessionMetadata.class));
		Mockito.stub(message.getSession().getVdb()).toReturn(new VDBMetaData());
		Map<String, DataPolicy> map = message.getAllowedDataPolicies();
		map.put("role", Mockito.mock(DataPolicy.class)); //$NON-NLS-1$
		assertFalse(map.isEmpty());
		
		message.setSession(Mockito.mock(SessionMetadata.class));
		Mockito.stub(message.getSession().getVdb()).toReturn(new VDBMetaData());
		map = message.getAllowedDataPolicies();
		assertTrue(map.isEmpty());
	}
	
	@Test public void testAnyAuthenticated() {
		DQPWorkContext message = new DQPWorkContext();
		SessionMetadata mock = Mockito.mock(SessionMetadata.class);
		message.setSession(mock);
		VDBMetaData vdb = new VDBMetaData();
		DataPolicyMetadata dpm = new DataPolicyMetadata();
		dpm.setAnyAuthenticated(true);
		vdb.addDataPolicy(dpm);
		Mockito.stub(mock.getVdb()).toReturn(vdb);
		
		//unauthenticated
		Map<String, DataPolicy> map = message.getAllowedDataPolicies();
		assertEquals(0, map.size());
		
		//authenticated
		message = new DQPWorkContext();
		Mockito.stub(mock.getSubject()).toReturn(new Subject());
		message.setSession(mock);
		map = message.getAllowedDataPolicies();
		assertEquals(1, map.size());
	}
	
	@Test public void testRestoreSecurityContext() {
		final SecurityHelper sc = new SecurityHelper() {
			Object mycontext = null;
			
			@Override
			public Object getSecurityContext() {
				return this.mycontext;
			}
			@Override
			public void clearSecurityContext() {
				this.mycontext = null;
			}
			@Override
			public Object associateSecurityContext(Object context) {
				Object old = mycontext;
				this.mycontext = context;
				return old;
			}
			@Override
			public Subject getSubjectInContext(Object context) {
				return null;
			}
            @Override
            public Subject getSubjectInContext(String securityDomain) {
                return null;
            }
            @Override
            public Object authenticate(String securityDomain, String baseUserName,
            		Credentials credentials, String applicationName) throws LoginException {
                return null;
            }
            @Override
            public GSSResult negotiateGssLogin(String securityDomain, byte[] serviceTicket) throws LoginException {
                return null;
            }
		};	
		Object previousSC = "testSC";
		sc.associateSecurityContext(previousSC);
		
		DQPWorkContext message = new DQPWorkContext() {
		    @Override
            public Subject getSubject() {
		    	return new Subject();
		    }			
		};
		message.setSecurityHelper(sc);
		message.setSession(Mockito.mock(SessionMetadata.class));
		final String currentSC = "teiid-security-context"; //$NON-NLS-1$
		Mockito.stub(message.getSession().getSecurityContext()).toReturn(currentSC);
		
		Runnable r = new Runnable() {
			@Override
			public void run() {
				assertEquals(currentSC, sc.getSecurityContext());
			}
		};
		
		message.runInContext(r);
		
		assertEquals(previousSC, sc.getSecurityContext());
	}	
	
	
}
