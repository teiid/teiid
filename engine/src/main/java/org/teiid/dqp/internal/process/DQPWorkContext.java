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

import java.io.Serializable;
import java.security.Principal;
import java.security.acl.Group;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import javax.security.auth.Subject;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.security.SessionToken;
import org.teiid.dqp.message.RequestID;
import org.teiid.security.SecurityHelper;


public class DQPWorkContext implements Serializable {
	
	private static final long serialVersionUID = -6389893410233192977L;
	
	private static ThreadLocal<DQPWorkContext> CONTEXTS = new ThreadLocal<DQPWorkContext>() {
		protected DQPWorkContext initialValue() {
			return new DQPWorkContext();
		}
	};

	public static DQPWorkContext getWorkContext() {
		return CONTEXTS.get();
	}
	
	public static void setWorkContext(DQPWorkContext context) {
		CONTEXTS.set(context);
	}

	public static void releaseWorkContext() {
		CONTEXTS.set(null);
	}	
	
	private SessionMetadata session = new SessionMetadata();
    private String clientAddress;
    private String clientHostname;
    private SecurityHelper securityHelper;
    private HashMap<String, DataPolicy> policies;
    private boolean useCallingThread;
    
    public DQPWorkContext() {
	}

    public boolean useCallingThread() {
		return useCallingThread;
	}
    
    public void setUseCallingThread(boolean useCallingThread) {
		this.useCallingThread = useCallingThread;
	}
    
    public SessionMetadata getSession() {
		return session;
	}
    
    public void setSession(SessionMetadata session) {
		this.session = session;
		this.policies = null;
	}
    
    public void setSecurityHelper(SecurityHelper securityHelper) {
		this.securityHelper = securityHelper;
	}

    /**
     * @return
     */
    public String getUserName() {
		return session.getUserName();
    }
    
    public Subject getSubject() {
    	if (session.getLoginContext() != null) {
    		return session.getLoginContext().getSubject();
    	}
    	return null;
    }
    
    /**
     * @return
     */
    public String getVdbName() {
        return session.getVDBName();
    }

    /**
     * @return
     */
    public int getVdbVersion() {
        return session.getVDBVersion();
    }

	public String getSessionId() {
		return this.session.getSessionId();
	}

	public String getAppName() {
		return session.getApplicationName();
	}
	
	public RequestID getRequestID(long exeuctionId) {
		return new RequestID(this.getSessionId(), exeuctionId);
	}
	
	public SessionToken getSessionToken() {
		return session.getSessionToken();
	}

	public void setClientAddress(String clientAddress) {
		this.clientAddress = clientAddress;
	}

	/**
	 * Get the client address from the socket transport - not as reported from the client
	 * @return
	 */
	public String getClientAddress() {
		return clientAddress;
	}

	public void setClientHostname(String clientHostname) {
		this.clientHostname = clientHostname;
	}

	/**
	 * Get the client hostname from the socket transport - not as reported from the client
	 * @return
	 */
	public String getClientHostname() {
		return clientHostname;
	}
	
	public String getSecurityDomain() {
		return this.session.getSecurityDomain();
	}

	public Object getSecurityContext() {
		return session.getSecurityContext();
	}
	
	public VDBMetaData getVDB() {
		return session.getVdb();
	}
	
	public <V> V runInContext(Callable<V> callable) throws Throwable {
		FutureTask<V> task = new FutureTask<V>(callable);
		runInContext(task);
		try {
			return task.get();
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}
	
	public void runInContext(final Runnable runnable) {
		DQPWorkContext.setWorkContext(this);
		boolean associated = false;
		if (securityHelper != null && this.getSubject() != null) {
			associated = securityHelper.assosiateSecurityContext(this.getSecurityDomain(), this.getSecurityContext());			
		}
		try {
			runnable.run();
		} finally {
			if (associated) {
				securityHelper.clearSecurityContext(this.getSecurityDomain());			
			}
			DQPWorkContext.releaseWorkContext();
		}
	}

	public HashMap<String, DataPolicy> getAllowedDataPolicies() {
		if (this.policies == null) {
	    	this.policies = new HashMap<String, DataPolicy>();
	    	Set<String> userRoles = getUserRoles();
	    	
	    	// get data roles from the VDB
	    	for (DataPolicy policy : getVDB().getDataPolicies()) {
	        	if (matchesPrincipal(userRoles, policy)) {
	        		this.policies.put(policy.getName(), policy);
	        	}
	        }
		}
        return this.policies;
    }
    
	private boolean matchesPrincipal(Set<String> userRoles, DataPolicy policy) {
		if (policy.isAnyAuthenticated()) {
			return true;
		}
		return !Collections.disjoint(policy.getMappedRoleNames(), userRoles);
	}    

	private Set<String> getUserRoles() {
		Set<String> roles = new HashSet<String>();
		
		if (getSubject() == null) {
			return Collections.emptySet();
		}
		
		Set<Principal> principals = getSubject().getPrincipals();
		for(Principal p: principals) {
			// this JBoss specific, but no code level dependencies
			if ((p instanceof Group) && p.getName().equals("Roles")){ //$NON-NLS-1$
				Group g = (Group)p;
				Enumeration<? extends Principal> rolesPrinciples = g.members();
				while(rolesPrinciples.hasMoreElements()) {
					roles.add(rolesPrinciples.nextElement().getName());	
				}
			}
		}
		return roles;
	}	
}
