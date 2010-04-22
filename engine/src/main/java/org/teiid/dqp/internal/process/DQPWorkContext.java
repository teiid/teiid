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
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.security.auth.Subject;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.security.SessionToken;
import org.teiid.security.SecurityHelper;

import com.metamatrix.dqp.message.RequestID;

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
    
    public DQPWorkContext() {
	}
    
    public SessionMetadata getSession() {
		return session;
	}
    
    public void setSession(SessionMetadata session) {
		this.session = session;
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
	
	public <V> V runInContext(Callable<V> callable) throws Exception {
		DQPWorkContext.setWorkContext(this);
		boolean associated = false;
		if (securityHelper != null && this.getSubject() != null) {
			associated = securityHelper.assosiateSecurityContext(this.getSecurityDomain(), this.getSecurityContext());			
		}
		try {
			return callable.call();
		} finally {
			if (associated) {
				securityHelper.clearSecurityContext(this.getSecurityDomain());			
			}
			DQPWorkContext.releaseWorkContext();
		}
	}
	
	public void runInContext(final Runnable runnable) {
		try {
			runInContext(new Callable<Void>() {
				@Override
				public Void call() {
					runnable.run();
					return null;
				}
			});
		} catch (Exception e) {
		}
	}

	public HashMap<String, DataPolicy> getAllowedDataPolicies() {
		if (this.policies == null) {
	    	this.policies = new HashMap<String, DataPolicy>();
	    	Set<String> userRoles = getUserRoles();
	    	if (userRoles.isEmpty()) {
	    		return this.policies;
	    	}
	    	
	    	// get data roles from the VDB
	        List<DataPolicy> policies = getVDB().getDataPolicies();
	        
	    	for (DataPolicy policy : policies) {
	        	if (matchesPrincipal(userRoles, policy)) {
	        		this.policies.put(policy.getName(), policy);
	        	}
	        }
		}
        return this.policies;
    }
    
	private boolean matchesPrincipal(Set<String> userRoles, DataPolicy policy) {
		List<String> roles = policy.getMappedRoleNames();
		for (String role:roles) {
			return userRoles.contains(role);
		}
		return false;
	}    

	private Set<String> getUserRoles() {
		Set<String> roles = new HashSet<String>();
		
		if (getSubject() == null) {
			return Collections.EMPTY_SET;
		}
		
		Set<Principal> principals = getSubject().getPrincipals();
		for(Principal p: principals) {
			// this JBoss specific, but no code level dependencies
			if ((p instanceof Group) && p.getName().equals("Roles")){ //$NON-NLS-1$
				Group g = (Group)p;
				Enumeration rolesPrinciples = g.members();
				while(rolesPrinciples.hasMoreElements()) {
					roles.add(((Principal)rolesPrinciples.nextElement()).getName());	
				}
			}
		}
		return roles;
	}	
}
