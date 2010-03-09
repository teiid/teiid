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

import java.io.Serializable;
import java.security.Principal;
import java.security.acl.Group;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jboss.managed.api.annotation.ManagementComponent;
import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementProperties;
import org.jboss.managed.api.annotation.ManagementProperty;
import org.teiid.adminapi.AdminRoles;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.logging.api.AuditMessage;
import org.teiid.security.roles.AuthorizationActions;
import org.teiid.security.roles.AuthorizationPermission;
import org.teiid.security.roles.AuthorizationPoliciesHolder;
import org.teiid.security.roles.AuthorizationPolicy;
import org.teiid.security.roles.AuthorizationPolicyFactory;
import org.teiid.security.roles.AuthorizationRealm;
import org.teiid.security.roles.BasicAuthorizationPermission;
import org.teiid.security.roles.BasicAuthorizationPermissionFactory;
import org.teiid.security.roles.RolePermissionFactory;
import org.teiid.security.roles.StandardAuthorizationActions;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.LRUCache;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.AuthorizationService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.vdb.runtime.VDBKey;

/**
 * The Authorization Service is responsible for handling requests to determine
 * whether a Principal is entitled to perform a given action on a given resource
 * or set of resources.
 * <p>
 * Administration of the Authorization policies; creating/destroying Policies,
 * adding/deleting Principals and Permissions is available to Principals that
 * have the proper administrative role.
 * </p>
 */
@ManagementObject(componentType=@ManagementComponent(type="teiid",subtype="dqp"), properties=ManagementProperties.EXPLICIT)
public class AuthorizationServiceImpl implements AuthorizationService, Serializable {

	private static final long serialVersionUID = 5399603007837606243L;

	/*
	 * Configuration state
	 */
    private boolean useEntitlements; 

	protected LRUCache<VDBKey, Collection<AuthorizationPolicy>> policyCache = new LRUCache<VDBKey, Collection<AuthorizationPolicy>>();

    // Permission factory is reusable and thread safe
    private static final BasicAuthorizationPermissionFactory PERMISSION_FACTORY = new BasicAuthorizationPermissionFactory();
    
    private Collection<AuthorizationPolicy> adminPolicies = AuthorizationPolicyFactory.buildDefaultAdminPolicies();

	private VDBRepository vdbRepository;
    
	@Override
	public Collection getInaccessibleResources(int action, Collection resources, com.metamatrix.dqp.service.AuthorizationService.Context context) throws MetaMatrixComponentException {
        AuthorizationRealm realm = getRealm(DQPWorkContext.getWorkContext());
        AuthorizationActions actions = getActions(action);
        Collection permissions = createPermissions(realm, resources, actions);
        String auditContext = context.toString();
        Collection inaccessableResources = Collections.EMPTY_LIST;
        try {
            inaccessableResources = getInaccessibleResources(auditContext, permissions);
        } catch (AuthorizationMgmtException e) {
            throw new MetaMatrixComponentException(e);
        }

        // Convert inaccessable resources from auth permissions to string resource names
        Collection inaccessableResourceNames = Collections.EMPTY_LIST;
        if ( inaccessableResources != null && inaccessableResources.size() > 0 ) {
            inaccessableResourceNames = new ArrayList();
            for ( Iterator permItr = inaccessableResources.iterator(); permItr.hasNext(); ) {
                AuthorizationPermission permission = (AuthorizationPermission) permItr.next();
                inaccessableResourceNames.add(permission.getResourceName());
            }
        }
        return inaccessableResourceNames;
    }
    
    /**
     * Of those resources specified, return the subset for which the specified account
     * does <emph>NOT</emph> have authorization to access.
     * @param caller the session token of the principal that is calling this method
     * @param contextName the name of the context for the caller (@see AuditContext)
     * @param requests the permissions that detail the resources and the desired form of access
     * @return the subset of <code>requests</code> that the account does <i>not</i> have access to
     * @throws InvalidSessionException if the session token for this cache is not valid
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation
     */
    private Collection getInaccessibleResources(String contextName, Collection requests) throws AuthorizationMgmtException {
    	
    	SessionToken caller = getSession();
    	
        LogManager.logDetail(com.metamatrix.common.util.LogConstants.CTX_AUTHORIZATION, new Object[]{"getInaccessibleResources(", caller, ", ", contextName, ", ", requests, ")"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        
        List<String> resources = new ArrayList<String>();
        if (requests != null && ! requests.isEmpty()) {            
            Iterator permItr = requests.iterator();
            while ( permItr.hasNext() ) {
                resources.add(((AuthorizationPermission)permItr.next()).getResourceName());
            }            
        }
        
        // Audit - request
    	AuditMessage msg = new AuditMessage( contextName, "getInaccessibleResources-request", caller.getUsername(), resources.toArray(new String[resources.size()])); //$NON-NLS-1$
    	LogManager.log(MessageLevel.INFO, LogConstants.CTX_AUDITLOGGING, msg);
        
        if (isEntitled()){
            return Collections.EMPTY_LIST;
        }

        Collection results = new HashSet(requests);
        Collection policies = this.getPoliciesForPrincipal(getRequestedRealm(requests));

        Iterator policyIter = policies.iterator();
        
        while (policyIter.hasNext() && !results.isEmpty()) {
            Iterator requestIter = results.iterator();
            AuthorizationPolicy policy = (AuthorizationPolicy) policyIter.next();
            while (requestIter.hasNext()) {
                AuthorizationPermission request = (AuthorizationPermission) requestIter.next();
                if (policy.implies(request)) {
                    requestIter.remove();
                    continue;
                }
            }
        }

        if (results.isEmpty()) {
        	msg = new AuditMessage( contextName, "getInaccessibleResources-granted all", caller.getUsername(), resources.toArray(new String[resources.size()])); //$NON-NLS-1$
        	LogManager.log(MessageLevel.INFO, LogConstants.CTX_AUDITLOGGING, msg);
        } else {
        	msg = new AuditMessage( contextName, "getInaccessibleResources-denied", caller.getUsername(), resources.toArray(new String[resources.size()])); //$NON-NLS-1$
        	LogManager.log(MessageLevel.INFO, LogConstants.CTX_AUDITLOGGING, msg);
        }
        return results;
    }
    
    /**
     * Query <code>requests</code> for the <code>AuthorizationRealm</code> in
     * which they belong.
     * @param requests
     * @return The realm in which <i>all</i> the requests in the collection
     * belong.
     * @throws AuthorizationMgmtException if the request <i>do not all</i>
     * belong to the same realm.
     */
    private static AuthorizationRealm getRequestedRealm(final Collection requests)
            throws AuthorizationMgmtException {
        AuthorizationRealm theRealm = null;
        Iterator requestItr = requests.iterator();
        while (requestItr.hasNext()) {
            AuthorizationPermission aPerm = (AuthorizationPermission) requestItr.next();
            AuthorizationRealm aRealm = aPerm.getRealm();
            if ( theRealm != null ) {
                if ( ! theRealm.equals(aRealm) ) {
                    throw new AuthorizationMgmtException(DQPEmbeddedPlugin.Util.getString("AuthorizationServiceImpl.wrong_realms ")); //$NON-NLS-1$
                }
            } else {
                theRealm = aRealm;
            }
        }
        if ( theRealm == null ) {
            throw new AuthorizationMgmtException(DQPEmbeddedPlugin.Util.getString("AuthorizationServiceImpl.Authorization_Realm_is_null")); //$NON-NLS-1$
        }
        return theRealm;
    }
    
    @Override
    public boolean hasRole(String roleType, String roleName) throws MetaMatrixComponentException {
        
        AuthorizationRealm realm = null;
        
        if (ADMIN_ROLE.equalsIgnoreCase(roleType)) {
            realm = RolePermissionFactory.getRealm();
        } else if (DATA_ROLE.equalsIgnoreCase(roleType)){
            realm = getRealm(DQPWorkContext.getWorkContext());
        } else {
            return false;
        }
        
        try {
            return hasPolicy(realm, roleName);
        } catch (AuthorizationMgmtException err) {
            throw new MetaMatrixComponentException(err);
        }
    }
    
	private boolean matchesPrincipal(Set<MetaMatrixPrincipalName> principals, AuthorizationPolicy policy) {
		for (MetaMatrixPrincipalName principal : principals) {
			if (policy.getPrincipals().contains(principal)) {
				return true;
			}
		}
		return false;
	}
    
    private boolean hasPolicy(AuthorizationRealm realm, String policyName) throws AuthorizationMgmtException {

		if (isEntitled()) {
			return true;
		}

		Collection<AuthorizationPolicy> policies = getPoliciesForPrincipal(realm);

		HashSet applicablePolicies = new HashSet();
		applicablePolicies.add(policyName);

		if (realm == RolePermissionFactory.getRealm()) {
			if (AdminRoles.RoleName.ADMIN_PRODUCT.equals(policyName)) {
				applicablePolicies.add(AdminRoles.RoleName.ADMIN_SYSTEM);
			} else if (AdminRoles.RoleName.ADMIN_READONLY.equals(policyName)) {
				applicablePolicies.add(AdminRoles.RoleName.ADMIN_PRODUCT);
				applicablePolicies.add(AdminRoles.RoleName.ADMIN_SYSTEM);
			}
		}

		for (AuthorizationPolicy policy:policies) {
			if (applicablePolicies.contains(policy.getAuthorizationPolicyID().getDisplayName())) {
				return true;
			}
		}
		return false;
	}
    
    /**
     * Return a collection of all policies for which this principal has authorization, caching as needed.
     * Policies are returned for the principal and all groups in which the principal has membership.
     * <br><strong>NOTE:</strong> This method only goes to the authorization store when
     * <emph>none</emph> of the given principal's policies are found in the cache.
     * @param user the user account for which access is being checked; may not be null
     * (this is not checked for, however)
     * @return All policies for which the principal is authenticated - may be empty but never null.
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     * @throws MetaMatrixComponentException 
     */
    private Collection<AuthorizationPolicy> getPoliciesForPrincipal(AuthorizationRealm realm)
            throws AuthorizationMgmtException {

    	Set<AuthorizationPolicy> result = new HashSet<AuthorizationPolicy>();
    	Set<MetaMatrixPrincipalName> userRoles = getUserRoles();
    	if (userRoles.isEmpty()) {
    		return result;
    	}
    	
        Collection<AuthorizationPolicy> policies = getPoliciesInRealm(realm);
        
    	for (AuthorizationPolicy policy : policies) {
        	if (matchesPrincipal(userRoles, policy)) {
        		result.add(policy);
        		continue;
        	}
        }
        return result;
    }
    

	private Set<MetaMatrixPrincipalName> getUserRoles() {
		Set<MetaMatrixPrincipalName> roles = new HashSet<MetaMatrixPrincipalName>();
		Set<Principal> principals = DQPWorkContext.getWorkContext().getSubject().getPrincipals();
		for(Principal p: principals) {
			// this JBoss specific, but no code level dependencies
			if ((p instanceof Group) && p.getName().equals("Roles")){
				Group g = (Group)p;
				Enumeration rolesPrinciples = g.members();
				while(rolesPrinciples.hasMoreElements()) {
					roles.add(new MetaMatrixPrincipalName(((Principal)rolesPrinciples.nextElement()).getName(), MetaMatrixPrincipal.TYPE_GROUP));	
				}
			}
		}
		return roles;
	}

	@Override
    public Collection<AuthorizationPolicy> getPoliciesInRealm(AuthorizationRealm realm) throws AuthorizationMgmtException {

    	Collection<AuthorizationPolicy> policies = null;
        
    	VDBKey key = null;
    	
    	if (realm.getSubRealmName() != null) {
    		// get data roles for the user
        	key = new VDBKey(realm.getSuperRealmName(), realm.getSubRealmName());
	    	synchronized (this.policyCache) {
	        	policies = this.policyCache.get(key);
	        	if (policies == null ) {
					policies = getDataPolicies(realm);				
	        	}
	        	this.policyCache.put(key, policies);
	    	}
    	}
    	else {
    		// get admin roles
    		policies = getAdminPolicies();
    	}
    	return policies;
    }

	private Collection<AuthorizationPolicy> getDataPolicies(AuthorizationRealm realm) {
		Collection<AuthorizationPolicy> policies = null;
	    VDBMetaData vdb = this.vdbRepository.getVDB(realm.getSuperRealmName(), Integer.parseInt(realm.getSubRealmName()));
	    AuthorizationPoliciesHolder holder = vdb.getAttachment(AuthorizationPoliciesHolder.class);
	    
	    if (holder == null) {
	    	policies = Collections.emptyList();
	    }
	    else {
	    	policies = holder.getAuthorizationPolicies();
	    	//AuthorizationPolicyFactory.buildPolicies(vdb.getName(), String.valueOf(vdb.getVersion()), vdb.getDataRoles());
	    }
		return policies;
	}
	
	private Collection<AuthorizationPolicy> getAdminPolicies() {
		return adminPolicies;
	}
    
	@Override
    public void updatePoliciesInRealm(AuthorizationRealm realm, Collection<AuthorizationPolicy> policies) throws AuthorizationMgmtException {
        
    	if (realm.getSubRealmName() != null) {
        	VDBKey key = new VDBKey(realm.getSuperRealmName(), realm.getSubRealmName());
        	synchronized (this.policyCache) {
            	policies = this.policyCache.get(key);
	        	if (policies != null) {
	        		this.policyCache.remove(key);
	        	}
		        VDBMetaData vdb = this.vdbRepository.getVDB(realm.getSuperRealmName(), Integer.parseInt(realm.getSubRealmName()));
		        AuthorizationPoliciesHolder holder = new AuthorizationPoliciesHolder();
		        holder.setAuthorizationPolicies(policies);
		        vdb.addAttchment(AuthorizationPoliciesHolder.class, holder);
		        //vdb.setDataRoles(AuthorizationPolicyFactory.exportPolicies(policies));
				this.policyCache.put(key, policies);
        	}
        }
    	else {
    		// there is no admin API way to update the Admin Roles.
    		this.adminPolicies = policies;
    	}
    }
    
    protected boolean isEntitled(){
        if (DQPWorkContext.getWorkContext().getSubject() == null) {
            LogManager.logDetail(com.metamatrix.common.util.LogConstants.CTX_AUTHORIZATION,new Object[]{ "Automatically entitling principal", DQPWorkContext.getWorkContext().getSessionToken().getUsername()}); //$NON-NLS-1$ 
            return true;
        }
        return false;
    }

    /**
     * Determine whether entitlements checking is enabled on the server.
     *
     * @return <code>true</code> iff server-side entitlements checking is enabled.
     */
    @Override
    @ManagementProperty(description="Turn on checking the entitlements on resources based on the roles defined in VDB", readOnly=true)
    public boolean checkingEntitlements() {
        return useEntitlements;
    }

    
    /**
     * Create realm based on token
     * @param token Used to find info about this session
     * @return Realm to use (based on vdb name and version)
     */
    private AuthorizationRealm getRealm(DQPWorkContext context) {
        return new AuthorizationRealm(context.getVdbName(), String.valueOf(context.getVdbVersion()));
    }

    private AuthorizationActions getActions(int actionCode) {
        switch(actionCode) {
            case AuthorizationService.ACTION_READ: return StandardAuthorizationActions.DATA_READ;
            case AuthorizationService.ACTION_CREATE: return StandardAuthorizationActions.DATA_CREATE;
            case AuthorizationService.ACTION_UPDATE: return StandardAuthorizationActions.DATA_UPDATE;
            case AuthorizationService.ACTION_DELETE: return StandardAuthorizationActions.DATA_DELETE;
            default: return StandardAuthorizationActions.DATA_READ;
        }
    }

    /**
     * Take a list of resources (Strings) and create a list of permissions
     * suitable for sending to the authorization service.
     * @param realm Realm to use
     * @param resources Collection of String, listing resources
     * @param actions Actions to check for
     * @return Collection of BasicAuthorizationPermission
     */
    private Collection createPermissions(AuthorizationRealm realm, Collection resources, AuthorizationActions actions) {
        List permissions = new ArrayList(resources.size());
        Iterator iter = resources.iterator();
        while(iter.hasNext()) {
            String resource = (String) iter.next();

            BasicAuthorizationPermission permission =
                (BasicAuthorizationPermission) PERMISSION_FACTORY.create(resource, realm, actions);

            permissions.add(permission);
        }
        return permissions;
    }

    public void setVDBRepository(VDBRepository repo) {
    	this.vdbRepository = repo;
    }
    
	public void setUseEntitlements(Boolean useEntitlements) {
		this.useEntitlements = useEntitlements.booleanValue();
	}

	@Override
	public boolean isCallerInRole(String roleName) throws AuthorizationMgmtException {
        LogManager.logTrace(com.metamatrix.common.util.LogConstants.CTX_AUTHORIZATION, new Object[]{"isCallerInRole(", getSession(), roleName, ")"}); //$NON-NLS-1$ //$NON-NLS-2$
        return hasPolicy(RolePermissionFactory.getRealm(), roleName);
	}

	SessionToken getSession() {
		return DQPWorkContext.getWorkContext().getSessionToken();
	}
}
