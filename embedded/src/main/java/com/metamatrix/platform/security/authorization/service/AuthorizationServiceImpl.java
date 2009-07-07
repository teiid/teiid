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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.teiid.adminapi.AdminRoles;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.xml.sax.SAXException;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.api.exception.security.InvalidPrincipalException;
import com.metamatrix.api.exception.security.MembershipServiceException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.LRUCache;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.AuditMessage;
import com.metamatrix.dqp.service.AuthorizationService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.platform.security.api.AuthorizationActions;
import com.metamatrix.platform.security.api.AuthorizationPermission;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyFactory;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.BasicAuthorizationPermission;
import com.metamatrix.platform.security.api.BasicAuthorizationPermissionFactory;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.StandardAuthorizationActions;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.util.RolePermissionFactory;
import com.metamatrix.server.util.ServerAuditContexts;
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
public class AuthorizationServiceImpl implements AuthorizationService {

	/*
	 * Configuration state
	 */
    private boolean useEntitlements; 

	/*
	 * Injected state
	 */
	protected MembershipServiceInterface membershipService;
	protected VDBService vdbService;
	protected LRUCache<VDBKey, Collection<AuthorizationPolicy>> policyCache = new LRUCache<VDBKey, Collection<AuthorizationPolicy>>();

    // Permission factory is reusable and thread safe
    private static final BasicAuthorizationPermissionFactory PERMISSION_FACTORY = new BasicAuthorizationPermissionFactory();
    
    Collection<AuthorizationPolicy> adminPolicies;
    
    /*
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     */
    public void initialize(Properties env) throws ApplicationInitializationException {
    	this.useEntitlements = PropertiesUtils.getBooleanProperty(env, ENTITELEMENTS_ENABLED, false);
    }

    /*
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     */
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
    }

    /*
     * @see com.metamatrix.common.application.ApplicationService#stop()
     */
    public void stop() throws ApplicationLifecycleException {
    }

    /*
     */
    public Collection getInaccessibleResources(String connectionID, int action, Collection resources, int context)
        throws MetaMatrixComponentException {
        AuthorizationRealm realm = getRealm(DQPWorkContext.getWorkContext());
        AuthorizationActions actions = getActions(action);
        Collection permissions = createPermissions(realm, resources, actions);
        String auditContext = getAuditContext(context);
        Collection inaccessableResources = Collections.EMPTY_LIST;
        try {
            inaccessableResources = getInaccessibleResources(getSession(), auditContext, permissions);
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
    private Collection getInaccessibleResources(SessionToken caller, String contextName, Collection requests)
            throws AuthorizationMgmtException {
        LogManager.logDetail(com.metamatrix.common.util.LogConstants.CTX_AUTHORIZATION, new Object[]{"getInaccessibleResources(", caller, ", ", contextName, ", ", requests, ")"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        
        List resources = new ArrayList();
        if (requests != null && ! requests.isEmpty()) {            
            Iterator permItr = requests.iterator();
            while ( permItr.hasNext() ) {
                resources.add(((AuthorizationPermission)permItr.next()).getResourceName());
            }            
        }
        
        // Audit - request
    	AuditMessage msg = new AuditMessage( contextName, "getInaccessibleResources-request", caller.getUsername(), resources.toArray()); //$NON-NLS-1$
    	LogManager.log(MessageLevel.INFO, LogConstants.CTX_AUDITLOGGING, msg);
        
        if (isEntitled(caller.getUsername())) {
            return Collections.EMPTY_LIST;
        }

        Collection results = new HashSet(requests);
        try {
            Collection policies = this.getPoliciesForPrincipal(new MetaMatrixPrincipalName(caller.getUsername(), MetaMatrixPrincipal.TYPE_USER), getRequestedRealm(requests));

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
        } catch (InvalidPrincipalException e) {
            throw new AuthorizationMgmtException(e, DQPEmbeddedPlugin.Util.getString("AuthorizationServiceImpl.invalid_session")); //$NON-NLS-1$
        }

        if (results.isEmpty()) {
        	msg = new AuditMessage( contextName, "getInaccessibleResources-granted all", caller.getUsername(), resources.toArray()); //$NON-NLS-1$
        	LogManager.log(MessageLevel.INFO, LogConstants.CTX_AUDITLOGGING, msg);
        } else {
        	msg = new AuditMessage( contextName, "getInaccessibleResources-denied", caller.getUsername(), resources.toArray()); //$NON-NLS-1$
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
    
    public boolean hasRole(String connectionID, String roleType, String roleName) throws MetaMatrixComponentException {
        
        AuthorizationRealm realm = null;
        
        if (ADMIN_ROLE.equalsIgnoreCase(roleType)) {
            realm = RolePermissionFactory.getRealm();
        } else if (DATA_ROLE.equalsIgnoreCase(roleType)){
            realm = getRealm(DQPWorkContext.getWorkContext());
        } else {
            return false;
        }
        
        try {
            return hasPolicy(getSession(), realm, roleName);
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
    
    private boolean hasPolicy(SessionToken session, AuthorizationRealm realm, String policyName) throws AuthorizationMgmtException {

		if (isEntitled(session.getUsername())) {
			return true;
		}

		Collection<AuthorizationPolicy> policies;
		try {
			policies = getPoliciesForPrincipal(new MetaMatrixPrincipalName(session.getUsername(), MetaMatrixPrincipal.TYPE_USER), realm);
		} catch (InvalidPrincipalException e) {
			throw new AuthorizationMgmtException(e);
		}

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
    
    public Collection<String> getRoleNamesForPrincipal(MetaMatrixPrincipalName principal) throws AuthorizationMgmtException {
		Collection policies;
		try {
			policies = getPoliciesForPrincipal(principal, RolePermissionFactory.getRealm());
		} catch (InvalidPrincipalException e) {
			throw new AuthorizationMgmtException(e);
		}

		Collection<String> results = new HashSet<String>();

		for (Iterator i = policies.iterator(); i.hasNext();) {
			AuthorizationPolicy policy = (AuthorizationPolicy) i.next();
			results.add(policy.getAuthorizationPolicyID().getDisplayName());
		}

		return results;
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
     * @throws InvalidPrincipalException if the principal is invalid.
     * @throws MetaMatrixComponentException 
     */
    private Collection<AuthorizationPolicy> getPoliciesForPrincipal(MetaMatrixPrincipalName user, AuthorizationRealm realm)
            throws AuthorizationMgmtException, InvalidPrincipalException {

    	Set<AuthorizationPolicy> result = new HashSet<AuthorizationPolicy>();
    	
    	Set<MetaMatrixPrincipalName> principals = getGroupsForPrincipal(user);
    	
        if (principals == null || principals.size() == 0) {
            return result;
        }
        
        Collection<AuthorizationPolicy> policies = getPoliciesInRealm(realm);
        
    	for (AuthorizationPolicy policy : policies) {
        	if (matchesPrincipal(principals, policy)) {
        		result.add(policy);
        		continue;
        	}
        }
        return result;
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

	private Collection<AuthorizationPolicy> getDataPolicies(AuthorizationRealm realm) throws AuthorizationMgmtException {
		Collection<AuthorizationPolicy> policies = null;
		try {
		    VDBArchive vdb = vdbService.getVDB(realm.getSuperRealmName(), realm.getSubRealmName());
		    if (vdb.getDataRoles() == null) {
		    	policies = Collections.emptyList();
		    }
		    else {
		    	policies = AuthorizationPolicyFactory.buildPolicies(vdb.getName(), vdb.getVersion(), vdb.getDataRoles());
		    }
		} catch (SAXException e) {
			throw new AuthorizationMgmtException(e);
		} catch (IOException e) {
			throw new AuthorizationMgmtException(e);
		} catch (ParserConfigurationException e) {
			throw new AuthorizationMgmtException(e);
		} catch (MetaMatrixComponentException e) {
			throw new AuthorizationMgmtException(e);
		}
		return policies;
	}
	
	private Collection<AuthorizationPolicy> getAdminPolicies() {
		return adminPolicies;
	}
    
    public void updatePoliciesInRealm(AuthorizationRealm realm, Collection<AuthorizationPolicy> policies) throws AuthorizationMgmtException {
        
    	if (realm.getSubRealmName() != null) {
        	VDBKey key = new VDBKey(realm.getSuperRealmName(), realm.getSubRealmName());
        	synchronized (this.policyCache) {
            	policies = this.policyCache.get(key);
	        	if (policies != null) {
	        		this.policyCache.remove(key);
	        	}
				try {
			        VDBArchive vdb = vdbService.getVDB(realm.getSuperRealmName(), realm.getSubRealmName());
			        char[] dataRoles = AuthorizationPolicyFactory.exportPolicies(policies);
			        vdb.updateRoles(dataRoles);
				} catch (IOException e) {
					throw new AuthorizationMgmtException(e);
				} catch (MetaMatrixComponentException e) {
					throw new AuthorizationMgmtException(e);
				}
				this.policyCache.put(key, policies);
        	}
        }
    	else {
    		// there is no admin API way to update the Admin Roles.
    		this.adminPolicies = policies;
    	}
    }
    
    /**
     * Return all the groups that this prinicpal is a member of <i>and</i> the
     * given principal (implies this principal is a member of itself).
     * @param principal the principal for which to look for groups (may itself be a group).
     * @return the given principal and all groups of which it is a member.
     * @throws AuthorizationMgmtException if an error occurs while contacting the Membership svc.
     * @throws InvalidPrincipalException if the given principal is invalid.
     */
    private Set<MetaMatrixPrincipalName> getGroupsForPrincipal(MetaMatrixPrincipalName principal)
            throws AuthorizationMgmtException, InvalidPrincipalException {
        
    	LogManager.logDetail(com.metamatrix.common.util.LogConstants.CTX_AUTHORIZATION, new Object[] {"getGroupsForPrincipal(", principal, ") - Getting all group memberships."}); //$NON-NLS-1$ //$NON-NLS-2$
        // Get the set of all groups this Principal is a member of
        Set<MetaMatrixPrincipalName> allPrincipals = new HashSet<MetaMatrixPrincipalName>();
        try {
            Collection groups = Collections.EMPTY_SET;
            if (principal.getType() == MetaMatrixPrincipal.TYPE_USER || principal.getType() == MetaMatrixPrincipal.TYPE_ADMIN) {
                groups = membershipService.getGroupsForUser(principal.getName());
            } else if (principal.getType() == MetaMatrixPrincipal.TYPE_GROUP) {
            	MetaMatrixPrincipal groupPrincipal = membershipService.getPrincipal(principal);
            	groups = new HashSet();
            	groups.add(groupPrincipal.getName());
            }
            Iterator memberItr = groups.iterator();
            // Add all principals that each orig is member of
            while (memberItr.hasNext()) {
                // HACK: Convert ALL member principals to MetaMatrixPrincipalName objs
                // since Auth and Memb svcs don't speak the same language.
                MetaMatrixPrincipalName member = new MetaMatrixPrincipalName((String) memberItr.next(), MetaMatrixPrincipal.TYPE_GROUP);
                LogManager.logDetail(com.metamatrix.common.util.LogConstants.CTX_AUTHORIZATION, new Object[]{"getGroupsForPrincipal(", principal, ") - Adding membership <", member, ">"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                allPrincipals.add(member);
            }
            // Add original Principal, now that we know he's been authenticated.
            allPrincipals.add(principal);
        } catch (InvalidPrincipalException e) {
        	throw e;
        } catch (MetaMatrixSecurityException e) {
            throw new AuthorizationMgmtException(e, DQPEmbeddedPlugin.Util.getString("AuthorizationServiceImpl.error_seraching_policies")); //$NON-NLS-1$
        }
        return allPrincipals;
    }

    protected boolean isEntitled(String principal) {
        try {
            if (membershipService.isSuperUser(principal) || !membershipService.isSecurityEnabled()) {
                LogManager.logDetail(com.metamatrix.common.util.LogConstants.CTX_AUTHORIZATION,new Object[]{ "Automatically entitling principal", principal}); //$NON-NLS-1$ 
                return true;
            }
        }  catch (MembershipServiceException e) {
            LogManager.logError(com.metamatrix.common.util.LogConstants.CTX_AUTHORIZATION, e, DQPEmbeddedPlugin.Util.getString("AuthorizationServiceImpl.failed_to_get_groups")); //$NON-NLS-1$
        } 
        return false;
    }

    /**
     * Determine whether entitlements checking is enabled on the server.
     *
     * @return <code>true</code> iff server-side entitlements checking is enabled.
     */
    public boolean checkingEntitlements() {
        return useEntitlements;
    }

    /**
     * Create realm based on token
     * @param token Used to find info about this session
     * @return Realm to use (based on vdb name and version)
     */
    private AuthorizationRealm getRealm(DQPWorkContext context) {
        return new AuthorizationRealm(context.getVdbName(),context.getVdbVersion());
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

    private String getAuditContext(int auditCode) {
        switch(auditCode) {
            case AuthorizationService.CONTEXT_QUERY:    return ServerAuditContexts.CTX_QUERY;
            case AuthorizationService.CONTEXT_INSERT:   return ServerAuditContexts.CTX_INSERT;
            case AuthorizationService.CONTEXT_UPDATE:   return ServerAuditContexts.CTX_UPDATE;
            case AuthorizationService.CONTEXT_DELETE:   return ServerAuditContexts.CTX_DELETE;
            case AuthorizationService.CONTEXT_PROCEDURE:    return ServerAuditContexts.CTX_PROCEDURE;
            default: return ServerAuditContexts.CTX_QUERY;
        }
    }
    
    @Inject
	public void setMembershipService(MembershipServiceInterface membershipService) {
		this.membershipService = membershipService;
	}
    
    @Inject
    public void setAdminPolicies(@Named("AdminRoles") Collection<AuthorizationPolicy> adminPolicies) {
    	this.adminPolicies = adminPolicies;
    }

	public void setUseEntitlements(boolean useEntitlements) {
		this.useEntitlements = useEntitlements;
	}

	@Inject
	public void setVdbService(VDBService vdbService) {
		this.vdbService = vdbService;
	}

	@Override
	public boolean isCallerInRole(SessionToken session, String roleName) throws AuthorizationMgmtException {
        LogManager.logTrace(com.metamatrix.common.util.LogConstants.CTX_AUTHORIZATION, new Object[]{"isCallerInRole(", session, roleName, ")"}); //$NON-NLS-1$ //$NON-NLS-2$
        
        return hasPolicy(session, RolePermissionFactory.getRealm(), roleName);
	}

	
	SessionToken getSession() {
		return DQPWorkContext.getWorkContext().getSessionToken();
	}
}
