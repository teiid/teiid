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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.teiid.adminapi.AdminOptions;
import org.teiid.adminapi.AdminRoles;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.api.exception.security.InvalidPrincipalException;
import com.metamatrix.api.exception.security.MembershipServiceException;
import com.metamatrix.cache.CacheConfiguration;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.Cache.Type;
import com.metamatrix.cache.CacheConfiguration.Policy;
import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.actions.CreateObject;
import com.metamatrix.common.actions.DestroyObject;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.connection.TransactionMgr;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.properties.UnmodifiableProperties;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.common.util.LogContextsUtil;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.dqp.service.AuditMessage;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.admin.api.EntitlementMigrationReport;
import com.metamatrix.platform.admin.api.PermissionDataNode;
import com.metamatrix.platform.admin.api.PermissionDataNodeTreeView;
import com.metamatrix.platform.admin.api.exception.PermissionNodeException;
import com.metamatrix.platform.admin.apiimpl.PermissionDataNodeTreeViewImpl;
import com.metamatrix.platform.security.api.AuthorizationObjectEditor;
import com.metamatrix.platform.security.api.AuthorizationPermission;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.GranteeEntitlementEntry;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.StandardAuthorizationActions;
import com.metamatrix.platform.security.api.UserEntitlementInfo;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.security.api.service.AuthorizationServicePropertyNames;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.audit.AuditManager;
import com.metamatrix.platform.security.audit.SecurityAuditContexts;
import com.metamatrix.platform.security.authorization.cache.AuthorizationCache;
import com.metamatrix.platform.security.authorization.spi.AuthorizationSourceConnectionException;
import com.metamatrix.platform.security.authorization.spi.AuthorizationSourceException;
import com.metamatrix.platform.security.authorization.spi.AuthorizationSourceTransaction;
import com.metamatrix.platform.security.util.RolePermissionFactory;
import com.metamatrix.platform.service.api.exception.ServiceClosedException;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.api.exception.ServiceNotInitializedException;
import com.metamatrix.platform.service.api.exception.ServiceStateException;
import com.metamatrix.platform.service.controller.AbstractService;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;
import com.metamatrix.platform.util.PlatformProxyHelper;

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
public class AuthorizationServiceImpl extends AbstractService implements AuthorizationServiceInterface {
	
    protected AuthorizationCache authorizationCache;
    private Properties environment;
    private int retries = 1;
    private boolean serviceClosed;

    protected MembershipServiceInterface membershipServiceProxy;

    private SessionToken privlegedToken = new SessionToken();

   /**
    * The transaction mgr for ManagedConnections.
    */
    private TransactionMgr transMgr;
    
    private AuditManager auditManager;

    // -----------------------------------------------------------------------------------
    //                 S E R V I C E - R E L A T E D    M E T H O D S
    // -----------------------------------------------------------------------------------

    /**
     * Perform initialization and commence processing. This method is called only once.
     */
    protected void initService(Properties env) {

        try {
        	this.auditManager = AuditManager.getInstance();
        	
            membershipServiceProxy = PlatformProxyHelper.getMembershipServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);

            if (env == null) {
                this.environment = new Properties();
            } else {
                synchronized (env) {
                    this.environment = (Properties) env.clone(); // should copy this by values!!!
                }
            }
            if (!(this.environment instanceof UnmodifiableProperties)) {
                this.environment = new UnmodifiableProperties(this.environment);
            }

            String retryValue = environment.getProperty(AuthorizationServicePropertyNames.CONNECTION_RETRIES);
            if (retryValue != null) {
                try {
                    this.retries = Integer.parseInt(retryValue);
                } catch (Exception e) {
                    LogManager.logWarning(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0001, retryValue, Integer.toString(this.retries)));
                }
            }

            if (env.getProperty(AuthorizationServicePropertyNames.CONNECTION_FACTORY) == null) {
            	env.setProperty(AuthorizationServicePropertyNames.CONNECTION_FACTORY, AuthorizationServicePropertyNames.DEFAULT_FACTORY_CLASS);
            }
            env.setProperty(TransactionMgr.FACTORY, env.getProperty(AuthorizationServicePropertyNames.CONNECTION_FACTORY));

            transMgr = new TransactionMgr(env, this.getInstanceName());

            // Initialize cache
            CacheFactory cf = ResourceFinder.getCacheFactory();
            CacheConfiguration config = new CacheConfiguration(Policy.LRU, 0, 0);
            this.authorizationCache = new AuthorizationCache(cf.get(Type.AUTHORIZATION_POLICY, config), cf.get(Type.AUTHORIZATION_PRINCIPAL, config),environment);

            this.serviceClosed = false;

        } catch (Throwable e) {
            throw new ServiceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0004,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0004, this.getID()));
        }
    }

    protected AuthorizationSourceTransaction getReadTransaction() throws ManagedConnectionException {
        return (AuthorizationSourceTransaction) this.transMgr.getReadTransaction();
    }

    protected AuthorizationSourceTransaction getWriteTransaction() throws ManagedConnectionException {
        return (AuthorizationSourceTransaction) this.transMgr.getWriteTransaction();
    }

    /**
     * Close the service to new work if applicable. After this method is called
     * the service should no longer accept new work to perform but should continue
     * to process any outstanding work. This method is called by die().
     */
    protected void closeService() throws Exception {
        if ( ! serviceClosed ) {
            String instanceName = this.getInstanceName();
            LogManager.logInfo(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(LogMessageKeys.SEC_AUTHORIZATION_0001, instanceName));

            serviceClosed = true;
            auditManager.stop();

            LogManager.logInfo(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(LogMessageKeys.SEC_AUTHORIZATION_0002, instanceName));
        }
    }

    /**
     * Wait until the service has completed all outstanding work. This method
     * is called by die() just before calling dieNow().
     */
    protected void waitForServiceToClear() throws Exception {
        try {
            this.closeService();
        } catch (Exception e) {
            LogManager.logError(LogConstants.CTX_AUTHORIZATION, e,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0009));
        }
    }

    /**
     * Terminate all processing and reclaim resources. This method is called by dieNow()
     * and is only called once.
     */
    protected void killService() {
        try {
            this.closeService();
        } catch (Exception e) {
            LogManager.logError(LogConstants.CTX_AUTHORIZATION, e,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0009));
        }
    }

//==============================================================================
// Authorization Service Methods - Client access
//==============================================================================
    /**
     * Return whether the specified account has authorization to access the specified resource.
     * @param sessionToken the session token of the principal whose access is being checked
     * @param contextName the name of the context for the caller (@see AuditContext)
     * @param request the permission that details the resource and the desired form of access
     * @return true if the specified principal is granted access to the requested resource,
     * or false otherwise
     * @throws InvalidSessionException if the session token for this cache is not valid
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation
     */
    public boolean checkAccess(SessionToken sessionToken, String contextName, AuthorizationPermission request)
            throws InvalidSessionException, AuthorizationMgmtException {
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, new Object[]{"checkAccess(", sessionToken, ", ", contextName, ", ", request, ")"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        
        // Audit - request
        AuditMessage msg = new AuditMessage(contextName, "checkAccess-request", sessionToken.getUsername(),  new Object[]{request.getResourceName()}); //$NON-NLS-1$
        LogManager.log(MessageLevel.INFO, LogContextsUtil.CommonConstants.CTX_AUDITLOGGING, msg);
        
        boolean hasAccess = checkAccess(sessionToken, contextName, request, false);

        if (!hasAccess) {
            // Audit - denied
            msg = new AuditMessage(contextName, "checkAccess-denied", sessionToken.getUsername(),  new Object[]{request.getResourceName()}); //$NON-NLS-1$
            LogManager.log(MessageLevel.INFO, LogContextsUtil.CommonConstants.CTX_AUDITLOGGING, msg);
        }
        return hasAccess;
    }

    /**
     * Return whether the specified account has authorization to access the specified resource
     * and all its dependant resources.
     * 
     * @param sessionToken    the session token of the principal whose access is being checked
     * @param contextName     the name of the context for the caller (@see AuditContext)
     * @param request         the permission that details the resource and the desired form of access
     * @param fetchDependants If <code>true</code>, search authorization store for all dependant
     *                        permisssions of the given request. Access is checked for <i>all</i> resources - the given
     *                        request and all dependants.
     * @return true if the specified principal is granted access to the requested resources,
     *         or false otherwise
     * @throws com.metamatrix.admin.api.exception.security.InvalidSessionException
     *                                  if the session token for this cache is not valid
     * @throws com.metamatrix.api.exception.security.AuthorizationMgmtException
     *                                  if this service is unable to locate resources required
     *                                  for this operation
     */
    public boolean checkAccess(SessionToken sessionToken, String contextName, AuthorizationPermission request, boolean fetchDependants)
            throws InvalidSessionException, AuthorizationMgmtException {
        Collection requests = new ArrayList();
        if ( fetchDependants ) {
            requests = getDependantRequests(request);
        } else {
            requests.add(request);
        }
        return getInaccessibleResources(sessionToken, contextName, requests).isEmpty();
    }

    /**
     * Of those resources specified, return the subset for which the specified account
     * does <emph>NOT</emph> have authorization to access.
     * @param sessionToken the session token of the principal that is calling this method
     * @param contextName the name of the context for the caller (@see AuditContext)
     * @param requests the permissions that detail the resources and the desired form of access
     * @return the subset of <code>requests</code> that the account does <i>not</i> have access to
     * @throws InvalidSessionException if the session token for this cache is not valid
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation
     */
    public Collection getInaccessibleResources(SessionToken sessionToken, String context, Collection requests)
            throws InvalidSessionException, AuthorizationMgmtException {
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, new Object[]{"getInaccessibleResources(", sessionToken, ", ", context, ", ", requests, ")"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        
        List resources = new ArrayList();
        if (requests != null && ! requests.isEmpty()) {            
            Iterator permItr = requests.iterator();
            while ( permItr.hasNext() ) {
                resources.add(((AuthorizationPermission)permItr.next()).getResourceName());
            }            
        }
        
        // Audit - request
        if (!resources.isEmpty()) {
        	AuditMessage msg = new AuditMessage( context, "getInaccessibleResources-request", sessionToken.getUsername(), resources.toArray()); //$NON-NLS-1$
        	LogManager.log(MessageLevel.INFO, context, msg);
        }
        
        if (isEntitled(sessionToken.getUsername())) {
            return Collections.EMPTY_LIST;
        }

        Collection results = new HashSet(requests);
        try {
            Collection policies = this.getPoliciesForPrincipal(new MetaMatrixPrincipalName(sessionToken.getUsername(), MetaMatrixPrincipal.TYPE_USER), sessionToken, getRequestedRealm(requests));

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
            String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0020);
            throw new InvalidSessionException(e, msg);
        }

        if (results.isEmpty()) {
            // Audit - granted all requests
            if (!resources.isEmpty()) {
            	AuditMessage msg = new AuditMessage( context, "getInaccessibleResources-granted all", sessionToken.getUsername(), resources.toArray()); //$NON-NLS-1$
            	LogManager.log(MessageLevel.INFO, context, msg);
            }
        	
        } else {
            // Audit - denied
            if (!resources.isEmpty()) {
            	AuditMessage msg = new AuditMessage( context, "getInaccessibleResources-denied", sessionToken.getUsername(), resources.toArray()); //$NON-NLS-1$
            	LogManager.log(MessageLevel.INFO, context, msg);
            }
        }
        return results;
    }

    /**
     * Returns a <code>List</code> of entitlements to the given fully qualified group name in the given realm.
     * <p>The list contains objects of type {@link com.metamatrix.platform.security.api.UserEntitlementInfo UserEntitlementInfo}
     * which will contain all user entitlement information for each group found. Each of these objects
     * will contain 1 or more objects of type {@link com.metamatrix.platform.security.api.GranteeEntitlementEntry GranteeEntitlementEntry}
     * which contain the <i>Grantee</i>'s name the entitlement <i>Grantor</i> or entity specifying the <i>Grantee</i>
     * is entitled and the <i>Allowed Actions</i> the <i>Grantee</i> is entitled to perform on the group.</p>
     * The attributes availible are:
     * <ol>
     *   <li value=1>VDB Name</li>
     *   <li>VDB Version</li>
     *   <li>Group Name (fully qualified)</li>
     *      <ul>
     *          <li>Grantee Name; Grantor Name; Allowed Actions (A <code>String[]</code> of one or more of {CREATE, READ, UPDATE, DELETE})</li>
     *          <li> ... </li>
     *      </ul>
     * </ol>
     * @param realm The realm in which the element must live.
     * @param groupNames the fully qualified group names - the resources - for which to look up permissions.
     * Collection of <code>String</code>.
     * @return The <code>List</code> of entitlements to the given element in the
     * given realm - May be empty but never null.
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation.
     */
    public List getGroupEntitlements(AuthorizationRealm realm, Collection groupNames)
            throws AuthorizationMgmtException{
        List entitlements = new ArrayList();
        for (Iterator groupItr = groupNames.iterator(); groupItr.hasNext();) {
            String groupName = (String) groupItr.next();
            entitlements.addAll(this.getGroupEntitlements(realm, groupName));
        }
        return entitlements;
    }

    /**
     * Returns a <code>List</code> of entitlements to the given fully qualified group name in the given realm.
     * <p>The list contains objects of type {@link com.metamatrix.platform.security.api.UserEntitlementInfo UserEntitlementInfo}
     * which will contain all user entitlement information for each group found. Each of these objects
     * will contain 1 or more objects of type {@link com.metamatrix.platform.security.api.GranteeEntitlementEntry GranteeEntitlementEntry}
     * which contain the <i>Grantee</i>'s name the entitlement <i>Grantor</i> or entity specifying the <i>Grantee</i>
     * is entitled and the <i>Allowed Actions</i> the <i>Grantee</i> is entitled to perform on the group.</p>
     * The attributes availible are:
     * <ol>
     *   <li value=1>VDB Name</li>
     *   <li>VDB Version</li>
     *   <li>Group Name (fully qualified)</li>
     *      <ul>
     *          <li>Grantee Name; Grantor Name; Allowed Actions (A <code>String[]</code> of one or more of {CREATE, READ, UPDATE, DELETE})</li>
     *          <li> ... </li>
     *      </ul>
     * </ol>
     * @param realm The realm in which the element must live.
     * @param fullyQualifiedGroupName The resource for which to look up permissions.
     * @return The <code>List</code> of entitlements to the given element in the
     * given realm - May be empty but never null.
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation.
     * @throws ServiceStateException if the Authorization service is not taking requests.
     */
    public List getGroupEntitlements(AuthorizationRealm realm, String fullyQualifiedGroupName)
            throws AuthorizationMgmtException {
        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,
                new Object[]{"getGroupEntitlements(", realm, fullyQualifiedGroupName, ")"}); //$NON-NLS-1$ //$NON-NLS-2$
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        // Can't allow any metachars
        if (fullyQualifiedGroupName.indexOf('%') > 0) {
            throw new AuthorizationMgmtException(ErrorMessageKeys.SEC_AUTHORIZATION_0022,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0022));
        }

        Map entitlementMap = new HashMap();
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                entitlementMap = transaction.getGroupEntitlements(realm, fullyQualifiedGroupName);
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0023);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0023, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0024);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0024, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0025);
                throw new AuthorizationMgmtException(e, exceptionMsg);
            } finally {
                completeTransaction(success, transaction);
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "Attempting to retry getting entitlements for resource."); //$NON-NLS-1$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }

        return buildEntitlementList(realm, entitlementMap, true);
    }

    /**
     * Returns a <code>List</code> of entitlements to the given element pattern in the given realm.
     * <p>The list contains objects of type {@link com.metamatrix.platform.security.api.UserEntitlementInfo UserEntitlementInfo}
     * which will contain all user entitlement information for each element found. Each of these objects
     * will contain 1 or more objects of type {@link com.metamatrix.platform.security.api.GranteeEntitlementEntry GranteeEntitlementEntry}
     * which contain the <i>Grantee</i>'s name the entitlement <i>Grantor</i> or entity specifying the <i>Grantee</i>
     * is entitled and the <i>Allowed Actions</i> the <i>Grantee</i> is entitled to perform on the element.</p>
     * The attributes availible are:
     * <ol>
     *   <li value=1>VDB Name</li>
     *   <li>VDB Version</li>
     *   <li>Group Name (fully qualified)</li>
     *   <li>Element Name (fully qualified)</li>
     *      <ul>
     *          <li>Grantee Name; Grantor Name; Allowed Actions (A <code>String[]</code> of one or more of {CREATE, READ, UPDATE, DELETE})</li>
     *          <li> ... </li>
     *      </ul>
     * </ol>
     * @param realm The realm in which the element must live.
     * @param elementNames The fully qualified element resource for which to look up permissions. Collection of <code>String</code>.
     * @return The <code>List</code> of entitlements to the given element in the
     * given realm - May be empty but never null.
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation.
     * @throws ServiceStateException if the Authorization service is not taking requests.
     */
    public List getElementEntitlements(AuthorizationRealm realm, Collection elementNames)
            throws AuthorizationMgmtException {
        List entitlements = new ArrayList();
        for (Iterator eleItr = elementNames.iterator(); eleItr.hasNext();) {
            String elementName = (String) eleItr.next();
            entitlements.addAll(this.getElementEntitlements(realm, elementName));
        }
        return entitlements;
    }

    /**
     * Returns a <code>List</code> of entitlements to the given element pattern in the given realm.
     * <p>The list contains objects of type {@link com.metamatrix.platform.security.api.UserEntitlementInfo UserEntitlementInfo}
     * which will contain all user entitlement information for each element found. Each of these objects
     * will contain 1 or more objects of type {@link com.metamatrix.platform.security.api.GranteeEntitlementEntry GranteeEntitlementEntry}
     * which contain the <i>Grantee</i>'s name the entitlement <i>Grantor</i> or entity specifying the <i>Grantee</i>
     * is entitled and the <i>Allowed Actions</i> the <i>Grantee</i> is entitled to perform on the element.</p>
     * The attributes availible are:
     * <ol>
     *   <li value=1>VDB Name</li>
     *   <li>VDB Version</li>
     *   <li>Group Name (fully qualified)</li>
     *   <li>Element Name (fully qualified)</li>
     *      <ul>
     *          <li>Grantee Name; Grantor Name; Allowed Actions (A <code>String[]</code> of one or more of {CREATE, READ, UPDATE, DELETE})</li>
     *          <li> ... </li>
     *      </ul>
     * </ol>
     * @param realm The realm in which the element must live.
     * @param elementNamePattern The resource for which to look up permissions. SQL '%' pattern matching may be used.
     * @return The <code>List</code> of entitlements to the given element in the
     * given realm - May be empty but never null.
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation.
     * @throws ServiceStateException if the Authorization service is not taking requests.
     */
    public List getElementEntitlements(AuthorizationRealm realm, String elementNamePattern)
            throws AuthorizationMgmtException {
        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,
                new Object[]{"getElementEntitlements(", realm, elementNamePattern, ")"}); //$NON-NLS-1$ //$NON-NLS-2$
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        Map entitlementMap = new HashMap();
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                entitlementMap = transaction.getElementEntitlements(realm, elementNamePattern);
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0023);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0023, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0024);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0024, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0025);
                throw new AuthorizationMgmtException(e, exceptionMsg);
            } finally {
                completeTransaction(success, transaction);
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "Attempting to retry getting entitlements for resource."); //$NON-NLS-1$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }

        return buildEntitlementList(realm, entitlementMap, false);
    }

    /**
     * Build the entitlement list and expand any user groups found in entry lists.
     */
    private List buildEntitlementList(AuthorizationRealm realm, Map entitlementMap, boolean isGroup) {
        List entitlementList = new ArrayList();

        // Create a UserEntitlementInfo for each resource in the map
        Iterator resourceItr = entitlementMap.keySet().iterator();
        while (resourceItr.hasNext()) {
            String resourceName = (String) resourceItr.next();

            String groupName = null;
            String elementName = null;
            if (isGroup) {
                groupName = resourceName;
            } else {
                int eleIndex = resourceName.lastIndexOf('.');
                groupName = resourceName.substring(0, eleIndex);
                elementName = resourceName.substring(eleIndex + 1, resourceName.length());
            }

            UserEntitlementInfo entitlementEntry = new UserEntitlementInfo(realm, groupName, elementName);

            // Add grantee entries to each UserEntitlementInfo expanding any groups
            Set entrySet = (Set) entitlementMap.get(resourceName);
            Iterator granteeItr = entrySet.iterator();
            while (granteeItr.hasNext()) {
                GranteeEntitlementEntry aGrantee = (GranteeEntitlementEntry) granteeItr.next();
                // Add the user to this entry
                entitlementEntry.addTriplet(aGrantee);
            }

            // Add to returned list
            entitlementList.add(entitlementEntry);
        }
        return entitlementList;
    }


//==============================================================================
// Authorization Service Methods - Administration
//==============================================================================

    /**
     * Obtain the names of all of the realms known to the system.
     * @return the collection of realm names
     */
    private Collection getRealmNames()
            throws AuthorizationMgmtException {

        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION, new Object[]{"getRealmNames()"}); //$NON-NLS-1$
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        Collection realmList = new ArrayList();
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                realmList = transaction.getRealmNames();
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0023);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0023, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0024);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString( ErrorMessageKeys.SEC_AUTHORIZATION_0024, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0025);
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0025, exceptionMsg);
            } finally {
                completeTransaction(success, transaction);
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "Attempting to retry getting entitlements for resource."); //$NON-NLS-1$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        } else if (realmList == null) {
            // Enforce fact that returned val will not be null.
            if (exception != null) {
                throw new AuthorizationMgmtException(exception, exceptionMsg);
            }
            throw new AuthorizationMgmtException(ErrorMessageKeys.SEC_AUTHORIZATION_0028,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0028));
        }
        return realmList;
    }

    /**
     * Obtain the names of all of the realms known to the system.
     * @param caller the session token of the principal that is attempting to access the policies.
     * @return the set of realm names
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    public Collection getRealmNames(SessionToken caller)
            throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException {
        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION, new Object[]{"getRealmNames(", caller, ")"}); //$NON-NLS-1$ //$NON-NLS-2$
        return getRealmNames();
    }

    /**
     * Return whether there is an existing policy with the specified ID.
     * @param caller the session token of the principal that is attempting to access the policies.
     * @param policyID the ID that is to be checked
     * @return true if a policy with the specified ID exists
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    public boolean containsPolicy(SessionToken caller, AuthorizationPolicyID policyID)
            throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException {
        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,
                new Object[]{"containsPolicy(", caller, ", ", policyID, ")"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        boolean policyContained = false;
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                policyContained = transaction.containsPolicy(policyID);
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0029, policyID);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0029, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0030, policyID);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString( ErrorMessageKeys.SEC_AUTHORIZATION_0030, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0031, policyID);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0031, e, exceptionMsg));
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0031, exceptionMsg);
            } finally {
                completeTransaction(success, transaction);
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "Attempting to retry search for policy ID."); //$NON-NLS-1$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }
        return policyContained;
    }

    /**
     * Locate the IDs of all of the policies that are accessible by the caller.
     * @param caller the session token of the principal that is attempting to access the policies.
     * @return the set of all policy IDs; never null but possibly empty.
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    public Collection findAllPolicyIDs(SessionToken caller)
            throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException {
        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION, new Object[]{"findAllPolicyIDs(", caller, ")"}); //$NON-NLS-1$ //$NON-NLS-2$
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        Set policyIDs = new HashSet();
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                policyIDs.addAll(transaction.findAllPolicyIDs());
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0032);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0032, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0033);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0033, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0034);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0034, e, exceptionMsg));
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0034, exceptionMsg);
            } finally {
                completeTransaction(success, transaction);
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "Attempting to retry search for all policy IDs."); //$NON-NLS-1$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        } 
        return policyIDs;
    }

    /**
     * Locate the IDs of all of the policies that apply to the specified principals
     * and that are accessible by the caller.
     * <p><emph>Unkown whether this method should return policyIds that apply to ALL
     * (set intersection) or ANY (set union) principals in the collection.
     * Currently returns ANY.</emph></p>
     * @param caller the session token of the principal that is attempting to access the policies.
     * @param principals the Set of <code>MetaMatrixPrincipalName</code>s to whom the
     * returned policies should apply (may not null, empty or invalid, all of which
     * would result in an empty result).
     * @return the set of all policy IDs; never null but possibly empty.
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    public Collection findPolicyIDs(SessionToken caller, Collection principals)
            throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException {
        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,
                new Object[]{"findPolicyIDs(", caller, ", ", principals, ")"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Set allPolicyIDs = new HashSet();
        Iterator pItr = principals.iterator();
        MetaMatrixPrincipalName principal = null;
        while (pItr.hasNext()) {
            Collection thisPrincipalsGroups = null;
            try {
                principal = (MetaMatrixPrincipalName) pItr.next();
                thisPrincipalsGroups = this.getGroupsForPrincipal(principal);
                // Return ANY (set union) not ALL (set intersection)
                Iterator itr = this.findPolicyIDs(thisPrincipalsGroups).iterator();
                // HashSet has no addAll()
                while (itr.hasNext()) {
                    allPolicyIDs.add(itr.next());
                }
            } catch (MetaMatrixSecurityException e) {
                String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0035, principal);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0035, e, msg));
                return Collections.EMPTY_SET;
            }
        }
        return allPolicyIDs;
    }

    /**
     * Locate the policies that have the specified IDs.  Any ID that is invalid is simply
     * ignored.
     * @param caller the session token of the principal that is attempting to access the
     * specified policies
     * @param policyIDs the policy IDs for which the policies are to be obtained
     * @return the set of entitlements that correspond to those specified IDs that are valid
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    public Collection getPolicies(SessionToken caller, Collection policyIDs)
            throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException {
        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,
                new Object[]{"getPolicies(", caller, ", ", policyIDs, ")"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        return this.getPolicies(policyIDs);
    }

    /**
     * Locate the policy that has the specified ID.  An ID that is invalid is simply
     * ignored.
     * @param caller the session token of the principal that is attempting to access the
     * specified policies
     * @param policyID the ID of the policy to be obtained
     * @return the policy that correspond to the specified ID; may be null.
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or has expired.
     * @throws AuthorizationException if the caller is unable to perform this operation.
     * @throws AuthorizationMgmtException if there were errors with the SPI.
     */
    public AuthorizationPolicy getPolicy(SessionToken caller, AuthorizationPolicyID policyID)
            throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException {
        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION, new Object[]{"getPolicy(", caller, ", ", policyID, ")"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        AuthorizationPolicy policy = null;
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                policy = transaction.getPolicy(policyID);
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0037, policyID);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString( ErrorMessageKeys.SEC_AUTHORIZATION_0037, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0038, policyID);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0038, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0039, policyID);
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0039, exceptionMsg);
            } finally {
                completeTransaction(success, transaction);
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, new Object[]{"Attempting to retry getting policy for ID (", policyID, ")"}); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }
        return policy;
    }

	private void completeTransaction(boolean success, AuthorizationSourceTransaction transaction) {
		if (transaction != null) {
		    try {
		        if (success) {
		            transaction.commit();
		        } else {
		            transaction.rollback();
		        }
		        transaction.close();
		    } catch (Exception e) {
		    	LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0015,e));
		    }
		}
	}

    /**
     * Verify that caller is in the specified logical role.
     * @param caller The session token of the MetaMatrix principle involking an administrative method.
     * @param roleName The name of the role in question.
     * @return true if caller's session token is valid and he is a MetaMatrix administrator.
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    public boolean isCallerInRole(SessionToken caller, String roleName)
            throws AuthorizationMgmtException {
        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,
                new Object[]{"isCallerInRole(", caller, roleName, ")"}); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            return hasPolicy(caller, RolePermissionFactory.getRealm(), roleName);
        } catch (MembershipServiceException err) {
            throw new AuthorizationMgmtException(err);
        } 
    }

    /**
     * Obtain the names of all of the roles and their descriptions known to the system.
     * @param caller the session token of the principal that is attempting to access the roles.
     * @return a Map of role descriptions key by the role's name.
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    public Map getRoleDescriptions(SessionToken caller)
            throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException {
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        Map roleDescs = null;
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                roleDescs = transaction.getRoleDescriptions();
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0040);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0040, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0041);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0041, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0042);
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0042, exceptionMsg);
            } finally {
                completeTransaction(success, transaction);
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "Attempting to retry getting role descriptions."); //$NON-NLS-1$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }
        return roleDescs;
    }

    /**
     * Returns a collection <code>MetaMatrixPrincipalName</code> objects containing the name
     * of the principal along with its type which belong to the given role.
     * {@link com.metamatrix.platform.security.api.MetaMatrixPrincipalName}
     * @param caller the session token of the principal that is attempting to access the roles.
     * @param roleName String name of MetaMatrix role for which principals
     * are sought
     * @return The collection of <code>MetaMatrixPrincipalName</code>s who are in the given role, possibly enpty, never null.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to see the requested information
     */
    public Collection getPrincipalsForRole(SessionToken caller, String roleName)
            throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException {
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        Collection principals = null;
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                principals = transaction.getPrincipalsForRole(roleName);
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0043, roleName.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString( ErrorMessageKeys.SEC_AUTHORIZATION_0043, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0044, roleName.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0044, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0045, roleName.toString());
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0045, exceptionMsg);
            } finally {
                completeTransaction(success, transaction);
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                    new Object[]{"Attempting to retry getting principals for role \"", roleName.toString(), "\"."}); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }
        return principals;
    }

    /**
     * Returns a Collection of String names of MetaMatrix roles to which the
     * given principal is assigned.
     * @param caller The <code>SessionToken</code> of the principal making the request.
     * @param principal <code>MetaMatrixPrincipalName</code> for which roles are sought
     * @param explicitOnly If true, only return roles assigned directly to given principal.
     * If false, return all roles directly assigned and inherited.
     * @return The collection of role names belonging to the given principal, possibly enpty, never null.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationMgmtException if there is a problem internally with the MembershipService
     * @throws AuthorizationException if admninistrator does not have the authority to see the requested information
     */
    public Collection getRoleNamesForPrincipal(SessionToken caller, MetaMatrixPrincipalName principal)
            throws AuthorizationMgmtException {
        try {
            if (isEntitled(principal.getName())) {
                Map roles = getRoleDescriptions(privlegedToken);
                //wrap in a new hashset to be serializable
                return new HashSet(roles.keySet());
            }
        } catch (MetaMatrixSecurityException e) {
            String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0075);
            throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0075, msg);
        } 
        
        Collection policies;
        try {
            policies = getPoliciesForPrincipal(principal, caller, RolePermissionFactory.getRealm());
        } catch (InvalidPrincipalException err) {
            throw new AuthorizationMgmtException(err);
        }
        
        Collection results = new HashSet();
        
        for (Iterator i = policies.iterator(); i.hasNext();) {
            AuthorizationPolicy policy = (AuthorizationPolicy)i.next();
            
            results.add(policy.getAuthorizationPolicyID().getDisplayName());
        }
        
        return results;
    }

    protected boolean isEntitled(String principal) {
        try {
            if (membershipServiceProxy.isSuperUser(principal) || !membershipServiceProxy.isSecurityEnabled()) {
                LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                                     new Object[]{ "Automatically entitling principal", principal}); //$NON-NLS-1$ 
                return true;
            }
            return false;
        }  catch (MembershipServiceException e) {
            String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0075);
            throw new ServiceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0075, msg);
        } 
    }

    /**
     * Remove given Principal from <emph>ALL</emph> <code>AuthorizationPolicies</code> to
     * which he belongs.
     * @param caller the session token of the principal that is attempting to remove the Principal.
     * @param principal <code>MetaMatrixPrincipalName</code> which should be deleted.
     * @return true if at least one policy in which the principal had authorization
     * was found and deleted, false otherwise.
     * @throws AuthorizationException if admninistrator does not have the authority to preform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     * @throws ServiceStateException if the Authorization service is closed to client requests.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    public boolean removePrincipalFromAllPolicies(SessionToken caller, MetaMatrixPrincipalName principal)
            throws AuthorizationException, AuthorizationMgmtException {
        boolean success = false;
        String exceptionMsg = null;

        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getWriteTransaction();
                success = transaction.removePrincipalFromAllPolicies(principal);
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0050, principal.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString( ErrorMessageKeys.SEC_AUTHORIZATION_0050, e, exceptionMsg));
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0051, principal.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0051, e, exceptionMsg));
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0052, principal.toString());
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0052, exceptionMsg);
            } finally {
                if (success) {
                	if (transaction != null) {
	                    try {
	                        transaction.commit();           // commit the transaction
	                        transaction.close();
	                    } catch (Exception e) {
	                        String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0053, principal.toString());
	                        LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0053, e, msg));
	                        throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0053, msg);
	                    }
                	}
                    // Clear cache so that Auth serv reflects latest state next time around
                    Collection removedPrincPolicyIDs = this.authorizationCache.findPolicyIDs(principal, caller);
                    this.authorizationCache.removePolicysFromCache(removedPrincPolicyIDs);
                    this.authorizationCache.removePrincipalFromCache(principal);
                } else {    // Unsuccessful
                    if (transaction != null) {
                        try {
                            transaction.rollback();         // rollback the transaction
                            transaction.close();
                        } catch (Exception e) {
                            String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0054, principal.toString());
                            LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0054, e, msg));
                            throw new AuthorizationMgmtException(e, msg);
                        }
                    }
                }
                transaction = null;                 // ensure the transaction is not retained
            }
            // Try again...
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                    new Object[]{ "Attempting to retry removing principal \"", principal.toString(), "\" from ALL policies."}); //$NON-NLS-1$ //$NON-NLS-2$
        }

        return success;
    }

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermissionsImpl</code> in the given <code>AuthorizationRealm</code>.<br>
     * <strong>NOTE:</strong> It is the responsibility of the caller to determine
     * which of the <code>AuthorizationPolicy</code>'s <code>AuthorizationPermissionsImpl</code>
     * are actually in the given <code>AuthorizationRealm</code>.  The <code>AuthorizationPolicy</code>
     * may span <code>AuthorizationRealm</code>s.
     * @param caller The session token of the principal that is attempting to retrieve the policies.
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * in the given realm - possibly empty but never null.
     * @throws AuthorizationException if admninistrator does not have the authority to preform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    public Collection getPolicyIDsWithPermissionsInRealm(SessionToken caller, AuthorizationRealm realm)
            throws AuthorizationException, AuthorizationMgmtException {
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        Collection policyIDs = null;
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                policyIDs = transaction.getPolicyIDsWithPermissionsInRealm(realm);
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0055, realm.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0055, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0056, realm.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0056, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0057, realm.toString());
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0057, exceptionMsg);
            } finally {
                completeTransaction(success, transaction);
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                    new Object[]{"Attempting to retry getting Authorization PolicyIDs with permissions belonging to realm \"", realm.toString(), "\"."}); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }
        return policyIDs;
    }
    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * in the given <code>AuthorizationRealm</code>.
     * <br>This method will only work for Data Access Authorizations because the realm
     * is encoded in a Data Access policy name.
     * <strong>NOTE:</strong> It is the responsibility of the caller to determine
     * which of the <code>AuthorizationPolicy</code>'s <code>AuthorizationPermissionsImpl</code>
     * are actually in the given <code>AuthorizationRealm</code>.  The <code>AuthorizationPolicy</code>
     * may span <code>AuthorizationRealm</code>s.
     * @param caller The session token of the principal that is attempting to retrieve the policies.
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * in the given realm - possibly empty but never null.
     * @throws AuthorizationException if admninistrator does not have the authority to preform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    public Collection getPolicyIDsInRealm(SessionToken caller, AuthorizationRealm realm)
            throws AuthorizationException, AuthorizationMgmtException {
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        Collection policyIDs = null;
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                policyIDs = transaction.getPolicyIDsInRealm(realm);
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0058, realm.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0058, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0059, realm.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0059, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0060, realm.toString());
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0060, exceptionMsg);
            } finally {
            	completeTransaction(success, transaction);
                transaction = null; // ensure the transaction is not retained
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                    new Object[]{"Attempting to retry getting Authorization Policies belonging to realm \"", realm.toString(), "\"."}); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }
        return policyIDs;
    }

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicy</code>s
     * that have <code>AuthorizationPermissionsImpl</code> in the given <code>AuthorizationRealm</code>.<br>
     * <strong>NOTE:</strong> It is the responsibility of the caller to determine
     * which of the <code>AuthorizationPolicy</code>'s <code>AuthorizationPermissionsImpl</code>
     * are actually in the given <code>AuthorizationRealm</code>.  The <code>AuthorizationPolicy</code>
     * may span <code>AuthorizationRealm</code>s.
     * @param caller The session token of the principal that is attempting to retrieve the policies.
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicy</code>s that have permissions
     * in the given realm - possibly empty but never null.
     * @throws AuthorizationException if admninistrator does not have the authority to preform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    public Collection getPoliciesInRealm(SessionToken caller, AuthorizationRealm realm)
            throws AuthorizationException, AuthorizationMgmtException {
        Collection policyIDs = this.getPolicyIDsInRealm(caller, realm);
        return this.getPolicies(policyIDs);
    }

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermissionsImpl</code> that exist in the given
     * <emph>partial</emph> <code>AuthorizationRealm</code>.<br>
     * The implementation is such that all <code>AuthorizationPolicyID</code>s
     * whose <code>AuthorizationRealm</code> <emph>starts with</emph> the given
     * <code>AuthorizationRealm</code> are returned.
     * @param caller The session token of the principal that is attempting to retrieve the policies.
     * @param realm The <emph>partial</emph> realm in which to search for
     * <code>AuthorizationPermission</code>s whose realm name <emph>starts with</emph>
     * the given realm.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * in the given partial realm - possibly empty but never null.
     * @throws AuthorizationException if admninistrator does not have the authority to preform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    public Collection getPolicyIDsInPartialRealm(SessionToken caller, AuthorizationRealm realm)
            throws AuthorizationException, AuthorizationMgmtException {
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        Collection policyIDs = null;
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                policyIDs = transaction.getPolicyIDsInPartialRealm(realm);
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0058, realm.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0058, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0059, realm.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0059, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0060, realm.toString());
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0060, exceptionMsg);
            } finally {
            	completeTransaction(success, transaction);
                transaction = null; // ensure the transaction is not retained
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                    new Object[]{"Attempting to retry getting Authorization Policies belonging to realm \"", realm.toString(), "\"."}); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }
        return policyIDs;
    }

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermissionsImpl</code> on the given resource that
     * exists in the given <code>AuthorizationRealm</code>.<br>
     * @param caller The session token of the principal that is attempting to retrieve the policies.
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @param resourceName The resource for which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * on the given resource - possibly empty but never null.
     * @throws AuthorizationException if admninistrator does not have the authority to preform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    public Collection getPolicIDsForResourceInRealm(SessionToken caller, AuthorizationRealm realm, String resourceName)
            throws AuthorizationException, AuthorizationMgmtException {
        return getPolicIDsForResourceInRealm(realm, resourceName);
    }

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermissionsImpl</code> on the given resource that
     * exists in the given <code>AuthorizationRealm</code>.<br>
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @param resourceName The resource for which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * on the given resource - possibly empty but never null.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    private Collection getPolicIDsForResourceInRealm(AuthorizationRealm realm, String resourceName)
            throws AuthorizationMgmtException {
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        Collection policyIDs = null;
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                policyIDs = transaction.getPolicyIDsForResourceInRealm(realm, resourceName);
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0058, realm.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0058, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0059, realm.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0059, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0060, realm.toString());
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0060, exceptionMsg);
            } finally {
            	completeTransaction(success, transaction);
                transaction = null; // ensure the transaction is not retained
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                    new Object[]{"Attempting to retry getting Authorization Policies belonging to realm \"", realm.toString(), "\"."}); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }
        return policyIDs;
    }

    /**
     * Takes a tree of <code>PermissionDataNodeImpl</code>s that have their <code>Resource</code>s
     * filled in and fills in all permissions on resources that are found in the given
     * <code>AuthorizationPolicyID</code>.<br></br>
     * If any permissions are found that have no corresponding data node, a <code>AuthorizationMgmtException</code>
     * is thrown noting the missing resource name(s).
     * @param root The node containing the resource (group or element full name)
     * for which to search for permission(s).
     * @param root The root of the tree of PermissionDataNodes to fill in permissions for.
     * @return The root of the filled in tree.
     * If no permissions exist, the original is returned as the sole element in the list.
     * @throws AuthorizationMgmtException if there is a connection or communication error with the data source,
     * signifying that the method should be retried with a different connection; if there is an
     * unspecified or unknown error with the data source; or one or more permissions were found but
     * a corresponding <code>PermissionDataNodeImpl</code> could not be found.
     */
    public PermissionDataNode fillPermissionNodeTree(PermissionDataNode root, AuthorizationPolicyID policyID)
            throws AuthorizationMgmtException {
        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,
                new Object[]{"fillPermissionNodeTree(", root, policyID, ")"}); //$NON-NLS-1$ //$NON-NLS-2$
        Set permissions = this.getPermissionsForPolicy(policyID);
        Collection exceptions = Collections.EMPTY_LIST;
        PermissionDataNodeTreeView treeView = new PermissionDataNodeTreeViewImpl(root);
        if (permissions.size() > 0) {
            exceptions = treeView.setPermissions(permissions);
        }

        // If exceptions exist, display them
        if (exceptions.size() > 0) {
            StringBuffer msg = new StringBuffer();
            PermissionNodeException ex = null;
            Iterator exItr = exceptions.iterator();
            while (exItr.hasNext()) {
                ex = (PermissionNodeException) exItr.next();
                msg.append(ex.getClass().getSimpleName() + " "); //$NON-NLS-1$
                msg.append(ex.getMessage());
                msg.append(", "); //$NON-NLS-1$
            }
            // Trunc last 2 chars (", ")
            msg.setLength(msg.length() - 2);
            throw new AuthorizationMgmtException(msg.toString());
        }
        return root;
    }

    /**
     * Find and create all <code>AuthorizationPermissionsImpl</code> known to a policy.
     * @param policyID The policy indentifier.
     * @return The set of all permissions that belong to the given policy.
     */
    private Set getPermissionsForPolicy(AuthorizationPolicyID policyID)
            throws AuthorizationMgmtException {
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        Set permissions = Collections.EMPTY_SET;
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                permissions = transaction.getPermissionsForPolicy(policyID);
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0061, policyID.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0061, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0062, policyID.toString());
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0062, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0063, policyID.toString());
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0063, exceptionMsg);
            } finally {
            	completeTransaction(success, transaction);
                transaction = null; // ensure the transaction is not retained
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, new Object[]{"Attempting to retry getting permissions for policy \"", policyID.toString(), "\"."}); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }
        return permissions;
    }


    /**
     * Execute as a single transaction with the specified actions, and return
     * the set of IDs for the objects that were affected/modified by the action.
     * @param administrator the session token of the principal that is attempting to access the policies.
     * @param actions the ordered list of actions that are to be performed
     * on metamodel within the repository.
     * @return The set of objects that were affected by this transaction.
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or has expired.
     * @throws AuthorizationException if the administrator is unable to perform this operation.
     * @throws AuthorizationMgmtException if there were errors with the SPI.  Causes rollback.
     * @throws IllegalArgumentException if the action is null.
     */
    public Set executeTransaction(SessionToken administrator, List actions)
            throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException {
        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION, new Object[]{"executeTransaction(", administrator, actions, ")"}); //$NON-NLS-1$ //$NON-NLS-2$
        if (administrator == null) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0064));
        }

        if (actions == null) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0065));
        }
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                new Object[]{"Executing transaction with ", new Integer(actions.size()), " action(s)"}); //$NON-NLS-1$ //$NON-NLS-2$
        Set result = new HashSet();
        if (actions.isEmpty()) {
            return result;
        }
        // Audit - access modify
        AuditMessage logMsg = new AuditMessage(SecurityAuditContexts.CTX_AUTHORIZATION, "executeTransaction-modify", administrator.getUsername(),  new Object[]{this.printActions(actions)}); //$NON-NLS-1$
        LogManager.log(MessageLevel.INFO, LogContextsUtil.CommonConstants.CTX_AUDITLOGGING, logMsg);
        
        List actionsWithSameTarget = new ArrayList(7);   // guessing at an initial size, probably high
        Object currentTarget = null;
        ActionDefinition currentAction = null;
        ActionDefinition nextAction = null;
        AuthorizationSourceTransaction transaction = null;
        boolean successfulTxn = false;

        int actionCounter = -1;

        // Iterate through the actions, and apply all as a single transaction
        try {
            transaction = this.getWriteTransaction();
            boolean createObject = false;

            // Get the first action and its target, and add it to the list ...
            Iterator iter = actions.iterator();
            if (iter.hasNext()) {
                currentAction = (ActionDefinition) iter.next();
                currentTarget = currentAction.getTarget();
                actionsWithSameTarget.add(currentAction);
                LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                        new Object[]{"Target: <", currentTarget, "> First action: <", currentAction, ">"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }

            while (iter.hasNext()) {
                nextAction = (ActionDefinition) iter.next();
                if (currentAction instanceof CreateObject) {
                    createObject = true;
                }
                // If the current action is a 'DestroyObject' action, then process only
                // the destroy (other actions not processed up to this point do not
                // need to be processed, since the target will be destroyed anyway).
                if (currentAction instanceof DestroyObject) {
                    // If creating and destroying an object in the same action list,
                    // then don't need to do anything ...
                    if (!createObject) {
                        this.executeTransactionsOnTarget(transaction, actionsWithSameTarget, currentTarget, administrator, result);
                    }
                    actionCounter += actionsWithSameTarget.size();
                    actionsWithSameTarget.clear();
                    createObject = false;
                    currentTarget = nextAction.getTarget();
                }

                // Otherwise, if the next action has another target, process up to the current action ...
                else if (currentTarget != nextAction.getTarget()) {
                    this.executeTransactionsOnTarget(transaction, actionsWithSameTarget, currentTarget, administrator, result);
                    actionCounter += actionsWithSameTarget.size();
                    actionsWithSameTarget.clear();
                    createObject = false;
                    currentTarget = nextAction.getTarget();
                }

                // Add this next action ...
                currentAction = nextAction;
                actionsWithSameTarget.add(currentAction);
                LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, new Object[]{"Target: ", currentTarget, " action: ", currentAction}); //$NON-NLS-1$ //$NON-NLS-2$
            }
            // Process the last set of actions ...
            if (actionsWithSameTarget.size() != 0) {
                this.executeTransactionsOnTarget(transaction, actionsWithSameTarget, currentTarget, administrator, result);
                createObject = false;
            }

            // As we've just made a change, clear caches
            // so that they'll reflect latest state next time around
            this.authorizationCache.clearCaches();

            successfulTxn = true;

        } catch (AuthorizationMgmtException e) {
            String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0066,printActions(actions));
            LogManager.logError(LogConstants.CTX_AUTHORIZATION, e, msg);
            throw e;
        } catch (Exception e) {
            String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0066,printActions(actions));
            LogManager.logError(LogConstants.CTX_AUTHORIZATION, e, msg);
            throw new AuthorizationMgmtException(e);
        } finally {
            if (transaction != null) {
                if ( successfulTxn ) {
                    // Commit the transaction
                    try {
                        transaction.commit();
                    } catch (ManagedConnectionException e) {
                        Object params = new Object[] {printActions(actions)};
                        String msg = PlatformPlugin.Util.getString("AuthorizationServiceImpl.Error_committing_transaction_after_executing_actions__{0}", params); //$NON-NLS-1$
                        LogManager.logError(LogConstants.CTX_AUTHORIZATION, e, msg);
                    }
                } else {
                    // rollback the transaction
                    try {
                        transaction.rollback();
                    } catch (ManagedConnectionException e) {
                        String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0067,printActions(actions));
                        LogManager.logError(LogConstants.CTX_AUTHORIZATION, e, msg);
                    }
                }

                try {
                    transaction.close();
                } catch (Exception e) {
                    String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0015,printActions(actions));
                    LogManager.logError(LogConstants.CTX_AUTHORIZATION, e, msg);
                }
                transaction = null;                     // ensure the transaction is not retained
            }
        }
        return result;
    }

    /**
     * Locate the IDs of all of the policies that apply to <emph>any</emph> of the
     * specified principals.<p>
     * <emph>NOTE</emph> This method is used to find the policyIDs that are accessible
     * to a user and any groups he belongs to.  The <code>principals</code> collection
     * should contain <emph>only a user (or group) and the groups in which he's a member</emph>.
     * @param principals the Set of <code>MetaMatrixPrincipalName</code>s to whom the
     * returned policies should apply (may not null, empty or invalid, any of which
     * may result in an empty result or simply ignored).
     * @return the set of all policy IDs; never null but possibly empty.
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    private Collection findPolicyIDs(Collection principals, AuthorizationRealm realm)
            throws AuthorizationMgmtException {
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        Set policyIDs = new HashSet();
        if (principals == null || principals.size() == 0) {
            return policyIDs;
        }

        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                policyIDs.addAll(transaction.findPolicyIDs(principals, realm));
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0068);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0068, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0069);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0069, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0070);
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0070, exceptionMsg);
            } catch (Exception e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0071);
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0071, exceptionMsg);
            } finally {
            	completeTransaction(success, transaction);
                transaction = null; // ensure the transaction is not retained
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "Attempting to retry search for policy IDs belonging to principal collection."); //$NON-NLS-1$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }

        return policyIDs;
    }

    /**
     * Locate the IDs of all of the policies that apply to <emph>any</emph> of the
     * specified principals.<p>
     * <emph>NOTE</emph> This method is used to find the policyIDs that are accessible
     * to a user and any groups he belongs to.  The <code>principals</code> collection
     * should contain <emph>only a user (or group) and the groups in which he's a member</emph>.
     * @param principals the Set of <code>MetaMatrixPrincipalName</code>s to whom the
     * returned policies should apply (may not null, empty or invalid, any of which
     * may result in an empty result or simply ignored).
     * @return the set of all policy IDs; never null but possibly empty.
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    private Collection findPolicyIDs(Collection principals)
            throws AuthorizationMgmtException {
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        Set policyIDs = new HashSet();
        if (principals == null || principals.size() == 0) {
            return policyIDs;
        }

        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                policyIDs.addAll(transaction.findPolicyIDs(principals));
                // Can't make judgement based on return val.
                // If were here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0068);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0068, e, exceptionMsg));
                exception = e;
                success = false;
            } catch ( AuthorizationSourceConnectionException e ) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0069);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0069, e, exceptionMsg));
                exception = e;
                success = false;
            } catch ( AuthorizationSourceException e ) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0070);
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0070, exceptionMsg);
            } catch ( Exception e ) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0071);
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0071, exceptionMsg);
            } finally {
            	completeTransaction(success, transaction);
                transaction = null; // ensure the transaction is not retained
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "Attempting to retry search for policy IDs belonging to principal collection."); //$NON-NLS-1$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }

        return policyIDs;
    }

    /**
     * Locate the policies that have the specified IDs.  Any ID that is invalid is simply
     * ignored.
     * @param policyIDs the policy IDs for which the policies are to be obtained
     * @return the set of entitlements that correspond to those specified IDs that are valid
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    private Collection getPolicies(Collection policyIDs)
            throws AuthorizationMgmtException {
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;
        Collection synchronizedPolicyIDs = Collections.synchronizedCollection(policyIDs);

        Set policies = new HashSet();
        AuthorizationSourceTransaction transaction = null;
        for (int i = 0; i < this.retries; i++) {
            try {
                transaction = getReadTransaction();
                policies.addAll(transaction.getPolicies(synchronizedPolicyIDs));
                // Can't make judgement based on return val.
                // If we're here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0072);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString( ErrorMessageKeys.SEC_AUTHORIZATION_0072, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0073);
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0073, e, exceptionMsg));
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0074);
                throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0074, exceptionMsg);
            } finally {
            	completeTransaction(success, transaction);
                transaction = null; // ensure the transaction is not retained
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "Attempting to retry search for policies with ID collection."); //$NON-NLS-1$
        }
        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }
        return policies;
    }
    
    public boolean hasPolicy(SessionToken caller, AuthorizationRealm realm, String policyName) 
        throws AuthorizationMgmtException, MembershipServiceException {
        
        if (isEntitled(caller.getUsername())) {
            return true;
        }
        
        Collection policies;
		try {
			policies = getPoliciesForPrincipal(new MetaMatrixPrincipalName(caller.getUsername(), MetaMatrixPrincipal.TYPE_USER), caller, realm);
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
        
        for (Iterator i = policies.iterator(); i.hasNext();) {
            AuthorizationPolicy policy = (AuthorizationPolicy)i.next();
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
     * @throws InvalidPrincipalException if the principal is invalid.
     */
    private Collection getPoliciesForPrincipal(MetaMatrixPrincipalName user, SessionToken session, AuthorizationRealm realm)
            throws AuthorizationMgmtException, InvalidPrincipalException {
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                new Object[]{"getPoliciesForPrincipal(", user, ", ", realm, ") - Trying cache first."}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        // Look for policYIDs... try the cache first.
        Collection applicablePolicyIDs = this.authorizationCache.findPolicyIDs(user, session);

        if ( applicablePolicyIDs != null && applicablePolicyIDs.size() > 0 ) {
            // If there are cached policies for this user, check if any are for given realm
            // if not, we have to check the store, so null out the applicablePolicyIDs ref.
            if ( ! hasPolicyIDsForRealm(applicablePolicyIDs, realm) ) {
                LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                        new Object[]{"getPoliciesForPrincipal(", user, ", ", realm, //$NON-NLS-1$ //$NON-NLS-2$
                                     ") - Principal has no policyIDs cached for the given realm."}); //$NON-NLS-1$
                applicablePolicyIDs.clear();
            } else {
                LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                        new Object[]{"getPoliciesForPrincipal(", user, ", ", realm, //$NON-NLS-1$ //$NON-NLS-2$
                                     ") - Found poliyIDs cached for the principal in the given realm."}); //$NON-NLS-1$
            }

        }
        if ( applicablePolicyIDs == null || applicablePolicyIDs.size() == 0 ) {
            // If the cache result is null, then this principal's applicable policy IDs have not been cached ...
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                    new Object[]{"getPoliciesForPrincipal(", user, ", ", realm, //$NON-NLS-1$ //$NON-NLS-2$
                                 ") - No policyIDs found in cache, going to store."}); //$NON-NLS-1$
            // Go to the store
            Collection principals = this.getGroupsForPrincipal(user);
            applicablePolicyIDs = this.findPolicyIDs(principals, realm);

            // If there still aren't any applicable policyIDs, then principal is not authorized (to do anything)
            if ( applicablePolicyIDs == null || applicablePolicyIDs.size() == 0 ) {
                LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                        new Object[]{"getPoliciesForPrincipal(", user, ", ", realm, //$NON-NLS-1$ //$NON-NLS-2$
                                     ") - No policyIDs found for realm - no authorization."}); //$NON-NLS-1$
                // Return empty set signifying principal has NO permissions
                return Collections.EMPTY_SET;
            }

            // Found some policyIDs for principal in store...
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                    new Object[]{"getPoliciesForPrincipal(", user, ", ", realm, //$NON-NLS-1$ //$NON-NLS-2$
                                 ") - Found policyIDs in store - caching: ", applicablePolicyIDs}); //$NON-NLS-1$
            // Cache this principal's policyIDs
            this.authorizationCache.cachePolicyIDsForPrincipal(user, session, applicablePolicyIDs);
        }

        Collection policies = new HashSet();
        // Look for policies by policyID... Try the cache first
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                new Object[]{"getPoliciesForPrincipal(", user, ", ", realm, //$NON-NLS-1$ //$NON-NLS-2$
                             ") - Looking up policies in cache by policyID."}); //$NON-NLS-1$
        policies = this.authorizationCache.findPolicies(applicablePolicyIDs);

        // If the number of policies a user has is less than the number of policyIDs, we're missing some
        // of his policies.
        if ( policies == null || policies.size() == 0 || policies.size() < applicablePolicyIDs.size() ) {
            // Policies were not cached against PolicyIDs... Go to store
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                    new Object[]{"getPoliciesForPrincipal(", user, ", ", realm, //$NON-NLS-1$ //$NON-NLS-2$
                                 ") - No policies were found in cache, going to store."}); //$NON-NLS-1$
            policies = this.getPolicies(applicablePolicyIDs);
            if ( policies != null && policies.size() > 0 ) {
                LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                        new Object[]{"getPoliciesForPrincipal(", user, ", ", realm, //$NON-NLS-1$ //$NON-NLS-2$
                                     ") - Found policies in store - caching."}); //$NON-NLS-1$
                // Cache retrieved policies, if any
                this.authorizationCache.cachePoliciesWithIDs(policies);
            }
        } else {
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                    new Object[]{"getPoliciesForPrincipal(", user, ", ", realm, //$NON-NLS-1$ //$NON-NLS-2$
                                 ") - Found policies <", policies, "> in cache."}); //$NON-NLS-1$ //$NON-NLS-2$
        }

        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                new Object[]{"getPoliciesForPrincipal(", user, ", ", realm, //$NON-NLS-1$ //$NON-NLS-2$
                             ") - Returning these Policies for principal: <", policies, ">"}); //$NON-NLS-1$ //$NON-NLS-2$
        return policies;
    }

    /**
     * Determine if one of the <code>AuthorizationPolicyID</code>s in the given
     * collection belong in the given <code>AuthorizationRealm</code>.
     * @param policyIDs the collection of policy IDs.
     * @param theRealmOfInterest the theRealmOfInterest to check for.
     * @return <code>true</code> iff at least one of the policy IDs are from
     * the given theRealmOfInterest.
     */
    private boolean hasPolicyIDsForRealm(Collection policyIDs, AuthorizationRealm theRealmOfInterest) {
        Iterator ID_itr = policyIDs.iterator();
        while ( ID_itr.hasNext() ) {
            AuthorizationPolicyID id = (AuthorizationPolicyID) ID_itr.next();
            AuthorizationRealm aRealm = id.getRealm();
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                    new Object[] {"hasPolicyIDsForRealm() - Comparing realms: <", aRealm, "> <", theRealmOfInterest, ">"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            if ( aRealm.equals(theRealmOfInterest) ) {
                LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                        "hasPolicyIDsForRealm() - Realms are equal."); //$NON-NLS-1$
                return true;
            }
        }
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                "hasPolicyIDsForRealm() - No realms found to be equal."); //$NON-NLS-1$
        return false;
    }

    /**
     * Return all the groups that this prinicpal is a member of <i>and</i> the
     * given principal (implies this principal is a member of itself).
     * @param principal the principal for which to look for groups (may itself be a group).
     * @return the given principal and all groups of which it is a member.
     * @throws AuthorizationMgmtException if an error occurs while contacting the Membership svc.
     * @throws InvalidPrincipalException if the given principal is invalid.
     */
    private Collection getGroupsForPrincipal(MetaMatrixPrincipalName principal)
            throws AuthorizationMgmtException, InvalidPrincipalException {
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                new Object[] {"getGroupsForPrincipal(", principal, ") - Getting all group memberships."}); //$NON-NLS-1$ //$NON-NLS-2$
        // Get the set of all groups this Principal is a member of
        Set allPrincipals = new HashSet();
        try {
            Collection groups = Collections.EMPTY_SET;
            if (principal.getType() == MetaMatrixPrincipal.TYPE_USER ||
                    principal.getType() == MetaMatrixPrincipal.TYPE_ADMIN) {
                groups = membershipServiceProxy.getGroupsForUser(principal.getName());
            } else if (principal.getType() == MetaMatrixPrincipal.TYPE_GROUP) {
            	MetaMatrixPrincipal groupPrincipal = membershipServiceProxy.getPrincipal(principal);
            	groups = new HashSet();
            	groups.add(groupPrincipal.getName());
            }
            Iterator memberItr = groups.iterator();
            // Add all principals that each orig is member of
            while (memberItr.hasNext()) {
                // HACK: Convert ALL member principals to MetaMatrixPrincipalName objs
                // since Auth and Memb svcs don't speak the same language.
                MetaMatrixPrincipalName member = new MetaMatrixPrincipalName((String) memberItr.next(), MetaMatrixPrincipal.TYPE_GROUP);
                LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                        new Object[]{"getGroupsForPrincipal(", principal, ") - Adding membership <", member, ">"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                allPrincipals.add(member);
            }
            // Add original Principal, now that we know he's been authenticated.
            allPrincipals.add(principal);
        } catch (InvalidPrincipalException e) {
        	throw e;
        } catch (ServiceException e) {
            String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0075);
            throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0075, msg);
        } catch (MetaMatrixSecurityException e) {
            String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0035);
            throw new AuthorizationMgmtException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0035, msg);
        }
        return allPrincipals;
    }

    /**
     * For the given Metabase request, find any and all permissions in MetabaseRealm dependant
     * on the given request.
     *
     * @param request the permission for which to find dependants.
     * @return All permissions dependant on the given permission. All have actions cloned from
     *         the given request.
     */
    private Collection getDependantRequests(AuthorizationPermission request) throws AuthorizationMgmtException {
        boolean success = false;
        Exception exception = null;
        String exceptionMsg = null;

        Set policies = new HashSet();
        AuthorizationSourceTransaction transaction = null;
        for ( int i = 0; i < this.retries; i++ ) {
            try {
                transaction = getReadTransaction();
                policies.addAll(transaction.getDependantPermissions(request));
                // Can't make judgement based on return val.
                // If we're here, call was successful.
                success = true;
                break;
            } catch (ManagedConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(
                        "AuthorizationServiceImpl.Exception_while_getting_dependant_permissions_for_request_{0}", request); //$NON-NLS-1$
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, e, exceptionMsg);
                exception = e;
                success = false;
            } catch (AuthorizationSourceConnectionException e) {
                exceptionMsg = PlatformPlugin.Util.getString(
                        "AuthorizationServiceImpl.Failure_communicating_with_authorization_source_while_getting_dependant_permissions_for_request_{0}", request); //$NON-NLS-1$
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, e, exceptionMsg);
                exception = e;
                success = false;
            } catch (AuthorizationSourceException e) {
                exceptionMsg = PlatformPlugin.Util.getString(
                        "AuthorizationServiceImpl.Unknown_exception_communicating_with_authorization_source_while_getting_dependant_permissions_for_request_{0}", request); //$NON-NLS-1$
                LogManager.logError(LogConstants.CTX_AUTHORIZATION, e, exceptionMsg);
                throw new AuthorizationMgmtException(e, exceptionMsg);
            } finally {
                if ( transaction != null ) {
                    try {
                        if ( success ) {
                            transaction.commit();
                        } else {
                            transaction.rollback();
                        }
                        transaction.close();
                    } catch (Exception e) {
                    	LogManager.logError(LogConstants.CTX_AUTHORIZATION, e, PlatformPlugin.Util.getString("AuthorizationServiceImpl.Unable_to_close_transaction.")); //$NON-NLS-1$
                    }
                    transaction = null;                     // ensure the transaction is not retained
                }
            }
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                                 "Attempting to retry search for policies with ID collection."); //$NON-NLS-1$
        }
        if ( !success ) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, exceptionMsg);
        }
        return policies;
    }

    /**
     * Executes the specified actions, which must all be applied to the same target, using
     * the specified transaction.  Clears the policy and principal caches so that any modification
     * is seen immediatly.
     *
     * @param transaction
     * @param actions
     * @param target
     * @param administrator
     * @param affectedObjects
     * @throws AuthorizationMgmtException
     */
    private void executeTransactionsOnTarget(AuthorizationSourceTransaction transaction, List actions,
                                             Object target, SessionToken administrator, Set affectedObjects)
            throws AuthorizationMgmtException {
        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,
                new Object[]{"executeTransactionsOnTarget(", administrator, actions, target, ")"}); //$NON-NLS-1$ //$NON-NLS-2$
        // Execute the action(s) ...
        Set results = null;
        Exception exception = null;
        String exceptionMsg = null;
        boolean success = false;

        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,
                new Object[]{"Executing ", new Integer(actions.size()), " action(s) on target \"", target, "\""}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        if (target instanceof AuthorizationPolicyID) {
            AuthorizationPolicyID policyID = (AuthorizationPolicyID) target;

//          *********************
//     vah 2/8/04 - cannot use retries at this level with
//          database transactions, because in order to retry
//          properly, a rollback of what was executed needs to be
//          undone, but this can't be done because this would
//          also rollback other targets that we performed
//          prior to this target.
//    *********************

//            for (int i = 0; i < this.retries; i++) {
                try {
                    results = transaction.executeActions(policyID, actions, administrator.getUsername());
                    // Can't make judgement based on return val.
                    // If were here, call was successful.
                    success = true;
//                    break;
                } catch (AuthorizationSourceConnectionException e) {
                    exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0076, target);
                    LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0076, e, exceptionMsg));
                    exception = e;
                    success = false;
                } catch (AuthorizationSourceException e) {
                    exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0076, target);
                    LogManager.logError(LogConstants.CTX_AUTHORIZATION, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0076, e, exceptionMsg));
                    exception = e;
                    success = false;
                }
        } else {
            exceptionMsg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0077, target);
            throw new AuthorizationMgmtException(ErrorMessageKeys.SEC_AUTHORIZATION_0077, exceptionMsg);
        }

        if (!success) {
            // Let caller know that none of the calls were successful.
            throw new AuthorizationMgmtException(exception, ErrorMessageKeys.SEC_AUTHORIZATION_0076, exceptionMsg);
        }
        affectedObjects.addAll(results);
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
                    throw new AuthorizationMgmtException(ErrorMessageKeys.SEC_AUTHORIZATION_0078,
                            PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0078));
                }
            } else {
                theRealm = aRealm;
            }
        }
        if ( theRealm == null ) {
            throw new AuthorizationMgmtException(PlatformPlugin.Util.getString("AuthorizationServiceImpl.Authorization_Realm_is_null")); //$NON-NLS-1$
        }
        return theRealm;
    }


    /**
     * Stringify the list of actions.
     */
    private String printActions(Collection actions) {
        StringBuffer sb = new StringBuffer();
        Iterator iter = actions.iterator();
        if (iter.hasNext()) {
            sb.append(iter.next().toString());
        }
        while (iter.hasNext()) {
            sb.append("; "); //$NON-NLS-1$
            sb.append(iter.next().toString());
        }
        return sb.toString();
    }

    /**
     * Outputs a String representation of this service - the class
     * name followed by either the instance name or some indication
     * of the state the service is in.
     */
    public String toString() {
        StringBuffer buff = new StringBuffer("AuthorizationService - "); //$NON-NLS-1$
        try {
            buff.append(super.getInstanceName());
        } catch (ServiceNotInitializedException e) {
            buff.append("not initialized."); //$NON-NLS-1$
        } catch (ServiceClosedException e) {
            buff.append("closed."); //$NON-NLS-1$
        } catch (ServiceStateException e) {
            buff.append("in invalid state."); //$NON-NLS-1$
        }
        buff.append("\n"); //$NON-NLS-1$
        return buff.toString();
    }

    public void migratePolicies(SessionToken token,
                                EntitlementMigrationReport rpt,
                                String targetVDBName,
                                String targetVDBVersion,
                                Set targetNodes,
                                Collection sourcePolicies,
                                AdminOptions options) 
        throws MetaMatrixComponentException,InvalidSessionException,AuthorizationException,AuthorizationMgmtException {

        AuthorizationRealm targetRealm = new AuthorizationRealm(targetVDBName, targetVDBVersion);

        // Get AuthorizationObjectEditor
        AuthorizationObjectEditor aoe = new AuthorizationObjectEditor();

        // Iterate over all Policies from source
        Iterator policyItr = sourcePolicies.iterator();
        int newPermissions = 0;
        while (policyItr.hasNext()) {
            AuthorizationPolicy sourcePolicy = (AuthorizationPolicy)policyItr.next();

            // Create new Policy
            AuthorizationPolicyID sourcePolicyID = sourcePolicy.getAuthorizationPolicyID();
            AuthorizationPolicyID newPolicyID = new AuthorizationPolicyID(sourcePolicyID.getDisplayName(), targetVDBName,
                                                                          targetVDBVersion);

            AuthorizationPolicy existingPolicy = null;

            try {
                existingPolicy = getPolicy(token, newPolicyID);
            } catch (AuthorizationMgmtException e) {
                // if policy does not exist this throws a exception
                existingPolicy = null;
            }

            boolean overWritten = false;

            if (existingPolicy != null) {
                if (options.containsOption(AdminOptions.OnConflict.EXCEPTION)) {
                    throw new AuthorizationException(PlatformPlugin.Util.getString("AuthorizationServiceImpl.role_exists", new Object[] {sourcePolicyID.getDisplayName()})); //$NON-NLS-1$
                }

                // delete the current the one
                if (options.containsOption(AdminOptions.OnConflict.OVERWRITE)) {
                    overWritten = true;
                    LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, new Object[] {
                        "overwriting existing role", sourcePolicyID.getDisplayName()}); //$NON-NLS-1$
                    aoe.remove(existingPolicy.getAuthorizationPolicyID());
                }

                // ignore this role and go on to the next roles
                if (options.containsOption(AdminOptions.OnConflict.IGNORE)) {
                    if (rpt != null) {
                        rpt.addResourceEntry(PlatformPlugin.Util.getString("AuthorizationServiceImpl.Succeeded_migration"), //$NON-NLS-1$
                                     "", //$NON-NLS-1$
                                     sourcePolicy.getAuthorizationPolicyID().getDisplayName(),
                                     sourcePolicy.getAuthorizationPolicyID().getDisplayName(),
                                     StandardAuthorizationActions.NONE_LABEL,
                                     PlatformPlugin.Util.getString("AuthorizationServiceImpl.Ignored")); //$NON-NLS-1$
                    }
                    LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, new Object[] {
                        "ignoring existing role", sourcePolicyID.getDisplayName()}); //$NON-NLS-1$
                    continue;
                }
            }

            // Create the new policy
            AuthorizationPolicy newPolicy = aoe.createAuthorizationPolicy(newPolicyID);

            if (rpt != null) {
                rpt.addResourceEntry(PlatformPlugin.Util.getString("AuthorizationServiceImpl.Succeeded_migration"), //$NON-NLS-1$
                             "", //$NON-NLS-1$
                             sourcePolicy.getAuthorizationPolicyID().getDisplayName(),
                             newPolicy.getAuthorizationPolicyID().getDisplayName(),
                             StandardAuthorizationActions.NONE_LABEL,
                             overWritten ? PlatformPlugin.Util.getString("AuthorizationServiceImpl.Overwritten") : PlatformPlugin.Util.getString("AuthorizationServiceImpl.Migrated")); //$NON-NLS-1$ //$NON-NLS-2$
            }
            try {
                newPolicy = aoe.clonePolicyPrincipals(sourcePolicy, newPolicy, membershipServiceProxy.getGroupNames(), rpt);
            } catch (MembershipServiceException err) {
                throw new AuthorizationException(err);
            }
            newPolicy = aoe.clonePolicyPermissions(sourcePolicy, newPolicy, targetRealm, targetNodes, rpt);
            newPermissions += newPolicy.getPermissionCount();
        }

        // Commit the actions we've built up
        List actions = aoe.getDestination().getActions();

        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "migrateEntitlements(" //$NON-NLS-1$
                                                                     + targetVDBName + " " + targetVDBVersion //$NON-NLS-1$
                                                                     + ") executing [" //$NON-NLS-1$
                                                                     + actions.size() + "] for [" //$NON-NLS-1$
                                                                     + newPermissions + "] cloned permissions"); //$NON-NLS-1$

        executeTransaction(token, actions);
    }
    
}
