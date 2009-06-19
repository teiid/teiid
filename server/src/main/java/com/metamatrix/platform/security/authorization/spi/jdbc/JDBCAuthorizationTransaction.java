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

package com.metamatrix.platform.security.authorization.spi.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.actions.AddObject;
import com.metamatrix.common.actions.CreateObject;
import com.metamatrix.common.actions.DestroyObject;
import com.metamatrix.common.actions.ExchangeObject;
import com.metamatrix.common.actions.RemoveObject;
import com.metamatrix.common.connection.BaseTransaction;
import com.metamatrix.common.connection.ManagedConnection;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.connection.jdbc.JDBCMgdResourceConnection;
import com.metamatrix.common.id.dbid.DBIDGenerator;
import com.metamatrix.common.id.dbid.DBIDGeneratorException;
import com.metamatrix.common.log.I18nLogManager;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.security.api.AuthorizationActions;
import com.metamatrix.platform.security.api.AuthorizationModel;
import com.metamatrix.platform.security.api.AuthorizationPermission;
import com.metamatrix.platform.security.api.AuthorizationPermissionFactory;
import com.metamatrix.platform.security.api.AuthorizationPermissions;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.GranteeEntitlementEntry;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.StandardAuthorizationActions;
import com.metamatrix.platform.security.authorization.spi.AuthorizationSourceConnectionException;
import com.metamatrix.platform.security.authorization.spi.AuthorizationSourceException;
import com.metamatrix.platform.security.authorization.spi.AuthorizationSourceTransaction;
import com.metamatrix.platform.security.util.RolePermissionFactory;
import com.metamatrix.platform.util.ErrorMessageKeys;

public class JDBCAuthorizationTransaction extends BaseTransaction implements AuthorizationSourceTransaction {

//    private static final int FALSE_ACTIONS_INDEX = -1;
    static final int DEFALT_ARRAY_INDEX = 0;
    static final String DEFALT_POLICY_DESCRIPTION = ""; //$NON-NLS-1$
    static final String ROLE_REALM = RolePermissionFactory.getRealmName();
    private Connection jdbcConnection;


    /**
     * Create a new instance of a transaction for a managed connection.
     * @param connection the connection that should be used and that was created using this
     * factory's <code>createConnection</code> method (thus the transaction subclass may cast to the
     * type created by the <code>createConnection</code> method.
     * @param readonly true if the transaction is to be readonly, or false otherwise
     * @throws ManagedConnectionException if there is an error creating the transaction.
     */
    JDBCAuthorizationTransaction( ManagedConnection connection, boolean readonly) throws ManagedConnectionException {
        super(connection,readonly);
        try {
            JDBCMgdResourceConnection jdbcManagedConnection = (JDBCMgdResourceConnection) connection;
            this.jdbcConnection = jdbcManagedConnection.getConnection();

        } catch ( Exception e ) {
            throw new ManagedConnectionException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0079,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0079));
        }
    }



//==============================================================================
// MetaBase Methods
//==============================================================================
    /**
     * Add the given resources as <code>AuthorizationPermission</code>s to existing
     * <code>AuthorizationPolicies</code> that have a permission with the given parent
     * as a resource. Use the parent's <code>AuthorizationActions</code> to create
     * the permission for each resource.
     * @param parent The uuid of the resource that will be the parent of the given
     * resources.
     * @param resources The uuids of the newly added resources.
     * @param realm Confine the resources to this realm.
     */
    public void addPermissionsWithResourcesToParent(String parent, Collection resources, AuthorizationRealm realm)
            throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        PreparedStatement statement = null;

        String sql = JDBCNames.SELECT_ACTIONS_PERM_FACTORY_AND_POLICYNAME_FOR_RESOURCE_IN_REALM;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "addPermissionsWithResourcesToParent(", parent, resources, realm,")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        // Get a Map of Permission->PolicyID for the Policies this new parent of these resources
        // participates in. We only need the Policies for which the parent is NOT recursive -
        // (recursive parents will show their children by default).
        // We'll need to create Permissions for each given resource, using the Actions of the
        // (non-recursive) parent, and add it to the parent's Policy.

        // Build arg List
        List argList = new ArrayList();
        argList.add(realm.getRealmName());
        // Since we'll only have to add new permissions to parent policies that are NOT
        // recursive, we'll only select those that don't have "/*" on the end.
        argList.add(parent);

        // Create PreparedStatement
        try {
            statement = prepareStatement(this.jdbcConnection, sql, argList);
        } catch(SQLException e) {
            throw new AuthorizationSourceConnectionException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0080,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0080, parent));
        }

        Map policyIDPermissions = new HashMap();
        String tempClassName = null;
        Class factoryClass = null;
        AuthorizationPermissionFactory permFactory = null;
        // Execute - populate the Map of Permission->PolicyID
        try {
            ResultSet results = statement.executeQuery();
            while ( results.next() ) {
                int actions = results.getInt(JDBCNames.AuthPermissions.ColumnName.ACTIONS);
                String factoryClassName = results.getString(JDBCNames.AuthPermTypes.ColumnName.FACTORY_CLASS_NAME);
                String policyName = results.getString(JDBCNames.AuthPolicies.ColumnName.POLICY_NAME);
                String policyDesc = results.getString(JDBCNames.AuthPolicies.ColumnName.DESCRIPTION);

                // Save instantiated permFactory until we find another type we have to
                // instaintiate
                if (! factoryClassName.equals(tempClassName)) {
                    factoryClass = Class.forName(factoryClassName);
                    permFactory = (AuthorizationPermissionFactory) factoryClass.newInstance();
                    tempClassName = factoryClassName;
                }

                AuthorizationPolicyID aPolicyID = new AuthorizationPolicyID(policyName, policyDesc);
                AuthorizationActions newActions = StandardAuthorizationActions.getAuthorizationActions(actions);
                // Must create Permissions for ALL resources
                Iterator resourceItr = resources.iterator();
                while ( resourceItr.hasNext() ) {
                    String resourceName = (String) resourceItr.next();
                    // No need to resolve resource to a path here since we're not returning the permission
                    AuthorizationPermission perm = permFactory.create(resourceName, realm, newActions);
                    policyIDPermissions.put(perm, aPolicyID);
                }
            }
        } catch(SQLException e) {
            throw new AuthorizationSourceConnectionException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0081,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0081, parent));
        } catch(ClassNotFoundException e){
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0082,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0082, factoryClass));
        } catch(IllegalAccessException e){
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0083,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0083));
        } catch(InstantiationException e) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0084,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0084, factoryClass));
        } finally {
            if ( statement != null) {
                try {
                    statement.close();
                } catch(SQLException e) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,e);

                }
                statement = null;
            }
        }

        // Create the new permissions and add them to the Policies
        Iterator newPermItr = policyIDPermissions.keySet().iterator();
        Map policyUUIDs = new HashMap(policyIDPermissions.size());
        while ( newPermItr.hasNext() ) {
            AuthorizationPermission aNewPerm = (AuthorizationPermission) newPermItr.next();
            AuthorizationPolicyID targetPolicyID = (AuthorizationPolicyID) policyIDPermissions.get(aNewPerm);
            try {

                Number policyUID = (Number) policyUUIDs.get(targetPolicyID);
                if (policyUID == null) {
                    policyUID = this.getPolicyUID(targetPolicyID);
                    policyUUIDs.put(targetPolicyID, policyUID);
                }

                 this.addPermission(policyUID, aNewPerm);
            } catch(SQLException e) {
                throw new AuthorizationSourceConnectionException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0086,
                        PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0086, targetPolicyID));
//            } catch(DatabaseViewException e){
//                throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0086,
//                        PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0086, targetPolicyID));
            } catch(AuthorizationSourceException e){
                throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0086,
                        PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0086, targetPolicyID));
            }
        }
    }

    /**
     * Remove all permissions in the system that are on the given resources.
     * @param resources The resource names of the resources to be removed.
     * @param realm Confines the resource names to this realm.
     */
    public void removePermissionsWithResources(Collection resources, AuthorizationRealm realm)
            throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        PreparedStatement statement = null;

        String sql = JDBCNames.DELETE_PERMISSIONS_WITH_RESOURCES_IN;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "removePermissionsWithResources(", resources, realm,")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        // Collect the IN clause
        StringBuffer inBuf = new StringBuffer(sql);
        inBuf.append("('"); //$NON-NLS-1$
        Iterator resourceItr = resources.iterator();
        boolean replaceCommaTick = false;
        while ( resourceItr.hasNext() ) {
            inBuf.append((String)resourceItr.next() + "','"); //$NON-NLS-1$
            replaceCommaTick = true;
        }
        if ( replaceCommaTick ) {
            inBuf.setLength(inBuf.length() - 2);
        }
        inBuf.append("))"); //$NON-NLS-1$
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                             "Removing permissions with resources: " + inBuf.toString() + "<->" + realm.getRealmName()); //$NON-NLS-1$ //$NON-NLS-2$


        // Execute
        try {
            statement = jdbcConnection.prepareStatement(inBuf.toString());
            statement.setString(1, realm.getRealmName());  

            statement.executeUpdate();
        } catch(SQLException e) {
            throw new AuthorizationSourceConnectionException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0088,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0088));
        } finally {
            if ( statement != null) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,e);
                }
                statement = null;
            }
        }
    }

    /**
     * Get the collection of permissions whose resources are dependant on the given permision.
     * The returned collection will contain a permission for each dependant resource all having
     * the actions of the original request.  The search is scoped to the <code>AuthorizationRealm</code>
     * of the given request.
     *
     * @param request The permission for which to find dependants.
     * @return A Collection of dependant permissions all with the actions of the given request. Note:
     * always contains the original permission.
     */
    public Collection getDependantPermissions(AuthorizationPermission request)
            throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Set resourceNames = new HashSet();
        String resourceName = request.getResourceName();
        AuthorizationRealm realm = request.getRealm();

        String sql = JDBCNames.SELECT_DEPENDANT_RESOURCES_FOR_RESOURCE_IN_REALM;
        sql = sql.toUpperCase();
        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,
                            new Object[]{"getDependantPermissions(", request, ")", "SQL: ", sql}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        PreparedStatement statement = null;

        try {
            statement = this.jdbcConnection.prepareStatement(sql);
            statement.setString(1, realm.getRealmName());
            statement.setString(2, resourceName + "%"); //$NON-NLS-1$

            ResultSet results = statement.executeQuery();
            while ( results.next() ) {
                String dependantResourceName = results.getString(JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME);
                resourceNames.add(dependantResourceName);
            }
        } catch (SQLException e) {
            throw new AuthorizationSourceConnectionException(e,
                        PlatformPlugin.Util.getString("JDBCAuthorizationTransaction.Error_getting_dependant_permissions_for_resource_{0}", //$NON-NLS-1$
                                                      resourceName));
        } finally {
            if ( statement != null) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,e);
                }
                statement = null;
            }
        }

        Collection dependants = null;
        if ( resourceNames.size() > 0 ) {
            dependants = new ArrayList(resourceNames.size() +1);
            dependants.add(request);

            String msg = PlatformPlugin.Util.getString("JDBCAuthorizationTransaction.Error_instantiating_factory_class_for_permission__{0}", //$NON-NLS-1$
                                                        request);
            AuthorizationPermissionFactory permFact = null;
            try {
                Class permFactClass = Class.forName(request.getFactoryClassName());
                permFact = (AuthorizationPermissionFactory) permFactClass.newInstance();
            } catch (ClassCastException e) {
                throw new AuthorizationSourceException(e, msg);
            } catch (ClassNotFoundException e) {
                throw new AuthorizationSourceException(e, msg);
            } catch (IllegalAccessException e) {
                throw new AuthorizationSourceException(e, msg);
            } catch (InstantiationException e) {
                throw new AuthorizationSourceException(e, msg);
            }
            AuthorizationActions actions = request.getActions();

            for ( Iterator resourceNameItr = resourceNames.iterator(); resourceNameItr.hasNext(); ) {
                String aResource = (String) resourceNameItr.next();
                AuthorizationPermission aPerm = permFact.create(aResource, realm, actions);
                dependants.add(aPerm);
            }
        } else {
            dependants = new ArrayList(1);
            dependants.add(request);
        }
        return dependants;
    }

    /**
     * Returns a compound <code>List</code> of entitlements to the given fully qualified
     * group in the given realm.
     * The returned <code>List</code> will be comprised of a <code>List</code>s of 6 elements.<br>
     * They are, in order:
     * <ol>
     *   <li value=0>VDB Name</li>
     *   <li>VDB Version</li>
     *   <li>Group Name (fully qualified)</li>
     *   <li>Grantor</li>
     *   <li>Grantee - of type {@link com.metamatrix.platform.security.api.MetaMatrixPrincipalName MetaMatrixPrincipalName}</li>
     *   <li>Allowed Action (one or more of {CREATE, READ, UPDATE, DELETE})</li>
     * </ol>
     * @param realm The realm in which the group must live.
     * @param fullyQualifiedGroupName The resource for which to look up permissions.
     * @return The <code>List</code> of entitlements to the given group in the
     * given realm - May be empty but never null.
     * @throws AuthorizationSourceConnectionException if there is an error communicating with the source.
     * @throws AuthorizationSourceException if there is an unspecified error.
     */
    public Map getGroupEntitlements(AuthorizationRealm realm, String fullyQualifiedGroupName)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "getGroupEntitlements(", realm, ", ", fullyQualifiedGroupName, ")"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        // Get SQL string and create PreparedStatement
        String sql = JDBCNames.SELECT_RESOURCE_PRINCIPALS_GRANTOR_AND_ACTIONS_FOR_RESOURCE_IN_REALM;
        sql = sql.toUpperCase();
// DEBUG:
//System.out.println("\ngetGroupEntitlements SQL: <" + sql + ">");
        List statementArgs = new ArrayList();
        statementArgs.add(realm.getRealmName());
        // Add "wildcard" to end of resource name for group query since we only
        // store entitlement resources as elements
        statementArgs.add(fullyQualifiedGroupName + "%"); //$NON-NLS-1$

        PreparedStatement statement = null;
        try {
            statement = prepareStatement(this.jdbcConnection, sql, statementArgs);
        } catch(SQLException e) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0089,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0089));
        }

        return getEntitlementsForResourceInRealm(statement, true);
// DEBUG:
//System.out.println("JDBC layer found <" + groupEntitlementMap.size() + "> resources.");
 //       return groupEntitlementMap;
    }

    /**
     * Returns a compound <code>List</code> of entitlements to the given fully qualified
     * element in the given realm.
     * The returned <code>List</code> will be comprised of a <code>List</code>s of 7 elements.<br>
     * They are, in order:
     * <ol>
     *   <li value=0>VDB Name</li>
     *   <li>VDB Version</li>
     *   <li>Group Name (fully qualified)</li>
     *   <li>Element</li>
     *   <li>Grantor</li>
     *   <li>Grantee - of type {@link com.metamatrix.platform.security.api.MetaMatrixPrincipalName MetaMatrixPrincipalName}</li>
     *   <li>Allowed Action (one or more of {CREATE, READ, UPDATE, DELETE})</li>
     * </ol>
     * @param realm The realm in which the element must live.
     * @param elementNamePattern The resource for which to look up permissions.
     * @return The <code>List</code> of entitlements to the given element in the
     * given realm - May be empty but never null.
     * @throws AuthorizationSourceConnectionException if there is an error communicating with the source.
     * @throws AuthorizationSourceException if there is an unspecified error.
     */
    public Map getElementEntitlements(AuthorizationRealm realm, String elementNamePattern)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "getElementEntitlements(", realm, ", ", elementNamePattern, ")"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        // Get SQL string and create PreparedStatement
        String sql = JDBCNames.SELECT_RESOURCE_PRINCIPALS_GRANTOR_AND_ACTIONS_FOR_RESOURCE_IN_REALM;
        sql = sql.toUpperCase();
        List statementArgs = new ArrayList();
        statementArgs.add(realm.getRealmName());
        statementArgs.add(elementNamePattern);

        PreparedStatement statement = null;
        try {
            statement = prepareStatement(this.jdbcConnection, sql, statementArgs);
        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0090,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0090));
        }

        return getEntitlementsForResourceInRealm(statement, false);
// DEBUG:
//System.out.println("JDBC layer found <" + eleEntitlementMap.size() + "> resources.");
 //       return eleEntitlementMap;
    }

    /**
     * Helper method for getElementEntitlements() and getGroupEntitlements().
     * This method takes a List of constant Strings and a PreparedStatement, each built differently
     *  by the calling method, adds the result(s) of executing the PreparedStatement, which are
     * the principals assigned the permission (as MetaMatrixPrincipalName objects, so we know their
     * type - User or Group) and adds each resulting List as an element of the returned master List.
     */
    private Map getEntitlementsForResourceInRealm(PreparedStatement statement, boolean isGroup)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "getEntitlementsForResourceInRealm(", statement, ", isGroup=", new Boolean(isGroup) }); //$NON-NLS-1$ //$NON-NLS-2$

        Map resourceToEntitlementMap = new HashMap();
        String resourceName = null;
        String principalName = null;
        int principalType = -1;
        String grantorName = null;
        int actionsValue = -1;
        // This Set will contain all grantees
        Set entitlementEntrySet = null;
// DEBUG:
//System.out.println("\n *** getEntitlements Group: <" + isGroup + ">");
        try {
            ResultSet results = statement.executeQuery();
            while (results.next()) {
                // Add the record variables
                resourceName = results.getString(1);
                principalName = results.getString(2);
                principalType = results.getInt(3);
                grantorName = results.getString(4);
                actionsValue = results.getInt(5);

// DEBUG:
//System.out.println(" *** Found <" + resourceName + "> Principal: <" + principalName + "> Type: <" + principalType + "> Actions: <" + actionsValue + ">");
                // If group query, trim element name from returned resourceName since we've got an
                // element name here (permission resources are stored as element full-paths - EXCEPT
                // for resources with DELETE action which can only be assigned to a group).
                if ( isGroup && ! StandardAuthorizationActions.getAuthorizationActions(actionsValue).implies(
                                                                    StandardAuthorizationActions.DATA_DELETE) ) {
                    resourceName = resourceName.substring(0, resourceName.lastIndexOf('.'));
                }

// DEBUG:
//System.out.println(" *** Resource Now: <" + resourceName + "> Principal: <" + principalName + "> Type: <" + principalType + "> Actions: <" + actionsValue + ">");
                // Create object and insert into Map
                GranteeEntitlementEntry anEntry =
                        new GranteeEntitlementEntry(new MetaMatrixPrincipalName(principalName, principalType),
                                                    grantorName,
                                                    actionsValue);
                entitlementEntrySet = (Set) resourceToEntitlementMap.get(resourceName);
                if ( entitlementEntrySet == null ) {
                    entitlementEntrySet = new HashSet();
                }
                entitlementEntrySet.add(anEntry);
                resourceToEntitlementMap.put(resourceName, entitlementEntrySet);
            }
        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0091,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0091));
        } finally {
            if ( statement != null) {
                try {
                    statement.close();
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
                statement = null;
            }
        }
        return resourceToEntitlementMap;
    }

    /**
     * Obtain the names of all of the realms known to the system.
     * @return the collection of realm names
     */
     public Collection getRealmNames()
     throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        ArrayList realmNames = new ArrayList();
        String name = null;
        PreparedStatement statement = null;

        String sql = JDBCNames.SELECT_ALL_REALM_NAMES;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "getRealmNames()", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$
// DEBUG:
//Object[] OUT =  new Object[] { "getRealmNames()", "SQL: ", sql };
//System.out.println(parseObjArray(OUT));

        try {
//            statement = prepareStatement(this.jdbcConnection, sql);
            statement = jdbcConnection.prepareStatement(sql);

            ResultSet results = statement.executeQuery();
            while (results.next()) {
                name = results.getString(JDBCNames.AuthRealms.ColumnName.REALM_NAME);
                realmNames.add(name);
            }
        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0092,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0092));
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                    statement = null;
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }
        return realmNames;
     }

    /**
     * Returns a Map of String Metamatrix role names to String descriptions
     * of each role.
     * @return Map of String role names to String role descriptions
     * @throws AuthorizationSourceConnectionException if there is an error communicating with the source.
     * @throws AuthorizationSourceException if there is an unspecified error.
     */
    public Map getRoleDescriptions()
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Map roles = new HashMap();
        String sql = JDBCNames.SELECT_ALL_ROLES_AND_DESCRITPIONS;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "getRoleDescriptions()", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$
// DEBUG:
//Object[] OUT = new Object[] { "getRoleDescriptions()", "SQL: ", sql };
//System.out.println(parseObjArray(OUT));

        PreparedStatement statement = null;
        try {
            String roleName = null;
            String description = null;
            statement = jdbcConnection.prepareStatement(sql);
            statement.setString(1, ROLE_REALM);  


//            statement = prepareStatement(this.jdbcConnection, sql, ROLE_REALM);
            ResultSet results = statement.executeQuery();
            while (results.next()) {
                roleName = results.getString(1);
                description = results.getString(2);
                roles.put(roleName, description);
            }
        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0093,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0093));
        } finally {
            if ( statement != null) {
                try {
                    statement.close();
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
                statement = null;
            }
        }
        return roles;
    }

    /**
     * Returns a collection <code>MetaMatrixPrincipalName</code> objects containing the name
     * of the principal along with its type which belong to the given role.
     * {@link com.metamatrix.platform.security.api.MetaMatrixPrincipalName}
     * @param roleName String name of MetaMatrix role for which principals
     * are sought
     * @return The collection of <code>MetaMatrixPrincipalName</code>s who are in the given role, possibly enpty, never null.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public Collection getPrincipalsForRole(String roleName)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Set principals = new HashSet();
        String sql = JDBCNames.SELECT_PRINCIPALS_FOR_ROLE_NAME;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "getPrincipalsForRole(", roleName, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] OUT =  new Object[] { "getPrincipalsForRole(", roleName, ")", "SQL: ", sql };
//System.out.println(parseObjArray(OUT));

        PreparedStatement statement = null;
        List statementArgs = new ArrayList();
        statementArgs.add(ROLE_REALM);
        statementArgs.add(roleName);
        try {
            String principalName = null;
            int principalType = -1;
            statement = prepareStatement(this.jdbcConnection, sql, statementArgs);
            ResultSet results = statement.executeQuery();
            while (results.next()) {
                principalName = results.getString(1);
                principalType = results.getInt(2);
                principals.add(new MetaMatrixPrincipalName(principalName, principalType));
            }
        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0094,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0094));
        } finally {
            if ( statement != null) {
                try {
                    statement.close();
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
                statement = null;
            }
        }
        return principals;
    }
    
     

    /**
     * Returns a Collection of String names of MetaMatrix roles which the
     * given principal belongs to
     * @param principals <code>MetaMatrixPrincipalName</code>s of a principal and
     * any group memberships for which roles are sought
     * @return The collection of role names belonging to the given principal, possibly enpty, never null.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public Collection getRoleNamesForPrincipal(Collection principals)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Set roleNames = new HashSet();
        String sql = JDBCNames.SELECT_ROLE_NAMES_FOR_PRINCIPAL_NAME;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "getRoleNamesForPrincipal(", principals, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] OUT =  new Object[] { "getRoleNamesForPrincipal(", principals, ")", "SQL: ", sql };
//System.out.println(parseObjArray(OUT));
        PreparedStatement statement = null;
        try {

            statement = jdbcConnection.prepareStatement(sql);

            Iterator principalItr = principals.iterator();
            while ( principalItr.hasNext() ) {
                MetaMatrixPrincipalName principal = (MetaMatrixPrincipalName) principalItr.next();


                    String roleName = null;

                    statement.setString(1, ROLE_REALM);  
                    statement.setString(2, principal.getName());  


                    ResultSet results = statement.executeQuery();
                    while (results.next()) {
                        roleName = results.getString(1);
                        roleNames.add(roleName);
                    }

                statement.clearParameters();
            }
        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0095,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0095));
        } finally {
            if ( statement != null) {
                try {
                    statement.close();
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
                statement = null;
            }
        }

        return roleNames;
    }

    /**
     * Return whether there is an existing policy with the specified ID.
     * @param id the ID that is to be checked
     * @return true if a policy with the specified ID exists
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public boolean containsPolicy(AuthorizationPolicyID id)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        if( id == null){
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0096));
        }

//        LogManager.logTrace( LogSecurityConstants.CTX_AUTHORIZATION,
//            new Object[] { "containsPolicy(", id, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        try {
            Number result = getPolicyUID(id);
            if (result == null) {
                return false;
            }
            return true;

          } catch ( SQLException e ) {
              throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0097,
                      PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0097, id));
        }
    }

    /**
     * Locate the IDs of all of the policies that are accessible by the caller.
     * @return the set of all policy IDs
     */
    public Collection findAllPolicyIDs()
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Collection policyIDs = new ArrayList();
        PreparedStatement statement = null;
        // select policyName from authPolicies
        String sql = JDBCNames.SELECT_ALL_POLICIES;
        sql = sql.toUpperCase();
        LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "findAllPolicyIDs()", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$
// DEBUG:
//Object[] OUT =  new Object[] { "findAllPolicyIDs()", "SQL: ", sql };
//System.out.println(parseObjArray(OUT));

        try {
            statement = jdbcConnection.prepareStatement(sql);

            ResultSet results = statement.executeQuery();
            policyIDs = this.populatePolicyIDs(results);
        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0098,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0098));
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                    statement = null;
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }
        return policyIDs;
    }

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermission</code>s in the given <code>AuthorizationRealm</code>.<br>
     * <strong>NOTE:</strong> It is the responsibility of the caller to determine
     * which of the <code>AuthorizationPolicy</code>'s <code>AuthorizationPermission</code>s
     * are actually in the given <code>AuthorizationRealm</code>.  The <code>AuthorizationPolicy</code>
     * may span <code>AuthorizationRealm</code>s.
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * in the given realm - possibly empty but never null.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public Collection getPolicyIDsWithPermissionsInRealm(AuthorizationRealm realm)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Collection policyIDs = new ArrayList();
        PreparedStatement statement = null;

        String sql = JDBCNames.SELECT_POLICY_NAMES_WITH_PERMISSIONS_IN_REALM;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "getPolicyIDsWithPermissionsInRealm(", realm, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] OUT =  new Object[] { "getPolicyIDsWithPermissionsInRealm(", realm, ")", sql };
//System.out.println("\n ** Realm: " + AuthorizationPolicyID.parseRealm(realm));
//System.out.println(parseObjArray(OUT));

       try{

           statement = jdbcConnection.prepareStatement(sql);
           statement.setString(1, realm.getRealmName());

            ResultSet results = statement.executeQuery();
            policyIDs = this.populatePolicyIDs(results);
       } catch ( SQLException e ) {
           throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0099,
                   PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0099, realm));
        } finally {
            if (statement != null) {
                try {
                     statement.close();
                     statement = null;
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }
        return policyIDs;
    }
    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * in the given <code>AuthorizationRealm</code>.
     * <br>This method will only work for Data Access Authorizations because the realm
     * is encoded in a Data Access policy name.
     * <strong>NOTE:</strong> It is the responsibility of the caller to determine
     * which of the <code>AuthorizationPolicy</code>'s <code>AuthorizationPermission</code>s
     * are actually in the given <code>AuthorizationRealm</code>.  The <code>AuthorizationPolicy</code>
     * may span <code>AuthorizationRealm</code>s.
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * in the given realm - possibly empty but never null.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public Collection getPolicyIDsInRealm(AuthorizationRealm realm)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Collection policyIDs = new ArrayList();
        PreparedStatement statement = null;

        String sql = JDBCNames.SELECT_POLICY_NAMES_FOR_REALM;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "getPolicyIDsInRealm(", realm, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] OUT =  new Object[] { "getPolicyIDsWithPermissionsInRealm(", realm, ")", sql };
//System.out.println("\n ** Realm: " + AuthorizationPolicyID.parseRealm(realm));
//System.out.println(parseObjArray(OUT));

       try{
           statement = jdbcConnection.prepareStatement(sql);
           statement.setString(1, "%" + AuthorizationPolicyID.parseRealm(realm));  //$NON-NLS-1$

            ResultSet results = statement.executeQuery();
            policyIDs = this.populatePolicyIDs(results);
       } catch ( SQLException e ) {
           throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0100,
                   PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0100, realm));
        } finally {
            if (statement != null) {
                try {
                     statement.close();
                     statement = null;
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }
        return policyIDs;
    }

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermissionsImpl</code> that exist in the given
     * <emph>partial</emph> <code>AuthorizationRealm</code>.<br>
     * The implementation is such that all <code>AuthorizationPolicyID</code>s
     * whose <code>AuthorizationRealm</code> <emph>starts with</emph> the given
     * <code>AuthorizationRealm</code> are returned.
     * @param realm The <emph>partial</emph> realm in which to search for
     * <code>AuthorizationPermission</code>s whose realm name <emph>starts with</emph>
     * the given realm.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * in the given partial realm - possibly empty but never null.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public Collection getPolicyIDsInPartialRealm(AuthorizationRealm realm)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Collection policyIDs = new ArrayList();
        PreparedStatement statement = null;

        String sql = JDBCNames.SELECT_POLICY_NAMES_FOR_REALM_STARTS_WITH;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "getPolicyIDsInPartialRealm(", realm, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] OUT =  new Object[] { "getPolicyIDsInPartialRealm(", realm, ")", sql };
//System.out.println(parseObjArray(OUT));

       try{
           statement = jdbcConnection.prepareStatement(sql);
           statement.setString(1, realm.getRealmName() + "%");//$NON-NLS-1$

            ResultSet results = statement.executeQuery();
            policyIDs = this.populatePolicyIDs(results);
       } catch ( SQLException e ) {
           throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0101,
                   PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0101, realm));
        } finally {
            if (statement != null) {
                try {
                     statement.close();
                     statement = null;
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }
        return policyIDs;
    }

    /**
     * Locate the IDs of all of the policies that apply to the specified principals
     * and are in the given realm.
     * @param principals the Set of <code>MetaMatrixPrincipalName</code> to whom
     * the returned policies should apply to  (may not null, empty or invalid,
     * all of which would result in an empty result).
     * @return the set of all policy IDs; never null but possibly empty
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public Collection findPolicyIDs(Collection principals, AuthorizationRealm realm)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Collection policyIDs = new ArrayList();
        MetaMatrixPrincipalName principalName = null;
        PreparedStatement statement = null;
        String realmName = realm.getRealmName();

        // select policyName from authPolicies for each principal
        String sql = JDBCNames.SELECT_POLICY_NAMES_FOR_PRINCIPALS_IN_REALM;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "findPolicyIDs(", principals, realmName, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] OUT =  new Object[] { "findPolicyIDs(", principals, realmName, ")", sql };
//System.out.println(parseObjArray(OUT));

        try {
            statement = jdbcConnection.prepareStatement(sql);

              Iterator iter = principals.iterator();
              while (iter.hasNext()){
                    principalName = (MetaMatrixPrincipalName) iter.next();

                    statement.setString(1, principalName.getName().toLowerCase());
                    statement.setString(2, realmName.toLowerCase());


                    ResultSet results = statement.executeQuery();
                    policyIDs.addAll(populatePolicyIDs(results));

                    statement.clearParameters();
              }
        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0102,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0102, realm));
        } finally {
            if (statement != null){
                try {
                     statement.close();
                     statement = null;
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }

        return policyIDs;
    }

    /**
     * Locate the IDs of all of the policies that apply to the specified principals.
     * @param principals the Set of <code>MetaMatrixPrincipalName</code> to whom
     * the returned policies should apply to  (may not null, empty or invalid,
     * all of which would result in an empty result).
     * @return the set of all policy IDs; never null but possibly empty
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public Collection findPolicyIDs(Collection principals)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Collection policyIDs = new ArrayList();
        MetaMatrixPrincipalName principalName = null;
        PreparedStatement statement = null;

        // select policyName from authPolicies for each principal
        String sql = JDBCNames.SELECT_POLICY_NAMES_FOR_PRINCIPALS;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "findPolicyIDs(", principals, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] OUT =  new Object[] { "findPolicyIDs(", principals ")", sql };
//System.out.println(parseObjArray(OUT));

        try {
                statement = jdbcConnection.prepareStatement(sql);

                Iterator iter = principals.iterator();
                while (iter.hasNext()){
                    principalName = (MetaMatrixPrincipalName) iter.next();
                    statement.setString(1, principalName.getName());

                    ResultSet results = statement.executeQuery();
                    policyIDs.addAll(populatePolicyIDs(results));

                    statement.clearParameters();
                }
        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0103,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0103));
        } finally {
            if (statement != null){
                try {
                     statement.close();
                     statement = null;
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }

        return policyIDs;
    }

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermissionsImpl</code> on the given resource that
     * exists in the given <code>AuthorizationRealm</code>.<br>
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @param resourceName The resource for which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * on the given resource - possibly empty but never null.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public Collection getPolicyIDsForResourceInRealm(AuthorizationRealm realm, String resourceName)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Collection policyIDs = new ArrayList();
        PreparedStatement statement = null;

        String sql = JDBCNames.SELECT_POLICY_NAMES_WITH_RESOURCE_IN_REALM;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "getPolicIDsForResourceInRealm(", realm, resourceName, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] OUT =  new Object[] { "getPolicIDsForResourceInRealm(", realm, resourceName, ")", sql };
//System.out.println(parseObjArray(OUT));

        List statementArgs = new ArrayList();
        statementArgs.add(realm.getRealmName());
        statementArgs.add(resourceName);
       try{
            statement = prepareStatement(this.jdbcConnection, sql, statementArgs);
            ResultSet results = statement.executeQuery();
            policyIDs = this.populatePolicyIDs(results);
       } catch ( SQLException e ) {
           throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0104,
                   PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0104, realm));
        } finally {
            if (statement != null) {
                try {
                     statement.close();
                     statement = null;
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }
        return policyIDs;
    }

    /**
     * Locate the policies that have the specified IDs.  Any ID that is invalid is simply
     * ignored.
     * @param policyIDs the policy IDs for which the policies are to be obtained
     * @return the set of entitlements that correspond to those specified IDs that are valid
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public Collection getPolicies(Collection policyIDs)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Collection results = new HashSet();
        Iterator iter = policyIDs.iterator();
        while(iter.hasNext()){
            AuthorizationPolicyID policyID = (AuthorizationPolicyID) iter.next();
            results.add(this.getPolicy(policyID));
        }
        return results;
    }

    /**
     * Locate the policy that has the specified ID.  Any ID that is invalid is simply
     * ignored.
     * specified policies
     * @param policyID the ID of the policy to be obtained
     * @return the policy that correspond to the specified ID
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public AuthorizationPolicy getPolicy(AuthorizationPolicyID policyID)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        return JDBCAuthorizationReader.getPolicy(policyID, jdbcConnection);
    }

    /**
     * Find and create all <code>AuthorizationPermissionsImpl</code> known to a policy.
     * @param policyID The policy indentifier.
     * @return The set of all permissions that belong to the given policy.
     */
    public Set getPermissionsForPolicy(AuthorizationPolicyID policyID)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        return JDBCAuthorizationReader.getPermissionsForPolicy(policyID, jdbcConnection);
    }

    /**
     * Remove given Principal from <emph>ALL</emph> <code>AuthorizationPolicies</code> to
     * which he belongs.
     * @param principal <code>MetaMatrixPrincipalName</code> which should be deleted.
     * @return true if at least one policy in which the principal had authorization
     * was found and deleted, false otherwise.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public boolean removePrincipalFromAllPolicies(MetaMatrixPrincipalName principal)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        String principalName = principal.getName();
        boolean removed = false;
//        Collection policyIDs = new ArrayList();
        PreparedStatement statement = null;

        String sql = JDBCNames.DELETE_PRINCIPAL_FROM_ALL_POLICIES;
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "removePrincipalFromAllPolicies(", principal, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] OUT =  new Object[] { "getPolicIDsForResourceInRealm(", realm, resourceName, ")", sql };
//System.out.println(parseObjArray(OUT));


       try{
           statement = jdbcConnection.prepareStatement(sql);
           statement.setString(1, principalName);
           statement.execute();
           LogManager.logDetail( LogConstants.CTX_AUTHORIZATION, "Deleted principal " + principalName + " from policies."); //$NON-NLS-1$ //$NON-NLS-2$
           removed = true;
        } catch ( SQLException e ) {
             throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0109,
                     PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0109, principal));

        } finally {
            if (statement != null) {
                try {
                     statement.close();
                     statement = null;
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }


        return removed;
    }

    /**
     * Execute the actions on given object.
     * @param targetPolicyID The ID of the policy on which to execute the transactions.
     * @param actions The list of actions to execute.
     * @param grantor The principal name of the policy grantor.
     * @return The set of objects effected by this method.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public Set executeActions(AuthorizationPolicyID targetPolicyID, List actions, String grantor)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException, AuthorizationSourceException {
        Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }

        affectedIDs.add(targetPolicyID);
        try {
            // Iterate through the actions ...
            Iterator iter = actions.iterator();
//            DatabaseTable table = null;
            // This should be an integer, but Oracle returns a BigDecimal.
            Number policyUID = null;
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();
                Object args[] = action.getArguments();
                //====================
                // Create
                //====================
                if(action instanceof CreateObject) {
                    AuthorizationPolicy policy = (AuthorizationPolicy) args[DEFALT_ARRAY_INDEX];
                    LogManager.logDetail( LogConstants.CTX_AUTHORIZATION, "Creating Policy: " + targetPolicyID); //$NON-NLS-1$
                    if(policy != null){
//                        table = JDBCNames.AuthPolicyUpdateView.TABLE;
                        this.addPolicyIntoAuthPolicies(policy);
                    }
                //====================
                // Add
                //====================
                } else if (action instanceof AddObject) {
                    if ( policyUID == null ) {
                        policyUID = this.getPolicyUID(targetPolicyID);
                        if ( policyUID == null ) {
                            break;
                        }
                    }
                    AddObject anAction = (AddObject) action;
                    if (anAction.getAttributeCode().intValue() == AuthorizationModel.Attribute.PRINCIPAL_NAME.getCode()) {
                        // Add a principal
//                        table = JDBCNames.AuthPrincipalsUpdateView.TABLE;
                        MetaMatrixPrincipalName newPrincipal = (MetaMatrixPrincipalName) args[DEFALT_ARRAY_INDEX];
                        if( newPrincipal != null){

                            LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                                "Adding Principal " + newPrincipal + " to Policy: " + targetPolicyID); //$NON-NLS-1$ //$NON-NLS-2$
                            this.addPrincipal(policyUID, newPrincipal, grantor);
                        }
                    } else if (anAction.getAttributeCode().intValue() == AuthorizationModel.Attribute.PRINCIPAL_SET.getCode()) {
                        // Add a set of principals
//                        table = JDBCNames.AuthPrincipalsUpdateView.TABLE;
                        Set newPrincipals = (Set) args[DEFALT_ARRAY_INDEX];
                        if ( (newPrincipals != null) && (!newPrincipals.isEmpty())){

                            LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                                "Adding Principals " + newPrincipals + " to Policy: " + targetPolicyID); //$NON-NLS-1$ //$NON-NLS-2$
                            this.addPrincipals(policyUID, newPrincipals, grantor);
                        }

                    } else if (anAction.getAttributeCode().intValue() == AuthorizationModel.Attribute.PERMISSION.getCode()) {
                        // Add a permission
 //                       table = JDBCNames.AuthPermissionsUpdateView.TABLE;
                        AuthorizationPermission newPerm= (AuthorizationPermission) args[DEFALT_ARRAY_INDEX];
                        if( newPerm != null){

                            LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                                "Adding Permission " + newPerm + " to Policy: " + targetPolicyID); //$NON-NLS-1$ //$NON-NLS-2$
                            this.addPermission(policyUID, newPerm);
                        }
                    } else if (anAction.getAttributeCode().intValue() == AuthorizationModel.Attribute.PERMISSIONS.getCode()) {
                        // Add a set of permissions
//                        table = JDBCNames.AuthPermissionsUpdateView.TABLE;
                        AuthorizationPermissions newPerms = (AuthorizationPermissions) args[DEFALT_ARRAY_INDEX];
                        if( newPerms != null){

                            LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                                "Adding Permissions " + newPerms + " to Policy: " + targetPolicyID); //$NON-NLS-1$ //$NON-NLS-2$
                            this.addPermissions(policyUID, newPerms );
                        }
                    }
                //====================
                // Exchange
                //====================
                } else if (action instanceof ExchangeObject) {
                    if ( policyUID == null ) {
                        policyUID = this.getPolicyUID(targetPolicyID);
                    }
                    ExchangeObject anAction = (ExchangeObject) action;
                    if (anAction.getAttributeCode().intValue() == AuthorizationModel.Attribute.DESCRIPTION.getCode()) {
                        //
                        // Exchange description
                        String newDescription = (String) args[DEFALT_ARRAY_INDEX + 1];
                        LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                                "Setting description: " + newDescription + " on Policy: " + targetPolicyID); //$NON-NLS-1$ //$NON-NLS-2$

                        updateAuthPolicy(policyUID, newDescription);


                    }
                //====================
                // Remove
                //====================
                } else if (action instanceof RemoveObject) {
                    if ( policyUID == null ) {
                        policyUID = this.getPolicyUID(targetPolicyID);
                    }
                    RemoveObject anAction = (RemoveObject) action;
                    if (anAction.getAttributeCode().intValue() == AuthorizationModel.Attribute.PERMISSION_SET.getCode()) {
                        //
                        // Remove AuthorizationPermissionsImpl (as Set of AuthorizationPermission objs)
                        Set permsToRemove = (Set) args[DEFALT_ARRAY_INDEX];
                        //Iterator permIter = newPermissions.iterator();
//                        table = JDBCNames.AuthPermissionsUpdateView.TABLE;
                        //newPermissions.removeAll(oldPermissions);
                        if( (permsToRemove != null) && (!permsToRemove.isEmpty())) {

                            LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                                "Removing Permissions: " + permsToRemove + " from Policy: " + targetPolicyID); //$NON-NLS-1$ //$NON-NLS-2$

                            executeBatchRemovePermissions(policyUID, permsToRemove.iterator());
//                            this.removePermissions(policyUID, permsToRemove);
                        }
                    } else if (anAction.getAttributeCode().intValue() == AuthorizationModel.Attribute.PERMISSIONS.getCode()) {
                        //
                        // RemoveAll permissions (as AuthorizationPermissionsImpl collection)
                        AuthorizationPermissions permsToRemove = (AuthorizationPermissions) args[DEFALT_ARRAY_INDEX];
//                        table = JDBCNames.AuthPermissionsUpdateView.TABLE;
                        if( permsToRemove != null){

                            LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                                "Removing Permissions: " + permsToRemove + " from Policy: " + targetPolicyID); //$NON-NLS-1$ //$NON-NLS-2$
                            executeBatchRemovePermissions(policyUID, permsToRemove.iterator());

//                            this.removePermissions(policyUID, permsToRemove);
                        }
                    } else if (anAction.getAttributeCode().intValue() == AuthorizationModel.Attribute.PERMISSION.getCode()) {
                        //
                        // Remove permission
                        AuthorizationPermission permToRemove = (AuthorizationPermission) args[DEFALT_ARRAY_INDEX];
//                        table = JDBCNames.AuthPermissionsUpdateView.TABLE;
                        if( permToRemove != null){

                            LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                                "Removing Permission: " + permToRemove + " from Policy: " + targetPolicyID); //$NON-NLS-1$ //$NON-NLS-2$
                            this.removePermission( policyUID, permToRemove);
                        }
                    } else if (anAction.getAttributeCode().intValue() == AuthorizationModel.Attribute.PRINCIPAL_SET.getCode()) {
                        //
                        // Remove/RemoveAll principals
//                        table = JDBCNames.AuthPrincipalsUpdateView.TABLE;
                        Set principalsToRemove = (Set) args[DEFALT_ARRAY_INDEX];
                        if( (principalsToRemove != null) && (!principalsToRemove.isEmpty())) {
                            LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                                "Removing Principals: " + principalsToRemove + " from Policy: " + targetPolicyID); //$NON-NLS-1$ //$NON-NLS-2$
                            this.removePrincipals(policyUID, principalsToRemove);
                        }
                    } else if (anAction.getAttributeCode().intValue() == AuthorizationModel.Attribute.PRINCIPAL_NAME.getCode()) {
                        //
                        // Remove principal
                        MetaMatrixPrincipalName principalToRemove = (MetaMatrixPrincipalName) args[DEFALT_ARRAY_INDEX];
//                        table = JDBCNames.AuthPrincipalsUpdateView.TABLE;
                        if( (principalToRemove != null) ) {
                            LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                                "Removing Principal: " + principalToRemove + " to Policy: " + targetPolicyID); //$NON-NLS-1$ //$NON-NLS-2$
//                            boolean removed =
                            this.removePrincipal(policyUID, principalToRemove);
                        }
                    }
                //====================
                // Destroy
                //====================
                } else if (action instanceof DestroyObject) {
                    if ( policyUID == null ) {
                        policyUID = this.getPolicyUID(targetPolicyID);
                    }

                    LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                        "Destroying Policy: " + targetPolicyID); //$NON-NLS-1$
                    // Remove all permissions from policy
//                    table = JDBCNames.AuthPermissionsUpdateView.TABLE;
//                    String criteria = JDBCNames.AuthPermissionsUpdateView.Columns.POLICY_UID.getNameInSource();
                    LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                        "Removing all Permissions from Policy: " + targetPolicyID); //$NON-NLS-1$
                    this.removeAll(JDBCNames.DELETE_ALL_PERMISSIONS_FOR_POLICY, policyUID);
                    //table, criteria, policyUID);

                    // Remove all principals from policy
//                    table = JDBCNames.AuthPrincipalsUpdateView.TABLE;
//                    criteria = JDBCNames.AuthPrincipalsUpdateView.Columns.POLICY_UID.getNameInSource();
                    LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                        "Removing all Principals from Policy: " + targetPolicyID); //$NON-NLS-1$
                    this.removeAll(JDBCNames.DELETE_ALL_PRINCIPALS_FOR_POLICY,policyUID);
//                    table, criteria, policyUID);

                    // Remove the policy
//                    table = JDBCNames.AuthPolicyUpdateView.TABLE;
//                    criteria = JDBCNames.AuthPolicyUpdateView.Columns.POLICY_UID.getNameInSource();
                    LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
                        "Removing the Policy: " + targetPolicyID); //$NON-NLS-1$
                    this.removeAll(JDBCNames.DELETE_ALL_POLICIES_FOR_POLICY, policyUID);
//                    table, criteria, policyUID);
                } else {
                    // Error: Unknown Action
                    throw new AuthorizationSourceException(ErrorMessageKeys.SEC_AUTHORIZATION_0110,
                            PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0110, action, targetPolicyID));
                }
            }

//        } catch ( DatabaseViewException e ) {
//            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0108,
//                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0108, targetPolicyID));

        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0108,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0108, targetPolicyID));
//        } catch ( DatabaseViewException e ) {
//            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0108,
//                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0108, targetPolicyID));
        }

        return affectedIDs;
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    /**
     * Return a collection of newly created policyIDs from a query result.
     */
    private Collection populatePolicyIDs(ResultSet results) throws SQLException {
        Set policyIDs = new HashSet();
        String name = null;
        String desc = null;
        while (results.next()) {
            name = results.getString(JDBCNames.AuthPolicies.ColumnName.POLICY_NAME);
            desc = results.getString(JDBCNames.AuthPolicies.ColumnName.DESCRIPTION);
            AuthorizationPolicyID pID = new AuthorizationPolicyID(name, desc);
            policyIDs.add(pID);
        }
        return policyIDs;
    }

    /**
     * Add a permission to policy. This permission is added into database regardless if it exists.
     *
     */
    private void addPermission(Number policyUID, AuthorizationPermission perm )
    throws SQLException, AuthorizationSourceException {
        // Set up view for table
//        String criteriaColumnName = JDBCNames.AuthPermissionsUpdateView.Columns.POLICY_UID.getNameInSource();
//        DatabaseView view = this.getDatabaseView(table, criteriaColumnName, policyUID );
//        DatabaseView.Results rowSet = view.getResults();
          Number permissionTypeUID = getPermissionTypeUID(perm);
//        // Add realm if not already present
          Number realmUID = updateAuthorizationRealm(perm);



        PreparedStatement statement = null;

        String sql = JDBCNames.INSERT_PERMISSION;
        sql = sql.toUpperCase();

 //       List statementArgs = new ArrayList();
 //       statementArgs.add(principalName);
       try{
           long permUID = DBIDGenerator.getInstance().getID(JDBCNames.AuthPermissions.TABLE_NAME);

           statement = jdbcConnection.prepareStatement(sql);

           statement.setLong(1, policyUID.longValue());
           statement.setLong(2, realmUID.longValue());
           statement.setLong(3, permUID);
           statement.setLong(4, permissionTypeUID.longValue());

           // vah - 8-17-03 DB2 driver does not handle
           // the conversion of boolean (true or false) to
           // char (1 or 0) like the oracle driver.
           statement.setString(5, perm.getResourceName());

           statement.setString(6, perm.getContentModifier());
           statement.setInt(7, perm.getActions().getValue());


            statement.execute();
           LogManager.logDetail( LogConstants.CTX_AUTHORIZATION, "Inserted permissions for policy " + perm.getResourceName()); //$NON-NLS-1$ 
         } catch ( DBIDGeneratorException e ) {
             String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0108, perm.getResourceName(), perm.getRealmName());
             throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0111, msg);

        } catch ( SQLException e ) {
         String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0108, perm.getResourceName(), perm.getRealmName());
         throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0111, msg);

        } finally {
            if (statement != null) {
                try {
                     statement.close();
                     statement = null;
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }

    }

    /**
     * Add a permissions to policy - AuthorizationPermissionsImpl.
     */
    private void addPermissions(Number policyUID, AuthorizationPermissions permissions )
    throws SQLException, AuthorizationSourceException {
        // Set up view for table

        executeBatchAddPermissions(policyUID, permissions);

//        Iterator permIter = permissions.iterator();
//
//        while ( permIter.hasNext() ) {
//            AuthorizationPermission perm = (AuthorizationPermission) permIter.next();
////DEBUG:
////System.out.println("\n *** addPermissions: Adding perm: " + perm);
//            this.addPermission(policyUID, perm);
//        }
    }

//    private void addPermissions(Number policyUID, DatabaseTable table, Set permSet )
//    throws SQLException, DatabaseViewException, AuthorizationSourceException {
//        // Set up view for table
//        Iterator permIter = permSet.iterator();
//
//        while ( permIter.hasNext() ) {
//            AuthorizationPermission perm = (AuthorizationPermission) permIter.next();
//            this.addPermission(policyUID, table, perm);
//        }
//    }


    /**
     * Remove a permission from policy. The permission <code> perm <code> is deleted as well as the permissions
     * which have the same contents but permission uid.
     */
    private void removePermission(Number policyUID, AuthorizationPermission perm)
    throws AuthorizationSourceException {
     
      PreparedStatement statement = null;
//      Number permTypeUID = this.getPermissionTypeUID(perm);

      String sql = JDBCNames.DELETE_PERMISSION;
      sql = sql.toUpperCase();

      try{
          statement = jdbcConnection.prepareStatement(sql);
          LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
              new Object[] { "removePermission(", policyUID, perm.getRealm(),")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

         statement.setLong(1, policyUID.longValue());
         statement.setString(2, perm.getResourceName());

          statement.execute();

          LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                               "Removed permission: " + policyUID + "<->" + perm.getRealm()); //$NON-NLS-1$ //$NON-NLS-2$

//       /DEBUG:
//       //System.out.println(" *** removePermission: Deleting rowSet for resource: " + perm.getResourceName());

       } catch ( SQLException e ) {
           throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0112,
                   PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0112, perm));

      } finally {
          if (statement != null) {
              try {
                   statement.close();
                   statement = null;
              } catch ( SQLException se ) {
                  I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
              }
          }
      }

    }




    /**
     * Remove all (permissions AND principals) from policy.
     */
    private void removeAll(String sql, Number policyUID) throws AuthorizationSourceException {

        PreparedStatement statement = null;

        sql = sql.toUpperCase();

        List statementArgs = new ArrayList();
        statementArgs.add(policyUID);

        try {
            statement = jdbcConnection.prepareStatement(sql);
            statement.setLong(1, policyUID.longValue());

            statement.execute();

        } catch (SQLException e) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0113,
                                                   PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0113,
                                                                                 policyUID));

        } finally {
            if (statement != null) {
                try {
                    statement.close();
                    statement = null;
                } catch (SQLException se) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085, se);
                }
            }
        }
    }




    private void addPrincipal(Number policyUID, MetaMatrixPrincipalName principal, String grantor)
    throws SQLException {


       PreparedStatement statement = null;

       String sql = JDBCNames.INSERT_PRINCIPAL;
       sql = sql.toUpperCase();

      try{

          statement = jdbcConnection.prepareStatement(sql);

          statement.setLong(1, policyUID.longValue());
          statement.setString(2, principal.getName());
          statement.setLong(3, principal.getType());
          statement.setString(4, grantor);


           statement.execute();
//           policyIDs = this.populatePolicyIDs(results);
          LogManager.logDetail( LogConstants.CTX_AUTHORIZATION, "Inserted principal " + principal.getName()); //$NON-NLS-1$ 
//        } catch ( DBIDGeneratorException e ) {
//            String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0108, perm.getResourceName(), perm.getRealmName());

       } finally {
           if (statement != null) {
               try {
                    statement.close();
                    statement = null;
               } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
               }
           }
       }


    }

    private void addPrincipals(Number policyUID, Set principals, String grantor)
    throws SQLException {
         // Set up view for table
//        String criteriaColumnName = JDBCNames.AuthPrincipalsUpdateView.Columns.POLICY_UID.getNameInSource();
//        DatabaseView view = this.getDatabaseView(table, criteriaColumnName, policyUID);
        Iterator principalItr = principals.iterator();
        while ( principalItr.hasNext() ) {
            MetaMatrixPrincipalName principal = (MetaMatrixPrincipalName) principalItr.next();
            this.addPrincipal(policyUID, principal, grantor);
        }
    }

     /**
     * Remove a principal from policy.
     */
    private boolean removePrincipal( Number policyUID, MetaMatrixPrincipalName principal )
    throws SQLException, AuthorizationSourceException {
        // Set up view for table
        boolean removed = false;

      PreparedStatement statement = null;

      String sql = JDBCNames.DELETE_PRINCIPAL;
      sql = sql.toUpperCase();


     try{

         statement = jdbcConnection.prepareStatement(sql);
         LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
             new Object[] { "removePrincipal(", policyUID, principal,")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

         statement.setLong(1, policyUID.longValue());
         statement.setString(2, principal.getName());

          statement.execute();

         LogManager.logDetail(LogConstants.CTX_AUTHORIZATION,
                              "Removed principal: " + policyUID + "<->" + principal); //$NON-NLS-1$ //$NON-NLS-2$

    //   /DEBUG:
    //   //System.out.println(" *** removePermission: Deleting rowSet for resource: " + perm.getResourceName());


      } finally {
         if (statement != null) {
              try {
                   statement.close();
                   statement = null;
              } catch ( SQLException se ) {
                   I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
              }
          }
      }


        return removed;
  }

    
    private boolean removePrincipals(Number policyUID, Set principals)
    throws SQLException, AuthorizationSourceException {
        Iterator iter = principals.iterator();
        boolean allRemoved = true;

        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "removePrincipals(", policyUID, ")" }); //$NON-NLS-1$ //$NON-NLS-2$ 

        while( iter.hasNext() ){
            MetaMatrixPrincipalName principal = (MetaMatrixPrincipalName) iter.next();
            // each time this method recalls above method, there is no much overhead because ...
            if ( this.removePrincipal(policyUID, principal) == false && allRemoved == true ) {
                allRemoved = false;
            }
        }
        return allRemoved;
    }
    
    
    
    
    /**
     * Remove entries from AUTHREALM, AUTHPERMISSIONS, AUTHPOLICIES, AUTHPRINCIPALS
     * for the specified realm 
     * @param realm
     * @return
     * @throws SQLException
     * @throws AuthorizationSourceException
     * @since 4.3
     */
    public void removePrincipalsAndPoliciesForRealm(AuthorizationRealm realm) throws AuthorizationSourceException {

        try {
            Number realmUID = getRealmUID(realm);
            
            removePermissionsForRealm(realmUID);
            removeRealm(realmUID);
            
            removePrincipalsForRealm(realm);
            removePoliciesForRealm(realm);
            
        
        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0122,
                PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0122, realm));
        }

    }
    
    /**
     * Remove entries from AUTHPERMISSIONS for the specified realm 
     * @param realmUID
     * @since 4.3
     */
    protected void removePermissionsForRealm(Number realmUID) throws SQLException {
        PreparedStatement statement = null;
        String sql = JDBCNames.DELETE_PERMISSIONS_FOR_REALM;
        sql = sql.toUpperCase();

        try {
            statement = jdbcConnection.prepareStatement(sql);
            LogManager.logTrace(LogConstants.CTX_AUTHORIZATION, new Object[] {
                "removePermissionsForRealm(", realmUID, ")", "SQL: ", sql}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

            statement.setLong(1, realmUID.longValue());
            statement.execute();

            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "Removed AUTHPERMISSIONS for realm: " + realmUID); //$NON-NLS-1$ 
        } finally {
            close(statement);
        }
    }    
    
    /**
     * Remove the specified entry from AUTHREALM 
     * @param realmUID
     * @since 4.3
     */
    protected void removeRealm(Number realmUID) throws SQLException {
        PreparedStatement statement = null;
        String sql = JDBCNames.DELETE_REALM;
        sql = sql.toUpperCase();

        try {
            statement = jdbcConnection.prepareStatement(sql);
            LogManager.logTrace(LogConstants.CTX_AUTHORIZATION, new Object[] {
                "removeRealm(", realmUID, ")", "SQL: ", sql}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

            statement.setLong(1, realmUID.longValue());
            statement.execute();

            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "Removed AUTHREALM: " + realmUID); //$NON-NLS-1$ 
        } finally {
            close(statement);
        }
    }
    
    /**
     * Remove entries from AUTHPRINCIPALS for the specified realm
     * @param realmUID
     * @since 4.3
     */
    protected void removePrincipalsForRealm(AuthorizationRealm realm) throws SQLException {
        
        String policyName = "%" + AuthorizationPolicyID.parseRealm(realm);   //$NON-NLS-1$
        
        PreparedStatement statement = null;
        String sql = JDBCNames.DELETE_PRINCIPALS_FOR_REALM;
        sql = sql.toUpperCase();
        

        try {
            statement = jdbcConnection.prepareStatement(sql);
            LogManager.logTrace(LogConstants.CTX_AUTHORIZATION, new Object[] {
                "removePrincipalsForRealm(", realm, ")", "SQL: ", sql}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

            statement.setString(1, policyName);
            statement.execute();

            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "Removed AUTHPRINCIPALS for realm: " + realm); //$NON-NLS-1$ 
        } finally {
            close(statement);
        }
    }

    /**
     * Remove entries from AUTHPOLICIES for the specified realm
     * @param realmUID
     * @since 4.3
     */
    protected void removePoliciesForRealm(AuthorizationRealm realm) throws SQLException {
        
        String policyName = "%" + AuthorizationPolicyID.parseRealm(realm);   //$NON-NLS-1$
        
        PreparedStatement statement = null;
        String sql = JDBCNames.DELETE_POLICIES_FOR_REALM;
        sql = sql.toUpperCase();

        try {
            statement = jdbcConnection.prepareStatement(sql);
            LogManager.logTrace(LogConstants.CTX_AUTHORIZATION, new Object[] {
                "removePoliciesForRealm(", realm, ")", "SQL: ", sql}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

            statement.setString(1, policyName);
            statement.execute();

            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, "Removed AUTHPOLICIES for realm: " + realm); //$NON-NLS-1$ 
        } finally {
            close(statement);
        }
    }
    
    


   private void addPolicyIntoAuthPolicies(AuthorizationPolicy policy)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException, SQLException {
      //  DatabaseTable table = JDBCNames.AuthPolicyUpdateView.TABLE;

        if (containsPolicy(policy.getAuthorizationPolicyID())) {
            throw new AuthorizationSourceConnectionException(ErrorMessageKeys.SEC_AUTHORIZATION_0115,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0115, policy.getAuthorizationPolicyID()));
        }

      PreparedStatement statement = null;

      String sql = JDBCNames.INSERT_POLICY;
      sql = sql.toUpperCase();
      String nameValue = policy.getAuthorizationPolicyID().getName();

     try{
         long policyUID = DBIDGenerator.getInstance().getID(JDBCNames.AuthPolicies.TABLE_NAME);
         String description = policy.getDescription();
         if( description == null){
             description = DEFALT_POLICY_DESCRIPTION;
         }

         statement = jdbcConnection.prepareStatement(sql);

         statement.setLong(1, policyUID);
         statement.setString(2, nameValue);
         statement.setString(3, description);


          statement.execute();
         LogManager.logDetail( LogConstants.CTX_AUTHORIZATION, "Inserted policy " + nameValue); //$NON-NLS-1$ 
     } catch ( DBIDGeneratorException e ) {
         String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0116, nameValue);
         throw new AuthorizationSourceConnectionException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0116, msg);

      } finally {
         if (statement != null) {
              try {
                   statement.close();
                   statement = null;
              } catch ( SQLException se ) {
                   I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
              }
          }
        }

 }

    /**
     * Get the Database UID for the given policID.
     */
    protected Number getPolicyUID(AuthorizationPolicyID policyID )
    throws AuthorizationSourceException, SQLException {
        if( policyID == null){
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0096));
        }
        String policyName = policyID.getName();
        PreparedStatement statement = null;
        // select policyID from AuthPolicies where name like ?
        String sql = JDBCNames.SELECT_POLICYUID_FOR_POLICY_NAME;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "getPolicyUID(", policyID, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] OUT =  new Object[] { "getPolicyUID(", policyID, ")", "SQL: ", sql };
//System.out.println(parseObjArray(OUT));

        try{
            statement = jdbcConnection.prepareStatement(sql);

            statement.setString(1, policyName);

            ResultSet results = statement.executeQuery();
            if(results.next()){
                long uid = results.getLong(JDBCNames.AuthPolicies.ColumnName.POLICY_UID);
                return new Long(uid);
            }

            return null;
        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0097,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0097, policyID));
        } finally {
            if( statement != null){
                try {
                    statement.close();
                    statement = null;
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }
    }

    /**
     * Update the authorization realm.
     */
    private Number updateAuthorizationRealm(AuthorizationPermission perm)
    throws AuthorizationSourceException {
        Number realmUID = getRealmUID(perm);
        if (realmUID == null) {
            realmUID = insertRealm(perm);
        }

        return realmUID;
    }

    
    
  private Number getRealmUID(AuthorizationPermission perm) throws AuthorizationSourceException {
        return getRealmUID(perm.getRealm());

  }
  
  protected Number getRealmUID(AuthorizationRealm realm) throws AuthorizationSourceException {
      PreparedStatement statement = null;

      String sql = JDBCNames.SELECT_AUTH_REALM_BY_NAME;
      sql = sql.toUpperCase();
      LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
          new Object[] { "getRealmUID()", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$
    
      try {
          statement = jdbcConnection.prepareStatement(sql);
    
          statement.setString(1, realm.getRealmName());
    
          ResultSet results = statement.executeQuery();
          if(results.next()){
              long uid = results.getLong(JDBCNames.AuthRealms.ColumnName.REALM_UID);
              return new Long(uid);
          }
          return null;
      } catch ( SQLException e ) {
          throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0092,
                  PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0092));
      } finally {
          close(statement);
      }    
  }
  

  private Number insertRealm(AuthorizationPermission perm)
        throws AuthorizationSourceException {
        AuthorizationRealm realm = perm.getRealm();
      Number realmUID = null;
      PreparedStatement statement = null;
      String sql = null;
       try{
          realmUID = new Long(DBIDGenerator.getInstance().getID(JDBCNames.AuthRealms.TABLE_NAME));

          sql = JDBCNames.INSERT_AUTH_REALM;
          statement = jdbcConnection.prepareStatement(sql);
          statement.setLong(1, realmUID.longValue());
          statement.setString(2, realm.getRealmName());
          statement.setString(3, realm.getDescription());
          statement.execute();
//          if (!statement.execute()){
//              throw new AuthorizationSourceException(ErrorMessageKeys.SEC_AUTHORIZATION_0119,
//                      PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0119, realm));
//          }
      } catch ( DBIDGeneratorException e ) {
          String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0121, realm);
          throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0121, msg);
      }catch (SQLException se){
          throw new AuthorizationSourceException(se, ErrorMessageKeys.SEC_AUTHORIZATION_0119,
                   PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0119, realm));
      }finally {
          if ( statement != null ) {
              try {
                  statement.close();
              } catch ( SQLException e ) {
                  I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0121,
                          e);
              }

        }
      }
      return realmUID;
  }

  private void updateAuthPolicy(Number policyUID, String description)
        throws SQLException, AuthorizationSourceException {

      PreparedStatement statement = null;
      String sql = null;
      try{
          sql = JDBCNames.UPDATE_AUTH_POLICY;
          statement = jdbcConnection.prepareStatement(sql);
          statement.setString(1, description);
          statement.setLong(2, policyUID.longValue());

//          Object[] OUT =  new Object[] { "updateAuthPolicy(", policyUID, ")", "SQL: ", sql };
//          System.out.println(parseObjArray(OUT));

          statement.execute();
//          if (!statement.execute()){
//              System.out.println("*** ERROR UPDATINT POLICY - UD" + policyUID.longValue());
//
//            throw new AuthorizationSourceException(ErrorMessageKeys.SEC_AUTHORIZATION_0106,
//                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0106, policyUID));
//          }
      }finally {
          if ( statement != null ) {
              try {
                  statement.close();
              } catch ( SQLException e ) {
                  I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0121,
                          e);
              }
          }
      }

 }

   

    
    /**
     * This method generates execution-ready preparedStatment for a mulitiple
     * parameter statement.
     * @param conn the jdbc connection instance.
     * @param sql SQL string.
     * @param args The List of String args to insert.
     * @throws SQLException when database error happens.
     * @return preparedStatement instance.
     */
    private PreparedStatement prepareStatement(Connection conn, String sql, List args)
    throws SQLException {
        PreparedStatement statement = conn.prepareStatement(sql);
        for ( int i=1; i<=args.size(); i++ ) {
            String arg = (String) args.get((i-1));
            if( arg != null ){
                statement.setString(i, arg);
            }
        }
        return statement;
    }


    /**
     * Get the authorization permission type UID.
     */
    private Number getPermissionTypeUID(AuthorizationPermission perm)
    throws AuthorizationSourceException {
        Number result = null;
        PreparedStatement statement = null;
        String factoryClassName = perm.getFactoryClassName();
        try{

            statement = jdbcConnection.prepareStatement(JDBCNames.SELECT_AUTH_PERMISSION_TYPE_UID_FOR_FACTORY_CLASS);
            statement.setString(1, factoryClassName);

            ResultSet results = statement.executeQuery();
            if ( results.next() ) {
                result = new Integer(results.getInt(1));
            }
        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0118,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0118, perm));
        } finally {
            if( statement != null ) {
                try {
                    statement.close();
                    statement = null;
                } catch(SQLException se) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }

        return result;
    }

    public void executeBatch(String sql, List paramData) throws SQLException {


        PreparedStatement prepStatement = null;
        List data = new ArrayList(0);
            /*
             * in order to really take advantage of statement batching the batch must be executed
             * in its own transaction.
             */
        try {
            prepStatement = jdbcConnection.prepareStatement(sql);

            final Iterator iter = paramData.iterator();
            while (iter.hasNext()) {


                data = (List)iter.next();

                int i = 0;

                final Iterator iterator = data.iterator();
                while (iterator.hasNext()) {

                     final Object dataValue = iter.next();

                         /*
                         * parameters indexes on a prepared statement start with 1....
                         */
                        i++;
                    prepStatement.setObject(i, dataValue);
    //                    if (dataValue instanceof String) {
    //                        prepStatement.setObject(i, dataValue, Types.LONGVARCHAR);
    //                    } else if (dataValue instanceof Integer) {
    //                    } else if (dataValue instanceof Integer) {
    //                        prepStatement.setObject(i, dataValue);//

                    prepStatement.execute();
                    prepStatement.clearParameters();

                }
            }


        } finally {
            /*
             * here we say that this statement was executed...We should always do this whether the
             * execution succeeded or not. This will allow any StatusListeners to realize that all processing
             * is complete for a particular request when the number of statements added for that
             * request equals the number of statements executed.
             */
            try {
                if (prepStatement != null) {
                    prepStatement.close();
                }
            } catch (SQLException se) {
                I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);

            }

        }
    }

    private void executeBatchAddPermissions(Number policyUID, AuthorizationPermissions permissions)
        throws SQLException, AuthorizationSourceException {



        PreparedStatement prepStatement = null;
 //       List data = new ArrayList(0);
            /*
             * in order to really take advantage of statement batching the batch must be executed
             * in its own transaction.
             */
            Number permissionTypeUID = null;
            Number realmUID = null;
            AuthorizationPermission perm = null;
        try {


            prepStatement = jdbcConnection.prepareStatement(JDBCNames.INSERT_PERMISSION);

            boolean first = true;


            final Iterator iter = permissions.iterator();
            while (iter.hasNext()) {

                perm = (AuthorizationPermission) iter.next();

//              // Add realm if not already present
                if (first) {
                    permissionTypeUID = getPermissionTypeUID(perm);
                    realmUID = updateAuthorizationRealm(perm);
                    first = false;

                }

                  long permUID = DBIDGenerator.getInstance().getID(JDBCNames.AuthPermissions.TABLE_NAME);


                prepStatement.setLong(1, policyUID.longValue());
                prepStatement.setLong(2, realmUID.longValue());
                prepStatement.setLong(3, permUID);
                prepStatement.setLong(4, permissionTypeUID.longValue());

                  // vah - 8-17-03 DB2 driver does not handle
                  // the conversion of boolean (true or false) to
                  // char (1 or 0) like the oracle driver.
                prepStatement.setString(5, perm.getResourceName());

                prepStatement.setString(6, perm.getContentModifier());
                prepStatement.setInt(7, perm.getActions().getValue());


                prepStatement.execute();
                prepStatement.clearParameters();


            }


        } catch ( DBIDGeneratorException e ) {
            String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0108, perm.getResourceName(), perm.getRealmName());
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0111, msg);

        } finally {
            /*
             * here we say that this statement was executed...We should always do this whether the
             * execution succeeded or not. This will allow any StatusListeners to realize that all processing
             * is complete for a particular request when the number of statements added for that
             * request equals the number of statements executed.
             */
            try {
                if (prepStatement != null) {
                    prepStatement.close();
                }
            } catch (SQLException se) {
                I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
            }

        }
    }


    private void executeBatchRemovePermissions(Number policyUID, Iterator authPermIter)
        throws SQLException, AuthorizationSourceException {

        PreparedStatement prepStatement = null;
            AuthorizationPermission perm = null;
        try {


            prepStatement = jdbcConnection.prepareStatement(JDBCNames.DELETE_PERMISSION.toUpperCase());
//            Number permTypeUID = null;

//            boolean first = true;


            while (authPermIter.hasNext()) {

                perm = (AuthorizationPermission) authPermIter.next();

//                if (first) {
//                    permTypeUID = this.getPermissionTypeUID(perm);
//                    first = false;
//                }


                prepStatement.setLong(1, policyUID.longValue());
                prepStatement.setString(2, perm.getResourceName());
//                prepStatement.setLong(3, perm.getActions().getValue());
//                prepStatement.setLong(4, permTypeUID.longValue());

                prepStatement.execute();
                prepStatement.clearParameters();


            }


        } finally {
            /*
             * here we say that this statement was executed...We should always do this whether the
             * execution succeeded or not. This will allow any StatusListeners to realize that all processing
             * is complete for a particular request when the number of statements added for that
             * request equals the number of statements executed.
             */
            try {
                if (prepStatement != null) {
                    prepStatement.close();
                }
            } catch (SQLException se) {
                I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
            }

        }
    }

    

    private void close(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
                statement = null;
            } catch (SQLException se) {
                I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085, se);
            }
        }
    }
        
}


