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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.metamatrix.common.log.I18nLogManager;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.security.api.AuthorizationActions;
import com.metamatrix.platform.security.api.AuthorizationPermission;
import com.metamatrix.platform.security.api.AuthorizationPermissionFactory;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.AuthorizationResource;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.StandardAuthorizationActions;
import com.metamatrix.platform.security.authorization.spi.AuthorizationSourceConnectionException;
import com.metamatrix.platform.security.authorization.spi.AuthorizationSourceException;
import com.metamatrix.platform.util.ErrorMessageKeys;


/** 
 * @since 4.2
 */
public class JDBCAuthorizationReader {

    
    /**
     * Returns the Set of roles for the specified principal
     * @param principals <code>MetaMatrixPrincipalName</code>s of a principal and
     * any group memberships for which roles are sought
     * @param jdbcConnection is the Connection to use to read the roles.
     * @return The Set of role for the principal, possibly empty, never null.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public static Set getRoleNamesForPrincipal(MetaMatrixPrincipalName principal, Connection jdbcConnection)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Set pRoles = new HashSet();
        String sql = JDBCNames.SELECT_ROLE_NAMES_FOR_PRINCIPAL_NAME;
        sql = sql.toUpperCase();
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
            new Object[] { "getRoleNamesForPrincipal(", principal, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] OUT =  new Object[] { "getRoleNamesForPrincipal(", principals, ")", "SQL: ", sql };
//System.out.println(parseObjArray(OUT));
        PreparedStatement statement = null;
        try {

            statement = jdbcConnection.prepareStatement(sql);
 
            String roleName = null;

            statement.setString(1, JDBCAuthorizationTransaction.ROLE_REALM);  
            statement.setString(2, principal.getName());  

            ResultSet results = statement.executeQuery();
            while (results.next()) {
                roleName = results.getString(1);
                pRoles.add(roleName);
            }

            statement.clearParameters();
                
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

        return pRoles;
       
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
    public static AuthorizationPolicy getPolicy(AuthorizationPolicyID policyID, Connection jdbcConnection)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        AuthorizationPolicy myPolicy = null;
        AuthorizationPolicyID pID = null;
        Set permissions = Collections.EMPTY_SET;
        Set principals = null;

        PreparedStatement statement = null;
        ResultSet results = null;
        try{
            principals = new HashSet();
            String policyName = policyID.getName();

            // Get new copy of policy ID
            String sql = JDBCNames.SELECT_POLICYID_FOR_NAME;
            sql = sql.toUpperCase();
            LogManager.logDetail( LogConstants.CTX_AUTHORIZATION,
              new Object[] { "getPolicy-1(", policyID, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] OUT2 =  new Object[] { "\ngetPolicy-1(", policyID, ")", sql };
//System.out.println(parseObjArray(OUT2));

            statement = jdbcConnection.prepareStatement(sql);
            statement.setString(1, policyName);

            results = statement.executeQuery();
            String name = null;
            String desc = null;
            if ( results.next() ) {
                name = results.getString(JDBCNames.AuthPolicies.ColumnName.POLICY_NAME);
                desc = results.getString(JDBCNames.AuthPolicies.ColumnName.DESCRIPTION);
            } else {
                throw new AuthorizationSourceException(ErrorMessageKeys.SEC_AUTHORIZATION_0105,
                        PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0105, policyID));
            }
            pID = new AuthorizationPolicyID(name, desc);

            statement.close();


            // Get the policy's principals
            sql = JDBCNames.SELECT_PRINCIPALS_FOR_POLICY_NAME;
            sql = sql.toUpperCase();
            LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
              new Object[] { "getPolicy-2(", policyID, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] OUT2 =  new Object[] { "\ngetPolicy-2(", policyID, ")", sql };
//System.out.println(parseObjArray(OUT2));

          statement = jdbcConnection.prepareStatement(sql);
          statement.setString(1, policyName);

//            statement = prepareStatement(this.jdbcConnection, sql, policyName);
            results = statement.executeQuery();
            while(results.next()){
                String principalName = results.getString(1);
                int principalType = results.getInt(2);
                principals.add(new MetaMatrixPrincipalName(principalName, principalType));
            }
            statement.close();
            results = null;

            // Get permissions belonging to this policy
            permissions = getPermissionsForPolicy(policyID, jdbcConnection);
        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0106,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0106, policyID));
        } finally{
            if( statement != null ){
                try {
                    statement.close();
                  //  statement = null;
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }

        myPolicy = new AuthorizationPolicy(pID, null, permissions);
        myPolicy.addAllPrincipals(principals);
        return myPolicy;
    }
    
    /**
     * Find and create all <code>AuthorizationPermissionsImpl</code> known to a policy.
     * @param policyID The policy indentifier.
     * @return The set of all permissions that belong to the given policy.
     */
    public static Set getPermissionsForPolicy(AuthorizationPolicyID policyID, Connection jdbcConnection)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        Set permissions = new HashSet();
        String policyName = policyID.getName();
        PreparedStatement statement = null;
        String factoryClassName = null;

        try{
            Class factoryClass = null;
            String tempClassName = null;

            // Get the policy's permissions
            AuthorizationPermissionFactory permFactory = null;
            String sql = JDBCNames.SELECT_PERMISSIONS_FOR_POLICY_NAME;
            sql = sql.toUpperCase();
            LogManager.logTrace( LogConstants.CTX_AUTHORIZATION,
              new Object[] { "getPermissionsForPolicy(", policyID, ")", "SQL: ", sql }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
// DEBUG:
//Object[] out =  new Object[] { "\ngetPermissionsForPolicy(", policyID, ")", sql };
//System.out.println(parseObjArray(out));

            statement = jdbcConnection.prepareStatement(sql);
            statement.setString(1, policyName);

            ResultSet results = statement.executeQuery();
            while(results.next()){
                String resourceName = results.getString(JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME);
                String realmName = results.getString(JDBCNames.AuthRealms.ColumnName.REALM_NAME);
                String realmDescription = results.getString(JDBCNames.AuthRealms.ColumnName.DESCRIPTION);
                int actionsValue = results.getInt(JDBCNames.AuthPermissions.ColumnName.ACTIONS);
                // get AuthorizationActions object from the action value
                AuthorizationActions actions = StandardAuthorizationActions.getAuthorizationActions(actionsValue);
                String contentModifier = results.getString(JDBCNames.AuthPermissions.ColumnName.CONTENT_MODIFIER);
                factoryClassName = results.getString(JDBCNames.AuthPermTypes.ColumnName.FACTORY_CLASS_NAME);

                // Save instantiated permFactory until we find another type we have to
                // instaintiate
                if(!factoryClassName.equals(tempClassName)){
                    factoryClass = Class.forName(factoryClassName);
                    permFactory = (AuthorizationPermissionFactory) factoryClass.newInstance();
                    tempClassName = factoryClassName;

                }

                AuthorizationRealm theRealm = new AuthorizationRealm(realmName);
                theRealm.setDescription(realmDescription);

                AuthorizationResource resource = permFactory.createResource(resourceName);
                AuthorizationPermission perm = permFactory.create(resource, theRealm, actions, contentModifier);
                permissions.add(perm);
            }

        } catch ( SQLException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0108,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0108, policyID));
        } catch ( ClassNotFoundException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0082,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0082, factoryClassName));
        } catch ( IllegalAccessException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0083,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0083));
        } catch ( InstantiationException e ) {
            throw new AuthorizationSourceException(e, ErrorMessageKeys.SEC_AUTHORIZATION_0084,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUTHORIZATION_0084, factoryClassName));
        } finally{
            if( statement != null ){
                try {
                    statement.close();
                  //  statement = null;
                } catch ( SQLException se ) {
                    I18nLogManager.logError(LogConstants.CTX_AUTHORIZATION, ErrorMessageKeys.SEC_AUTHORIZATION_0085,se);
                }
            }
        }
        return permissions;
    }

}
