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

import com.metamatrix.common.jdbc.JDBCReservedWords;

public class JDBCNames {

    private static final String INSERT      = JDBCReservedWords.INSERT + " ";//$NON-NLS-1$
    private static final String UPDATE      = JDBCReservedWords.UPDATE + " ";//$NON-NLS-1$
    private static final String DELETE = JDBCReservedWords.DELETE + " "; //$NON-NLS-1$
    private static final String SELECT = JDBCReservedWords.SELECT + " "; //$NON-NLS-1$
    private static final String FROM = " " + JDBCReservedWords.FROM + " "; //$NON-NLS-1$ //$NON-NLS-2$
    private static final String WHERE = " " + JDBCReservedWords.WHERE + " "; //$NON-NLS-1$ //$NON-NLS-2$
//    private static final String ORDER_BY    = " " + JDBCReservedWords.ORDER_BY + " ";
    private static final String SET         = " " + JDBCReservedWords.SET + " ";//$NON-NLS-1$ //$NON-NLS-2$
//    private static final String ON          = " " + JDBCReservedWords.ON + " ";
    private static final String INTO        = " " + JDBCReservedWords.INTO + " ";//$NON-NLS-1$ //$NON-NLS-2$
    private static final String DISTINCT = " " + JDBCReservedWords.DISTINCT + " "; //$NON-NLS-1$ //$NON-NLS-2$
    private static final String VALUES      = " " + JDBCReservedWords.VALUES + " ";//$NON-NLS-1$ //$NON-NLS-2$
    private static final String AND = " " + JDBCReservedWords.AND + " "; //$NON-NLS-1$ //$NON-NLS-2$
    private static final String LIKE = " " + JDBCReservedWords.LIKE + " "; //$NON-NLS-1$ //$NON-NLS-2$
    private static final String IN = " " + JDBCReservedWords.IN + " "; //$NON-NLS-1$ //$NON-NLS-2$
//    private static final String ALL_COLS  = " " + JDBCReservedWords.ALL_COLS + " ";
//    private static final String PERIOD = ".";//$NON-NLS-1$
 //   private static final String BLANK = " ";//$NON-NLS-1$

    /**
     * The Table definition for the Authorization Permission Types.
     */
    public static class AuthPermTypes {
        public static final String TABLE_NAME = "AUTHPERMTYPES"; //$NON-NLS-1$

        public static class ColumnName {
            public static final String PERM_TYPE_UID = "PERMTYPEUID"; //$NON-NLS-1$
            public static final String DISPLAY_NAME = "DISPLAYNAME"; //$NON-NLS-1$
            public static final String PERMISSION_CODE = "PERMISSIONCODE"; //$NON-NLS-1$
            public static final String FACTORY_CLASS_NAME = "FACTORYCLASSNAME"; //$NON-NLS-1$
        }
    }

    /**
     * The Table definition for the Authorization Permissions.
     */
    public static class AuthPermissions {
        public static final String TABLE_NAME = "AUTHPERMISSIONS"; //$NON-NLS-1$

        public static class ColumnName {
            public static final String PERMISSION_UID = "PERMISSIONUID"; //$NON-NLS-1$
            public static final String POLICY_UID = "POLICYUID"; //$NON-NLS-1$
            public static final String RESOURCE_NAME = "RESOURCENAME"; //$NON-NLS-1$
            public static final String REALM_UID = "REALMUID"; //$NON-NLS-1$
            public static final String ACTIONS = "ACTIONS"; //$NON-NLS-1$
            public static final String CONTENT_MODIFIER = "CONTENTMODIFIER"; //$NON-NLS-1$
            public static final String PERMISSION_TYPE_UID = "PERMTYPEUID"; //$NON-NLS-1$
        }
    }

    /**
     * The Table definition for the Authorization Realms.
     */
    public static class AuthRealms {
        public static final String TABLE_NAME = "AUTHREALMS"; //$NON-NLS-1$

        public static class ColumnName {
            public static final String REALM_UID = "REALMUID"; //$NON-NLS-1$
            public static final String REALM_NAME = "REALMNAME"; //$NON-NLS-1$
            public static final String DESCRIPTION = "DESCRIPTION"; //$NON-NLS-1$
        }
    }

    /**
     * The Table definition for the Authorization Principals.
     */
    public static class AuthPrincipals {
        public static final String TABLE_NAME = "AUTHPRINCIPALS"; //$NON-NLS-1$

        public static class ColumnName {
            public static final String POLICY_UID = "POLICYUID"; //$NON-NLS-1$
            public static final String PRINCIPAL_NAME = "PRINCIPALNAME"; //$NON-NLS-1$
            public static final String PRINCIPAL_TYPE = "PRINCIPALTYPE"; //$NON-NLS-1$
            public static final String GRANTOR = "GRANTOR"; //$NON-NLS-1$
        }
    }

    /**
     * The Table definition for the Authorization Policies.
     */
    public static class AuthPolicies {
        public static final String TABLE_NAME = "AUTHPOLICIES"; //$NON-NLS-1$

        public static class ColumnName {
            public static final String POLICY_UID = "POLICYUID"; //$NON-NLS-1$
            public static final String POLICY_NAME = "POLICYNAME"; //$NON-NLS-1$
            public static final String DESCRIPTION = "DESCRIPTION"; //$NON-NLS-1$
        }
    }


//==============================================================================
// SQL Statements Used by JDBCAuthorizationTransaction
//==============================================================================

    public static final String SELECT_POLICYUID_FOR_POLICY_NAME
            = SELECT
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPolicies.TABLE_NAME
            + WHERE
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + " = ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_AUTH_PERMISSION_TYPE_UID_FOR_FACTORY_CLASS
            = SELECT
            + JDBCNames.AuthPermTypes.TABLE_NAME + "." + JDBCNames.AuthPermTypes.ColumnName.PERM_TYPE_UID //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPermTypes.TABLE_NAME
            + WHERE
            + JDBCNames.AuthPermTypes.ColumnName.FACTORY_CLASS_NAME + " = ? "; //$NON-NLS-1$

    public static final String SELECT_ALL_POLICIES
            = SELECT
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.DESCRIPTION //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPolicies.TABLE_NAME;

    public static final String SELECT_ALL_REALM_NAMES
            = SELECT
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthRealms.TABLE_NAME;

    public static final String SELECT_POLICYID_FOR_NAME
            = SELECT
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.DESCRIPTION //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPolicies.TABLE_NAME
            + WHERE
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + " = ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_POLICY_NAMES_FOR_PRINCIPALS_IN_REALM
            = SELECT
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.DESCRIPTION //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPolicies.TABLE_NAME
            + WHERE
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + IN
            + "(" + SELECT //$NON-NLS-1$
                + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.POLICY_UID //$NON-NLS-1$
                + FROM
                + JDBCNames.AuthPrincipals.TABLE_NAME
                + WHERE
                + "lower(" + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.PRINCIPAL_NAME + ") = ?" //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                + AND
                + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.POLICY_UID //$NON-NLS-1$
                + IN
                + "(" + SELECT //$NON-NLS-1$
                    + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.POLICY_UID //$NON-NLS-1$
                    + FROM
                    + JDBCNames.AuthPermissions.TABLE_NAME
                    + WHERE
                    + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID //$NON-NLS-1$
                    + IN
                        + "(" + SELECT //$NON-NLS-1$
                        + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
                        + FROM
                        + JDBCNames.AuthRealms.TABLE_NAME
                        + WHERE
                        +"lower(" + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + ") = ?)))"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

    public static final String SELECT_POLICY_NAMES_FOR_PRINCIPALS
            = SELECT
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.DESCRIPTION //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPolicies.TABLE_NAME + ", " + JDBCNames.AuthPrincipals.TABLE_NAME //$NON-NLS-1$
            + WHERE
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.PRINCIPAL_NAME + " = ? "; //$NON-NLS-1$ //$NON-NLS-2$


    // Was view
    public static final String SELECT_PRINCIPALS_FOR_POLICY_NAME
            = SELECT
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.PRINCIPAL_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.PRINCIPAL_TYPE //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPrincipals.TABLE_NAME + ", " + JDBCNames.AuthPolicies.TABLE_NAME //$NON-NLS-1$
            + WHERE
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + " = ? "; //$NON-NLS-1$ //$NON-NLS-2$



    public static final String SELECT_PERMISSIONS_FOR_POLICY_NAME
            = SELECT
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.DESCRIPTION + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.ACTIONS + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.CONTENT_MODIFIER + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPermTypes.TABLE_NAME + "." + JDBCNames.AuthPermTypes.ColumnName.FACTORY_CLASS_NAME //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME + ", " + JDBCNames.AuthPermTypes.TABLE_NAME + ", " + JDBCNames.AuthPolicies.TABLE_NAME //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            + WHERE
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.PERMISSION_TYPE_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPermTypes.TABLE_NAME + "." + JDBCNames.AuthPermTypes.ColumnName.PERM_TYPE_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + " = ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_PRINCIPALS_FOR_ROLE_NAME
            = SELECT
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.PRINCIPAL_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.PRINCIPAL_TYPE //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME + ", " + JDBCNames.AuthPrincipals.TABLE_NAME + ", " + JDBCNames.AuthPolicies.TABLE_NAME //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            + WHERE
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + " = ? " //$NON-NLS-1$ //$NON-NLS-2$
            + AND
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + " = ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_RESOURCE_PRINCIPALS_GRANTOR_AND_ACTIONS_FOR_RESOURCE_IN_REALM
            = SELECT + DISTINCT
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.PRINCIPAL_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.PRINCIPAL_TYPE + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.GRANTOR + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.ACTIONS //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthPrincipals.TABLE_NAME + ", " + JDBCNames.AuthPolicies.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            + WHERE
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + " = ? " //$NON-NLS-1$ //$NON-NLS-2$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME + LIKE + " ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_ROLE_NAMES_FOR_PRINCIPAL_NAME
            = SELECT
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME + ", " + JDBCNames.AuthPrincipals.TABLE_NAME + ", " + JDBCNames.AuthPolicies.TABLE_NAME //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            + WHERE
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + " = ? " //$NON-NLS-1$ //$NON-NLS-2$
            + AND
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.PRINCIPAL_NAME + " = ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_ALL_ROLES_AND_DESCRITPIONS
            = SELECT + DISTINCT
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.DESCRIPTION //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPolicies.TABLE_NAME + ", " + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME //$NON-NLS-1$ //$NON-NLS-2$
            + WHERE
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + " = ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_POLICY_NAMES_WITH_PERMISSIONS_IN_REALM
            = SELECT + DISTINCT
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.DESCRIPTION //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPolicies.TABLE_NAME + ", " + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME //$NON-NLS-1$ //$NON-NLS-2$
            + WHERE
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + " = ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_POLICY_NAMES_FOR_REALM
            = SELECT + DISTINCT
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.DESCRIPTION //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPolicies.TABLE_NAME
            + WHERE
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + LIKE + " ?"; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_POLICY_NAMES_FOR_REALM_STARTS_WITH
            = SELECT + DISTINCT
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.DESCRIPTION //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPolicies.TABLE_NAME + ", " + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME //$NON-NLS-1$ //$NON-NLS-2$
            + WHERE
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + LIKE + " ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_POLICY_NAMES_WITH_RESOURCE_IN_REALM
            = SELECT + DISTINCT
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.DESCRIPTION //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPolicies.TABLE_NAME + ", " + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME //$NON-NLS-1$ //$NON-NLS-2$
            + WHERE
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + " = ? " //$NON-NLS-1$ //$NON-NLS-2$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME + " = ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_PERMISSION_IDS_WITH_RESOURCE_IN_REALM
            = SELECT + DISTINCT
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.PERMISSION_UID //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME //$NON-NLS-1$
            + WHERE
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + " = ? " //$NON-NLS-1$ //$NON-NLS-2$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME + " = ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_ACTIONS_PERM_FACTORY_AND_POLICYNAME_FOR_RESOURCE_IN_REALM
            = SELECT
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.ACTIONS + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPermTypes.TABLE_NAME + "." + JDBCNames.AuthPermTypes.ColumnName.FACTORY_CLASS_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + ", " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.DESCRIPTION //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthPolicies.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME + ", " + JDBCNames.AuthPermTypes.TABLE_NAME //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            + WHERE
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.PERMISSION_TYPE_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPermTypes.TABLE_NAME + "." + JDBCNames.AuthPermTypes.ColumnName.PERM_TYPE_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + " = ? " //$NON-NLS-1$ //$NON-NLS-2$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME + " = ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_DEPENDANT_RESOURCES_FOR_RESOURCE_IN_REALM
            = SELECT
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME //$NON-NLS-1$
            + WHERE
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + " = ? " //$NON-NLS-1$ //$NON-NLS-2$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME + LIKE + " ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_ACTIONS_FOR_RESOURCE_IN_REALM_FOR_POLICY
            = SELECT + DISTINCT
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.ACTIONS //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthPolicies.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME //$NON-NLS-1$ //$NON-NLS-2$
            + WHERE
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.POLICY_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPolicies.TABLE_NAME + "." + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + " = ? " //$NON-NLS-1$ //$NON-NLS-2$
            + AND
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + " = ? " //$NON-NLS-1$ //$NON-NLS-2$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME + " = ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String SELECT_PERMISSIONS_IN_REALM_FOR_RESOURCE_STARTS_WITH
            = SELECT
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME //$NON-NLS-1$
            + WHERE
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + " = ? " //$NON-NLS-1$ //$NON-NLS-2$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME + LIKE + " ? "; //$NON-NLS-1$ //$NON-NLS-2$

    public static final String DELETE_PERMISSIONS_WITH_RESOURCES_IN
            = DELETE
            + FROM
            + JDBCNames.AuthPermissions.TABLE_NAME
            + WHERE
            + JDBCNames.AuthPermissions.ColumnName.PERMISSION_UID
            + IN + "(" //$NON-NLS-1$
            + SELECT
            + JDBCNames.AuthPermissions.ColumnName.PERMISSION_UID
            + FROM
            + JDBCNames.AuthPermissions.TABLE_NAME + ", " + JDBCNames.AuthRealms.TABLE_NAME //$NON-NLS-1$
            + WHERE
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = " //$NON-NLS-1$ //$NON-NLS-2$
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + AND
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_NAME + " = ? " //$NON-NLS-1$ //$NON-NLS-2$
            + AND
            + JDBCNames.AuthPermissions.TABLE_NAME + "." + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME //$NON-NLS-1$
            + IN + " "; //$NON-NLS-1$

    public static final String DELETE_PERMISSION
            = DELETE
            + FROM
            + JDBCNames.AuthPermissions.TABLE_NAME
            + WHERE
            + JDBCNames.AuthPermissions.ColumnName.POLICY_UID + " = ? " //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME + " = ? " //$NON-NLS-1$
//            + AND
//            + JDBCNames.AuthPermissions.ColumnName.ACTIONS + " = ? " //$NON-NLS-1$
//            + AND
//            + JDBCNames.AuthPermissions.ColumnName.PERMISSION_TYPE_UID + " = ? " //$NON-NLS-1$
            ;


    public static final String DELETE_ALL_PERMISSIONS_FOR_POLICY
            = DELETE
            + FROM
            + JDBCNames.AuthPermissions.TABLE_NAME
            + WHERE
            + JDBCNames.AuthPermissions.ColumnName.POLICY_UID + " = ? " //$NON-NLS-1$
            ;

    public static final String INSERT_PRINCIPAL
                                    = INSERT + INTO + JDBCNames.AuthPrincipals.TABLE_NAME + "(" //$NON-NLS-1$
                                        + JDBCNames.AuthPrincipals.ColumnName.POLICY_UID + "," //$NON-NLS-1$
                                        + JDBCNames.AuthPrincipals.ColumnName.PRINCIPAL_NAME + "," //$NON-NLS-1$
                                        + JDBCNames.AuthPrincipals.ColumnName.PRINCIPAL_TYPE + "," //$NON-NLS-1$
                                        + JDBCNames.AuthPrincipals.ColumnName.GRANTOR +  ")" //$NON-NLS-1$
                                        + VALUES
                                        + "(?,?,?,?)"; //$NON-NLS-1$

    public static final String DELETE_PRINCIPAL_FROM_ALL_POLICIES
            = DELETE
            + FROM
            + JDBCNames.AuthPrincipals.TABLE_NAME  
            + WHERE
            + JDBCNames.AuthPrincipals.TABLE_NAME + "." + JDBCNames.AuthPrincipals.ColumnName.PRINCIPAL_NAME + " = ? " //$NON-NLS-1$ //$NON-NLS-2$
            ; 



    public static final String INSERT_PERMISSION
                                    = INSERT + INTO + JDBCNames.AuthPermissions.TABLE_NAME + "(" //$NON-NLS-1$
                                        + JDBCNames.AuthPermissions.ColumnName.POLICY_UID + "," //$NON-NLS-1$
                                        + JDBCNames.AuthPermissions.ColumnName.REALM_UID + "," //$NON-NLS-1$
                                        + JDBCNames.AuthPermissions.ColumnName.PERMISSION_UID + "," //$NON-NLS-1$
                                        + JDBCNames.AuthPermissions.ColumnName.PERMISSION_TYPE_UID + "," //$NON-NLS-1$
                                        + JDBCNames.AuthPermissions.ColumnName.RESOURCE_NAME + "," //$NON-NLS-1$
                                        + JDBCNames.AuthPermissions.ColumnName.CONTENT_MODIFIER + "," //$NON-NLS-1$
                                        + JDBCNames.AuthPermissions.ColumnName.ACTIONS +  ")" //$NON-NLS-1$
                                        + VALUES
                                        + "(?,?,?,?,?,?,?)"; //$NON-NLS-1$


    public static final String DELETE_ALL_PRINCIPALS_FOR_POLICY
            = DELETE
            + FROM
            + JDBCNames.AuthPrincipals.TABLE_NAME
            + WHERE
            + JDBCNames.AuthPrincipals.ColumnName.POLICY_UID + " = ? " //$NON-NLS-1$
            ;


    public static final String DELETE_PRINCIPAL
            = DELETE
            + FROM
            + JDBCNames.AuthPrincipals.TABLE_NAME
            + WHERE
            + JDBCNames.AuthPrincipals.ColumnName.POLICY_UID + " = ? " //$NON-NLS-1$
            + AND
            + JDBCNames.AuthPrincipals.ColumnName.PRINCIPAL_NAME + " = ? " //$NON-NLS-1$
            ;
    

    public static final String INSERT_POLICY
                                    = INSERT + INTO + JDBCNames.AuthPolicies.TABLE_NAME + "(" //$NON-NLS-1$
                                        + JDBCNames.AuthPolicies.ColumnName.POLICY_UID + "," //$NON-NLS-1$
                                        + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + "," //$NON-NLS-1$
                                        + JDBCNames.AuthPolicies.ColumnName.DESCRIPTION +  ")" //$NON-NLS-1$
                                        + VALUES
                                        + "(?,?,?)"; //$NON-NLS-1$


    public static final String DELETE_ALL_POLICIES_FOR_POLICY
            = DELETE
            + FROM
            + JDBCNames.AuthPolicies.TABLE_NAME
            + WHERE
            + JDBCNames.AuthPolicies.ColumnName.POLICY_UID + " = ? " //$NON-NLS-1$
            ;


    public static final String SELECT_AUTH_REALM_BY_NAME
            = SELECT
            + JDBCNames.AuthRealms.TABLE_NAME + "." + JDBCNames.AuthRealms.ColumnName.REALM_UID //$NON-NLS-1$
            + FROM
            + JDBCNames.AuthRealms.TABLE_NAME
            + WHERE
            + JDBCNames.AuthRealms.ColumnName.REALM_NAME + " = ? " //$NON-NLS-1$
            ;

    public static final String INSERT_AUTH_REALM
                                    = INSERT + INTO + AuthRealms.TABLE_NAME + "(" //$NON-NLS-1$
                                        + JDBCNames.AuthRealms.ColumnName.REALM_UID + "," //$NON-NLS-1$
                                        + JDBCNames.AuthRealms.ColumnName.REALM_NAME + "," //$NON-NLS-1$
                                        + JDBCNames.AuthRealms.ColumnName.DESCRIPTION +  ")" //$NON-NLS-1$
                                        + VALUES
                                        + "(?,?,?)"; //$NON-NLS-1$


    public static final String UPDATE_AUTH_POLICY
                                    = UPDATE + JDBCNames.AuthPolicies.TABLE_NAME
                                        + SET
                                        + JDBCNames.AuthPolicies.ColumnName.DESCRIPTION + "=?" //$NON-NLS-1$
                                        + WHERE
                                        + JDBCNames.AuthPolicies.ColumnName.POLICY_UID + "=?" //$NON-NLS-1$
                                        ;
    
    
    
    public static final String DELETE_PERMISSIONS_FOR_REALM = 
        DELETE + FROM
        + JDBCNames.AuthPermissions.TABLE_NAME
        + WHERE
        + JDBCNames.AuthPermissions.ColumnName.REALM_UID + " = ? "; //$NON-NLS-1$
    
    public static final String DELETE_REALM =
        DELETE + FROM
        + JDBCNames.AuthRealms.TABLE_NAME
        + WHERE
        + JDBCNames.AuthRealms.ColumnName.REALM_UID + " = ? "; //$NON-NLS-1$
    
    
    public static final String DELETE_PRINCIPALS_FOR_REALM = 
        DELETE + FROM
        + JDBCNames.AuthPrincipals.TABLE_NAME
        + WHERE
        + JDBCNames.AuthPrincipals.ColumnName.POLICY_UID 
        + IN
        + "(" + SELECT //$NON-NLS-1$
            + JDBCNames.AuthPolicies.ColumnName.POLICY_UID
            + FROM
            + JDBCNames.AuthPolicies.TABLE_NAME
            + WHERE
            + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + LIKE + " ? )"; //$NON-NLS-1$ 
     
    
    public static final String DELETE_POLICIES_FOR_REALM = 
        DELETE + FROM
        + JDBCNames.AuthPolicies.TABLE_NAME
        + WHERE
        + JDBCNames.AuthPolicies.ColumnName.POLICY_NAME + LIKE + " ?"; //$NON-NLS-1$ 
}


