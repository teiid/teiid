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

package com.metamatrix.platform.security.membership.spi.ldap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.naming.AuthenticationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.NamingSecurityException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import com.metamatrix.api.exception.security.InvalidUserException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.api.exception.security.UnsupportedCredentialException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SuccessfulAuthenticationToken;
import com.metamatrix.platform.security.membership.spi.MembershipDomain;
import com.metamatrix.platform.security.membership.spi.MembershipSourceException;

public class LDAPMembershipDomain implements
                                 MembershipDomain {

    public static final String ONELEVEL_SCOPE_VALUE = "ONELEVEL_SCOPE"; //$NON-NLS-1$
    public static final String OBJECT_SCOPE_VALUE = "OBJECT_SCOPE"; //$NON-NLS-1$
    public static final String SUBTREE_SCOPE_VALUE = "SUBTREE_SCOPE"; //$NON-NLS-1$
    // properties
    public static final String GROUPS_GROUP_MEMBER_ATTRIBUTE = "groups.groupMember.attribute"; //$NON-NLS-1$
    public static final String GROUPS_ROOT_CONTEXT = "groups.rootContext"; //$NON-NLS-1$
    public static final String GROUPS_SEARCH_SCOPE = "groups.searchScope"; //$NON-NLS-1$
    public static final String GROUPS_SEARCH_FILTER = "groups.searchFilter"; //$NON-NLS-1$
    public static final String GROUPS_DISPLAY_NAME_ATTRIBUTE = "groups.displayName.attribute"; //$NON-NLS-1$
    public static final String USERS_MEMBER_OF_ATTRIBUTE = "users.memberOf.attribute"; //$NON-NLS-1$
    public static final String USERS_ROOT_CONTEXT = "users.rootContext"; //$NON-NLS-1$
    public static final String USERS_SEARCH_SCOPE = "users.searchScope"; //$NON-NLS-1$
    public static final String USERS_DISPLAY_NAME_ATTRIBUTE = "users.displayName.attribute"; //$NON-NLS-1$
    public static final String USERS_SEARCH_FILTER = "users.searchFilter"; //$NON-NLS-1$
    public static final String LDAP_URL = "ldapURL"; //$NON-NLS-1$
    public static final String LDAP_ADMIN_PASSWORD = "ldapAdmin.password"; //$NON-NLS-1$
    public static final String LDAP_ADMIN_DN = "ldapAdmin.dn"; //$NON-NLS-1$
    public static final String TXN_TIMEOUT_IN_MILLIS = "txnTimeoutInMillis"; //$NON-NLS-1$

    // default property values
    public static final String LDAP_INITIAL_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory"; //$NON-NLS-1$
    public static final String LDAP_AUTH_TYPE = "simple"; //$NON-NLS-1$
    public static final String LDAP_USER_OBJECT_TYPE = "person"; //$NON-NLS-1$
    public static final String LDAP_REFERRAL_MODE = "follow"; //$NON-NLS-1$
    public static final String DEFAULT_SEARCH_FILTER = "(objectclass=*)"; //$NON-NLS-1$
    public static final String POOL_KEY = "com.sun.jndi.ldap.connect.pool"; //$NON-NLS-1$
    public static final String TIMEOUT_KEY = "com.sun.jndi.ldap.connect.timeout"; //$NON-NLS-1$
    public static final String DEFAULT_USERS_DISPLAY_NAME_ATTRIBUTE = "uid"; //$NON-NLS-1$
    public static final String DEFAULT_GROUPS_DISPLAY_NAME_ATTRIBUTE = "cn"; //$NON-NLS-1$

    static class UserEntry {

        private String dn;
        private Set groups;

        public UserEntry(String dn,
                         Set groups) {
            this.dn = dn;
            this.groups = groups;
        }

        public String getDn() {
            return this.dn;
        }

        public Set getGroups() {
            return this.groups;
        }

    }
    
    static class LdapContext {
        String context;
        String searchFilter = DEFAULT_SEARCH_FILTER;
        String displayAttribute;
        String memberOfAttribute;
        int searchScope = SearchControls.SUBTREE_SCOPE;
    }

    private String domainName;
    private String ldapURL;
    private String ldapAdminUserDN;
    private String ldapAdminUserPass;
    private String ldapTxnTimeoutInMillis;
    
    private List usersRootContexts;
    private List groupsRootContexts;

    private Hashtable adminContext = new Hashtable();

    public void initialize(Properties props) throws MembershipSourceException {
        this.domainName = props.getProperty(MembershipServiceInterface.DOMAIN_NAME);

        LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, "Initializing LDAP Domain: " + domainName); //$NON-NLS-1$

        ldapTxnTimeoutInMillis = props.getProperty(TXN_TIMEOUT_IN_MILLIS);

        ldapAdminUserDN = getPropertyValue(props, LDAP_ADMIN_DN, null);

        ldapAdminUserPass = getPropertyValue(props, LDAP_ADMIN_PASSWORD, null);
        
        ldapURL = getPropertyValue(props, LDAP_URL, null);
        if (ldapURL == null) {
            throw new MembershipSourceException(DQPEmbeddedPlugin.Util.getString("LDAPMembershipDomain.Required_property", LDAP_URL)); //$NON-NLS-1$
        }

        usersRootContexts = buildContexts(USERS_ROOT_CONTEXT,
                                          USERS_SEARCH_FILTER,
                                          USERS_DISPLAY_NAME_ATTRIBUTE,
                                          USERS_SEARCH_SCOPE,
                                          USERS_MEMBER_OF_ATTRIBUTE,
                                          DEFAULT_USERS_DISPLAY_NAME_ATTRIBUTE,
                                          props);
        
        groupsRootContexts = buildContexts(GROUPS_ROOT_CONTEXT,
                                           GROUPS_SEARCH_FILTER,
                                           GROUPS_DISPLAY_NAME_ATTRIBUTE,
                                           GROUPS_SEARCH_SCOPE,
                                           GROUPS_GROUP_MEMBER_ATTRIBUTE,
                                           DEFAULT_GROUPS_DISPLAY_NAME_ATTRIBUTE,
                                           props);
        
        if (props.getProperty(USERS_MEMBER_OF_ATTRIBUTE, "").trim().length() == 0 //$NON-NLS-1$ 
                        && props.getProperty(GROUPS_GROUP_MEMBER_ATTRIBUTE, "").trim().length() == 0) { //$NON-NLS-1$
        	LogManager.logWarning(LogConstants.CTX_MEMBERSHIP, DQPEmbeddedPlugin.Util.getString("LDAPMembershipDomain.Require_memberof_property", domainName ) ); //$NON-NLS-1$
        }
        
        // Create the root context.
        adminContext.put(Context.INITIAL_CONTEXT_FACTORY, LDAP_INITIAL_CONTEXT_FACTORY);
        adminContext.put(Context.PROVIDER_URL, this.ldapURL);
        adminContext.put(Context.REFERRAL, LDAP_REFERRAL_MODE);
        adminContext.put(POOL_KEY, Boolean.TRUE.toString());

        // If password is blank, we will perform an anonymous bind.
        if (ldapAdminUserDN != null && ldapAdminUserPass != null) {
            LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, domainName + ": Username was set to:" + ldapAdminUserDN); //$NON-NLS-1$
            adminContext.put(Context.SECURITY_AUTHENTICATION, LDAP_AUTH_TYPE);
            adminContext.put(Context.SECURITY_PRINCIPAL, this.ldapAdminUserDN);
            adminContext.put(Context.SECURITY_CREDENTIALS, this.ldapAdminUserPass);
        } else {
            LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, domainName
                                                                     + ": admin dn was blank; performing anonymous bind."); //$NON-NLS-1$
            adminContext.put(Context.SECURITY_AUTHENTICATION, "none"); //$NON-NLS-1$
        }

        if (ldapTxnTimeoutInMillis != null) {
            adminContext.put(TIMEOUT_KEY, ldapTxnTimeoutInMillis);
        }
    }

    private List buildContexts(String rootContextsProp,
                               String searchFilterProp,
                               String displayAttributeProp,
                               String searchScopeProp,
                               String memberOfAttributeProp, String defaultDisplayName, Properties props) throws MembershipSourceException {
        
        String rootContextsStr = props.getProperty(rootContextsProp, null);
        if (rootContextsStr == null) {
            throw new MembershipSourceException(DQPEmbeddedPlugin.Util.getString("LDAPMembershipDomain.Required_property", rootContextsProp)); //$NON-NLS-1$
        }
        
        String searchFilterStr = props.getProperty(searchFilterProp);
        String searchScopeStr = props.getProperty(searchScopeProp);
        String memberOfAttributeStr = props.getProperty(memberOfAttributeProp); 
        String displayAttributeStr = props.getProperty(displayAttributeProp); 

        String[] rootContexts = rootContextsStr.split("\\?"); //$NON-NLS-1$
        String[] displayAttributes = (displayAttributeStr != null) ? displayAttributeStr.split("\\?") : null; //$NON-NLS-1$
        String[] searchFilters = (searchFilterStr != null) ? searchFilterStr.split("\\?") : null; //$NON-NLS-1$
        String[] searchScopes = (searchScopeStr != null) ? searchScopeStr.split("\\?") : null; //$NON-NLS-1$
        String[] memberOfAttributes = (memberOfAttributeStr != null) ? memberOfAttributeStr.split("\\?") : null; //$NON-NLS-1$
        
        List results = new ArrayList();
        
        for (int i = 0; i < rootContexts.length; i++) {
            LdapContext context = new LdapContext();
            results.add(context);
            context.context = rootContexts[i];
            
            context.displayAttribute = getContextValue(displayAttributes, i, defaultDisplayName);
            context.memberOfAttribute = getContextValue(memberOfAttributes, i, null);
            
            context.searchFilter = getContextValue(searchFilters, i, context.searchFilter);
            context.searchScope = getSearchScope(getContextValue(searchScopes, i, null));
        }
        
        return results;
    }

    private static String getContextValue(String[] values, int i, String defaultValue) {
        String value = null;
        
        if (values != null) {
            if (values.length > i) {
                value = values[i];
            } else if (values.length == 1){
                value = values[0];
            }
        }
        
        if (value == null || value.trim().length() == 0) {
            value = defaultValue;
        }
        
        return value;
    }    
    
    private static String getPropertyValue(Properties props,
                                           String key,
                                           String defaultValue) {
        String result = props.getProperty(key);
        if (result == null || result.trim().length() == 0) {
            return defaultValue;
        }
        return result.trim();
    }

    private int getSearchScope(String scope) {
        if (scope == null) {
            return SearchControls.SUBTREE_SCOPE;
        }
        if (scope.equals(OBJECT_SCOPE_VALUE)) { 
            return SearchControls.OBJECT_SCOPE;
        }
        if (scope.equals(ONELEVEL_SCOPE_VALUE)) { 
            return SearchControls.ONELEVEL_SCOPE;
        }
        return SearchControls.SUBTREE_SCOPE;
    }

    public void shutdown() {
        LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, domainName + ": shutdown()"); //$NON-NLS-1$
    }

    public SuccessfulAuthenticationToken authenticateUser(String username,
                                                          Credentials credential,
                                                          Serializable trustedPayload,
                                                          String applicationName) throws UnsupportedCredentialException,
                                                                                 InvalidUserException,
                                                                                 LogonException,
                                                                                 MembershipSourceException {

        LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {
            domainName, "authenticateUser username", username, "applicationName", applicationName}); //$NON-NLS-1$ //$NON-NLS-2$
        
        if (username == null) {
            throw new UnsupportedCredentialException(DQPEmbeddedPlugin.Util.getString("LDAPMembershipDomain.No_annonymous", domainName)); //$NON-NLS-1$
        }

        UserEntry ue = getUserEntry(username, false);
        
        if (credential == null) {
            throw new UnsupportedCredentialException(DQPEmbeddedPlugin.Util.getString("LDAPMembershipDomain.No_annonymous", domainName)); //$NON-NLS-1$
        }

        Hashtable connenv = new Hashtable();
        connenv.put(Context.INITIAL_CONTEXT_FACTORY, LDAP_INITIAL_CONTEXT_FACTORY);
        connenv.put(Context.PROVIDER_URL, this.ldapURL);
        connenv.put(Context.SECURITY_AUTHENTICATION, LDAP_AUTH_TYPE);
        connenv.put(Context.SECURITY_PRINCIPAL, ue.getDn());
        connenv.put(Context.SECURITY_CREDENTIALS, String.valueOf(credential.getCredentialsAsCharArray()));

        DirContext ctx = null;
        try {
            ctx = new InitialDirContext(connenv);
        } catch (NamingSecurityException nse) {
            throw new LogonException(nse, nse.getMessage());
        } catch (NamingException ne) {
            throw new MembershipSourceException(ne, ne.getMessage());
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (NamingException ne) {
                    LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, ne, domainName + ": error closing context"); //$NON-NLS-1$
                }
            }
        }
        
        if(credential.getCredentialsAsCharArray().length == 0){
        	username = "";//$NON-NLS-1$
        }

        return new SuccessfulAuthenticationToken(trustedPayload, username);
    }

    public Set getGroupNames() throws MembershipSourceException {
        LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {
            domainName, " getGroupNames() called"}); //$NON-NLS-1$

        DirContext ctx = null;

        try {
            ctx = getAdminContext();
            return new HashSet(getGroupNames(ctx, null, false).values());
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (NamingException ne) {
                    LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, ne, domainName + ": error closing context"); //$NON-NLS-1$
                }
            }
        }
    }
    
    private DirContext getAdminContext() throws MembershipSourceException {
        try {
            return new InitialDirContext((Hashtable)adminContext.clone());
        } catch (AuthenticationException err) {
            throw new MembershipSourceException(err, DQPEmbeddedPlugin.Util.getString("LDAPMembershipDomain.Admin_credentials", domainName)); //$NON-NLS-1$
        } catch (NamingException err) {
            throw new MembershipSourceException(err);
        }
    }

    public Set getGroupNamesForUser(String username) throws InvalidUserException,
                                                    MembershipSourceException {

        LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {domainName, "getGroupNamesForUser", username}); //$NON-NLS-1$ 

        if(username.length() == 0){
        	return Collections.EMPTY_SET;
        }
        
        UserEntry ue = getUserEntry(username, true);

        return ue.getGroups();
    }
    
    public static final String escapeLDAPSearchFilter(String filter) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < filter.length(); i++) {
            char curChar = filter.charAt(i);
            switch (curChar) {
                case '\\':
                    sb.append("\\5c"); //$NON-NLS-1$
                    break;
                case '*':
                    sb.append("\\2a"); //$NON-NLS-1$
                    break;
                case '(':
                    sb.append("\\28"); //$NON-NLS-1$
                    break;
                case ')':
                    sb.append("\\29"); //$NON-NLS-1$
                    break;
                case '\u0000': 
                    sb.append("\\00"); //$NON-NLS-1$
                    break;
                default:
                    sb.append(curChar);
            }
        }
        return sb.toString();
    }

    private UserEntry getUserEntry(String username,
                                   boolean getGroups) throws MembershipSourceException,
                                                     InvalidUserException {

        username = escapeLDAPSearchFilter(username);
        
        LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {domainName, "getUserEntry", username, "getGroups", String.valueOf(getGroups)}); //$NON-NLS-1$ //$NON-NLS-2$
        
        DirContext ctx = null;

        try {

            ctx = getAdminContext();

            for (int i = 0; i < usersRootContexts.size(); i++) {
                
                LdapContext context = (LdapContext)usersRootContexts.get(i);

                String contextName = context.context;

                SearchControls sControls = new SearchControls();
                sControls.setSearchScope(context.searchScope);
                if (context.memberOfAttribute != null) {
                    sControls.setReturningAttributes(new String[] {context.memberOfAttribute});
                }
                String singleUserSearchFilter = "(" + context.displayAttribute + "=" + username + ")"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

                if (context.searchFilter.length() > 0) {
                    singleUserSearchFilter = "(&" + singleUserSearchFilter + context.searchFilter + ")"; //$NON-NLS-1$ //$NON-NLS-2$
                }
                
                LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {domainName, "searching context", contextName, "with filter", singleUserSearchFilter, "and search scope", String.valueOf(context.searchScope)}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                
                // We expect to receive only one user from this search, since the username attribute must be unique.
                NamingEnumeration usersEnumeration = ctx.search(contextName, singleUserSearchFilter, sControls);
                if (!usersEnumeration.hasMore()) {
                    LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {domainName, "no user match found in context", contextName}); //$NON-NLS-1$
                    continue;
                }
                SearchResult foundUser = (SearchResult)usersEnumeration.next();
                
                LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {domainName, "found user", username, "in context", contextName}); //$NON-NLS-1$ //$NON-NLS-2$
                
                if (usersEnumeration.hasMore()) {
                    LogManager
                              .logWarning(LogConstants.CTX_MEMBERSHIP,
                                          domainName
                                                          + ": Only expected one user when performing lookup. Check to ensure the display name is unique."); //$NON-NLS-1$
                }

                String RDN = foundUser.getName();
                String dn = RDN + ',' + contextName;
                HashSet groupList = new HashSet();
                
                if (getGroups) {
                    Map groupNames = getGroupNames(ctx, dn, context.memberOfAttribute == null);

                    if (context.memberOfAttribute != null) {
                        Attribute memberOfAttr = foundUser.getAttributes().get(context.memberOfAttribute);

                        if (memberOfAttr != null) {
                            int groupCount = memberOfAttr.size();
                            for (int j = 0; j < groupCount; j++) {
                                String groupDN = (String)memberOfAttr.get(i);
                                if (groupDN == null) {
                                    continue;
                                }
                                String groupRdn = (String)groupNames.get(groupDN);
                                if (groupRdn == null) {
                                    continue;
                                }
                                groupList.add(groupDN);
                                LogManager
                                          .logTrace(LogConstants.CTX_MEMBERSHIP, domainName
                                                                                         + "-----Adding user's group: " + groupDN); //$NON-NLS-1$
                            }
                        }
                    } else {
                        groupList.addAll(groupNames.values());
                    }
                }
                UserEntry ue = new UserEntry(dn, groupList);
                
                LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {domainName, "UserEntry retrieved for username", username, ue.getDn()}); //$NON-NLS-1$
                
                return ue;
            }
        } catch (NamingException ne) {
            throw new MembershipSourceException(ne);
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (NamingException ne) {
                    LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, ne, domainName + ": error closing context"); //$NON-NLS-1$
                }
            }
        }

        LogManager.logInfo(LogConstants.CTX_MEMBERSHIP,
                           domainName + ": No user DN found for user: " + username + ", could not authenticate."); //$NON-NLS-1$ //$NON-NLS-2$
        throw new InvalidUserException(username);
    }

    private Map getGroupNames(DirContext ctx,
                                           String userDn, boolean mustMatchDn) throws MembershipSourceException {

        LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {domainName, "getGroupNames", userDn, "mustMatchDn", String.valueOf(mustMatchDn)}); //$NON-NLS-1$ //$NON-NLS-2$
        
        Map groupNames = new HashMap();

        try {

            for (int i = 0; i < groupsRootContexts.size(); i++) {

                LdapContext context = (LdapContext)groupsRootContexts.get(i);
                
                String contextName = context.context;

                // Set the search controls to search subdirectories, or just the current level.
                SearchControls groupSC = new SearchControls();
                groupSC.setSearchScope(context.searchScope);
                groupSC.setReturningAttributes(new String[] {context.displayAttribute});

                String searchFilter = context.searchFilter;
                
                if (userDn != null && context.memberOfAttribute != null) {
                    searchFilter = "(&(" + context.memberOfAttribute  + "=" + userDn + ")" + searchFilter + ")"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                } else if (mustMatchDn) {
                    LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {domainName, "skipping group context"}); //$NON-NLS-1$
                    continue;
                }
                
                LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {domainName, "searching group context", contextName, "with filter", searchFilter, "and search scope", String.valueOf(context.searchScope)}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

                NamingEnumeration groupsEnum = ctx.search(contextName, searchFilter, groupSC);
                
                LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {domainName, "Parsing through groups search results."}); //$NON-NLS-1$

                while (groupsEnum.hasMore()) {
                    SearchResult curGroup = (SearchResult)groupsEnum.next();
                    String groupRDN = curGroup.getName();

                    String groupDN = groupRDN + ',' + contextName;
                    // GHH - if the context here is a single group, we end up with the groupRDN being an empty string, in which
                    // case there is now an extra comma at the start of groupDN
                    if (groupDN.charAt(0) == ',') {
                        groupDN = groupDN.substring(1);
                    }
                    Attributes attrs = curGroup.getAttributes();
                    if (attrs == null) {
                        continue;
                    }
                    // Get the display name.
                    Attribute groupDisplayNameAttr = attrs.get(context.displayAttribute);
                    if (groupDisplayNameAttr == null) {
                        continue;
                    }
                    String groupDisplayName = (String)groupDisplayNameAttr.get();
                    if (groupDisplayName == null) {
                        continue;
                    }

                    groupNames.put(groupDN, groupDisplayName);

                    LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {domainName, "Found groupDN", groupDN, "with display name", groupDisplayName}); //$NON-NLS-1$ //$NON-NLS-2$
                }
            }
        } catch (NamingException err) {
            throw new MembershipSourceException(err);
        }

        return groupNames;
    }

	List getUsersRootContexts() {
		return usersRootContexts;
	}

	List getGroupsRootContexts() {
		return groupsRootContexts;
	}

}
