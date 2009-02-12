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

package com.metamatrix.platform.security.membership.spi.file;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.api.exception.security.InvalidUserException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.api.exception.security.UnsupportedCredentialException;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.membership.service.SuccessfulAuthenticationToken;
import com.metamatrix.platform.security.membership.spi.MembershipDomain;
import com.metamatrix.platform.security.membership.spi.MembershipSourceException;
import com.metamatrix.platform.service.api.exception.ServiceStateException;

/**
 * A membership domain that loads user and group definitions from the file system. 
 */
public class FileMembershipDomain implements MembershipDomain {
    
    public static final String USERS_FILE = "usersFile"; //$NON-NLS-1$
    public static final String GROUPS_FILE = "groupsFile"; //$NON-NLS-1$
    public static final String CHECK_PASSWORD = "checkPassword"; //$NON-NLS-1$
    
    private boolean checkPasswords;
    
    private Properties users;
    private HashMap groups = new HashMap();
    private HashMap userGroups = new HashMap();

    /** 
     * @see com.metamatrix.platform.security.membership.spi.MembershipDomain#initialize(java.util.Properties)
     */
    public void initialize(Properties env) throws ServiceStateException {
        checkPasswords = Boolean.valueOf(env.getProperty(CHECK_PASSWORD, Boolean.TRUE.toString())).booleanValue();
        
        String userFile = env.getProperty(USERS_FILE);
        String groupFile = env.getProperty(GROUPS_FILE);
        
        if (userFile == null) {
            throw new ServiceStateException("Required property " +USERS_FILE+ " was missing."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        users = loadFile(userFile);
        
        if (groupFile == null) {
            throw new ServiceStateException("Required property " +GROUPS_FILE+ " was missing."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        groups.clear();
        groups.putAll(loadFile(groupFile));
        userGroups.clear();
        for (Iterator i = groups.entrySet().iterator(); i.hasNext();) {
            Map.Entry entry = (Map.Entry)i.next();
            String group = (String)entry.getKey();
            String userNames = (String)entry.getValue();
            String[] groupUsers = userNames.split(","); //$NON-NLS-1$
            
            for (int j = 0; j < groupUsers.length; j++) {
                String user = groupUsers[j].trim();
                Set uGroups = (Set)userGroups.get(user);
                if (uGroups == null) {
                    uGroups = new HashSet();
                    userGroups.put(user, uGroups);
                }
                uGroups.add(group);
            }
        }
    }
    
    private Properties loadFile(String fileName) throws ServiceStateException {
        Properties result = new Properties();

        //try the classpath
        InputStream is = this.getClass().getResourceAsStream(fileName);
        
        if (is == null) {
            try {
                //try the filesystem
                is = new FileInputStream(fileName);
            } catch (FileNotFoundException err) {
                try {
                    //try a url
                    is = new URL(fileName).openStream();
                } catch (MalformedURLException err1) {
                    throw new ServiceStateException(err, "Could not load file "+fileName+" for FileMembershipDomain"); //$NON-NLS-1$ //$NON-NLS-2$
                } catch (IOException err1) {
                    throw new ServiceStateException(err1, "Could not load file "+fileName+" for FileMembershipDomain"); //$NON-NLS-1$ //$NON-NLS-2$
                }
            } 
        }
        
        try {
            result.load(is);
        } catch (IOException err) {
            throw new ServiceStateException(err, "Could not load file "+fileName+" for FileMembershipDomain"); //$NON-NLS-1$ //$NON-NLS-2$
        } finally {
            try {
                is.close();
            } catch (IOException err) {
            }
        }
        return result;
    }
    
    /** 
     * @see com.metamatrix.platform.security.membership.spi.MembershipDomain#shutdown()
     */
    public void shutdown() throws ServiceStateException {
    }

    /** 
     * @see com.metamatrix.platform.security.membership.spi.MembershipDomain#authenticateUser(java.lang.String, com.metamatrix.platform.security.api.Credentials, java.io.Serializable, java.lang.String)
     */
    public SuccessfulAuthenticationToken authenticateUser(String username,
                                                          Credentials credential,
                                                          Serializable trustedPayload,
                                                          String applicationName) throws UnsupportedCredentialException,
                                                                                 InvalidUserException,
                                                                                 LogonException,
                                                                                 MembershipSourceException {
        if (username == null || credential == null) {
            throw new UnsupportedCredentialException("a username and password must be supplied for this domain"); //$NON-NLS-1$
        }
        
        String password = (String)users.get(username);
        
        if (password == null) {
            throw new InvalidUserException("user " + username + " is invalid"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        if (!checkPasswords || password.equals(String.valueOf(credential.getCredentialsAsCharArray()))) {
            return new SuccessfulAuthenticationToken(trustedPayload, username);
        }
                                
        throw new LogonException("user " + username + " could not be authenticated"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** 
     * @see com.metamatrix.platform.security.membership.spi.MembershipDomain#getGroupNames()
     */
    public Set getGroupNames() throws MembershipSourceException {
    	Set resultNames = new HashSet(groups.keySet());
        return resultNames;
    }

    /** 
     * @see com.metamatrix.platform.security.membership.spi.MembershipDomain#getGroupNamesForUser(java.lang.String)
     */
    public Set getGroupNamesForUser(String username) throws InvalidUserException,
                                                    MembershipSourceException {
    	// See if this user is in the domain
        if (!users.containsKey(username)) {
            throw new InvalidUserException("user " + username + " is invalid"); //$NON-NLS-1$ //$NON-NLS-2$
        }

        Set usersGroups = (Set)userGroups.get(username);
        if (usersGroups == null) {
            return Collections.EMPTY_SET;
        }
        return usersGroups;
    }
    
    /** 
     * @return Returns the checkPasswords.
     */
    protected boolean checkPasswords() {
        return this.checkPasswords;
    }
    
    /** 
     * @return Returns the groups.
     */
    protected HashMap getGroups() {
        return this.groups;
    }

    /** 
     * @return Returns the userGroups.
     */
    protected HashMap getUserGroups() {
        return this.userGroups;
    }

    /** 
     * @return Returns the users.
     */
    protected Properties getUsers() {
        return this.users;
    }

}
