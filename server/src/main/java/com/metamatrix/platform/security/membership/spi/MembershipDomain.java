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

package com.metamatrix.platform.security.membership.spi;

import java.io.Serializable;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.api.exception.security.InvalidUserException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.api.exception.security.UnsupportedCredentialException;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.membership.service.SuccessfulAuthenticationToken;
import com.metamatrix.platform.service.api.exception.ServiceStateException;

/**
 * Defines a Membership Domain for the Membership Service.
 */
public interface MembershipDomain {

    /**
     * Initialize this domain with the given properties.
     * 
     * @param env contains the properties for this domain as set by the console
     * @throws ServiceStateException
     */
    void initialize(Properties env) throws ServiceStateException;

    /**
     * Shut down this domain to further work.
     * 
     * @throws ServiceStateException
     */
    void shutdown() throws ServiceStateException;

    /**
     * Authenticate a user with the specified username and credential for use with the specified application. The application name
     * may also be used by the Membership Domain to determine the appropriate authentication mechanism. 
     * 
     * @param username
     *            The base username (without the domain suffix) of the individual attempting authentication. May be <code>null</code> if the
     *            membership domain implementation uses a mechanism other than username/credential authentication.
     * @param credential
     *            The credentials belonging to the individual seeking authentication. May be <code>null</code> for anonymous authentications.
     * @param trustedPayload
     *            The trusted payload set by the client.  May be <code>null</code> if not set by the client.
     * @param applicationName
     *            The name of the application to which the individual is attempting to authenticate. It's provided as a connection
     *            property when the individual connects (via URL or connection properties). This <code>applicationName</code>
     *            may be used by the authenticating membership domain as a basis for authentication and authorization.
     * @return the SuccessfulAuthenticationToken containing the username and trustedPayload.  The username in the 
     *          SuccessfulAuthenticationToken will be used to identify this user in later calls.  If the user is to be authenticated into a 
     *          different domain, that domain name should be set on the SuccessfulAuthenticationToken.  
     *          The return value should not be null.
     * @throws InvalidUserException if the user does not exist in this domain
     * @throws UnsupportedCredentialException if the credential or trustedPayload cannot be used to authenticate the user
     * @throws LogonException if the user was unsuccessfully authenticated
     * @throws MembershipSourceException if there was an internal error
     */
    SuccessfulAuthenticationToken authenticateUser(String username,
                            Credentials credential,
                            Serializable trustedPayload,
                            String applicationName) throws UnsupportedCredentialException, InvalidUserException, LogonException, MembershipSourceException;

    /**
     * Returns a String set all group names known to this domain.  The returned values should not be fully qualified with a domain suffix.
     * @return a set of String group names
     * @throws MembershipSourceException if there was an internal error
     */
    Set getGroupNames() throws MembershipSourceException;

    /**
     * Returns a String set of all group names the given user is a member of.  The returned values should not be fully qualified with a domain suffix.
     * @param username
     * @return a set of String group names
     * @throws InvalidUserException if the user does not exist in this domain
     * @throws MembershipSourceException if there was an internal error
     */
    Set getGroupNamesForUser(String username) throws InvalidUserException, MembershipSourceException;

}
