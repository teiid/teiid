/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.security;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

public interface SecurityHelper {

    /**
     * Associate the given context and return the old context
     * @param context
     * @return
     */
    Object associateSecurityContext(Object context);

    /**
     * Clear any security context associated with the thread
     */
    void clearSecurityContext();

    /**
     * Get the current security context associated with the thread
     * @return
     */
    Object getSecurityContext(String securityDomain);

    /**
     * Get the subject associated with the security context.
     * @param context
     * @return
     */
    Subject getSubjectInContext(Object context);

    /**
     * Authenticate the user and return the security context
     * @param securityDomain
     * @param baseUserName without the security domain suffix
     * @param credentials
     * @param applicationName
     * @return a non-null context object
     * @throws LoginException
     */
    Object authenticate(String securityDomain, String baseUserName, Credentials credentials, String applicationName)
            throws LoginException;

    /**
     * Negotiate the GSS login
     * @param securityDomain
     * @param serviceTicket
     * @return
     * @throws LoginException
     */
    GSSResult negotiateGssLogin(String securityDomain, byte[] serviceTicket) throws LoginException;

}
