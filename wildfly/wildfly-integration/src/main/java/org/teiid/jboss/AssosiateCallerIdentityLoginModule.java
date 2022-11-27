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
package org.teiid.jboss;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.jboss.security.SecurityContext;
import org.jboss.security.SubjectInfo;
import org.jboss.security.auth.spi.AbstractServerLoginModule;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;

/**
 * This login modules simply takes the subject in the current context and adds
 * its principle to shared state. This is same as CallerIdentityLoginModule,
 * just it does not extend the AbstractPasswordCredentialLoginModule
 */
public class AssosiateCallerIdentityLoginModule extends AbstractServerLoginModule {

    private Principal principal;

    public void initialize(Subject subject, CallbackHandler handler,
            Map sharedState, Map options) {
        super.initialize(subject, handler, sharedState, options);
    }

    /**
     * Performs the login association between the caller and the resource for a
     * 1 to 1 mapping. This acts as a login propagation strategy and is useful
     * for single-sign on requirements
     *
     * @return True if authentication succeeds
     * @throws LoginException
     */
    public boolean login() throws LoginException {

        SecurityContext sc = SecurityActions.getSecurityContext();
        SubjectInfo si = sc.getSubjectInfo();
        Subject subject = si.getAuthenticatedSubject();

        Set<Principal> principals = subject.getPrincipals();
        this.principal = principals.iterator().next();

        if (super.login() == true) {
            return true;
        }

        LogManager.logDetail(LogConstants.CTX_SECURITY, "Adding Passthrough principal="+principal.getName()); //$NON-NLS-1$

        // Put the principal name into the sharedState map
        sharedState.put("javax.security.auth.login.name", principal.getName()); //$NON-NLS-1$
        sharedState.put("javax.security.auth.login.password", ""); //$NON-NLS-1$ //$NON-NLS-2$
        super.loginOk = true;

        return true;
    }

    protected Principal getIdentity() {
        return principal;
    }

    protected Group[] getRoleSets() throws LoginException {
        return new Group[] {};
    }
}
