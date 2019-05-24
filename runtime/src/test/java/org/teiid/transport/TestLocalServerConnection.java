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

package org.teiid.transport;

import static org.junit.Assert.*;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.junit.Test;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;

public class TestLocalServerConnection {

    private class EmptySecurityHelper implements SecurityHelper {
        @Override
        public GSSResult negotiateGssLogin(String securityDomain,
                byte[] serviceTicket) throws LoginException {
            throw new AssertionError();
        }

        @Override
        public Subject getSubjectInContext(Object context) {
            return null;
        }

        @Override
        public Object getSecurityContext(String securityDomain) {
            return null;
        }

        @Override
        public void clearSecurityContext() {
            throw new AssertionError();
        }

        @Override
        public Object authenticate(String securityDomain, String baseUserName,
                Credentials credentials, String applicationName)
                throws LoginException {
            throw new AssertionError();
        }

        @Override
        public Object associateSecurityContext(Object context) {
            throw new AssertionError();
        }
    }

    @Test public void testSameSubjectNull() {
        DQPWorkContext workContext = new DQPWorkContext();
        workContext.setSecurityHelper(new EmptySecurityHelper());
        assertTrue(LocalServerConnection.sameSubject(workContext));
    }

}
