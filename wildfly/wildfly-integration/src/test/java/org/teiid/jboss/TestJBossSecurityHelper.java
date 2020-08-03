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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import javax.security.auth.Subject;

import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.plugins.JBossSecurityContext;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.security.Credentials;
import org.teiid.services.SessionServiceImpl;
import org.teiid.vdb.runtime.VDBKey;

import junit.framework.TestCase;

@SuppressWarnings("nls")
public class TestJBossSecurityHelper extends TestCase {

    private JBossSecurityHelper buildSecurityHelper(final String domain, final SecurityDomainContext sdc)
            throws Exception {
        Principal p = Mockito.mock(Principal.class);
        Mockito.stub(p.getName()).toReturn("alreadylogged"); //$NON-NLS-1$
        HashSet<Principal> principals = new HashSet<Principal>();
        principals.add(p);

        @SuppressWarnings("serial")
        JBossSecurityHelper sh = new JBossSecurityHelper() {

            @Override
            protected SecurityDomainContext getSecurityDomainContext(String securityDomain) {
                if (securityDomain.equals(domain)) {
                    return sdc;
                }
                return null;
            }
        };
        return sh;
    }

    public void testAuthenticate() throws Exception {
        Credentials credentials = new Credentials("pass1");

        String domains = "testFile";

        AuthenticationManager authManager = new AuthenticationManager() {
            public String getSecurityDomain() {
                return null;
            }
            public boolean isValid(Principal principal, Object credential, Subject activeSubject) {
                return true;
            }
            public boolean isValid(Principal principal, Object credential) {
                return true;
            }
            @Override
            public Principal getTargetPrincipal(Principal anotherDomainPrincipal, Map<String, Object> contextMap) {
                return null;
            }
            @Override
            public Subject getActiveSubject() {
                return null;
            }
            @Override
            public void logout(Principal arg0, Subject arg1) {
            }
        };
        final SecurityDomainContext securityContext = new SecurityDomainContext(authManager, null, null, null, null, null);

        JBossSecurityHelper ms = buildSecurityHelper(domains, securityContext);

        Object c = ms.authenticate(domains, "user1", credentials, null); //$NON-NLS-1$
        assertTrue(c instanceof JBossSecurityContext); //$NON-NLS-1$
        assertEquals(domains, ((JBossSecurityContext)c).getSecurityDomain());
    }

    public void validateSession(boolean securityEnabled) throws Exception {
        final ArrayList<String> domains = new ArrayList<String>();
        domains.add("somedomain");

        AuthenticationManager authManager = Mockito.mock(AuthenticationManager.class);
        Mockito.stub(authManager.isValid(new SimplePrincipal("steve"), "pass1", new Subject())).toReturn(true);

        final SecurityDomainContext securityContext = new SecurityDomainContext(authManager, null, null, null, null, null);

        SessionServiceImpl jss = new SessionServiceImpl() {
            @Override
            protected VDBMetaData getActiveVDB(String vdbName, String vdbVersion)
                    throws SessionServiceException {
                return Mockito.mock(VDBMetaData.class);
            }
        };
        jss.setSecurityHelper(buildSecurityHelper("somedomain", securityContext));
        jss.setSecurityDomain("somedomain");

        try {
            jss.validateSession(String.valueOf(1));
            fail("exception expected"); //$NON-NLS-1$
        } catch (InvalidSessionException e) {

        }

        SessionMetadata info = jss.createSession("x", "1", AuthenticationType.USERPASSWORD, "steve",  new Credentials("pass1"), "foo", new Properties()); //$NON-NLS-1$ //$NON-NLS-2$
        if (securityEnabled) {
            Mockito.verify(authManager).isValid(new SimplePrincipal("steve"), "pass1", new Subject());
        }

        String id1 = info.getSessionId();
        jss.validateSession(id1);

        assertEquals(1, jss.getActiveSessionsCount());
        assertEquals(0, jss.getSessionsLoggedInToVDB(new VDBKey("a", 1)).size()); //$NON-NLS-1$

        jss.closeSession(id1);

        try {
            jss.validateSession(id1);
            fail("exception expected"); //$NON-NLS-1$
        } catch (InvalidSessionException e) {

        }

        try {
            jss.closeSession(id1);
            fail("exception expected"); //$NON-NLS-1$
        } catch (InvalidSessionException e) {

        }
    }

    @Test public void testvalidateSession() throws Exception{
        validateSession(true);
    }

    @Test public void testvalidateSession2() throws Exception {
        validateSession(false);
    }
}
