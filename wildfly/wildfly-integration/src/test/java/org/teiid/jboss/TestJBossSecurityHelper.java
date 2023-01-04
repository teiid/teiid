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

import com.sun.security.auth.UserPrincipal;
import junit.framework.TestCase;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.security.Credentials;
import org.teiid.services.SessionServiceImpl;
import org.teiid.vdb.runtime.VDBKey;
import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.principal.NamePrincipal;
import org.wildfly.security.auth.server.*;
import org.wildfly.security.auth.server.event.*;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.evidence.Evidence;
import org.wildfly.security.evidence.PasswordGuessEvidence;
import org.wildfly.security.permission.PermissionVerifier;

import java.security.Principal;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;
import java.util.Properties;

@SuppressWarnings("nls")
public class TestJBossSecurityHelper extends TestCase {

    private JBossSecurityHelper buildSecurityHelper(final String domainName, SecurityDomain securityDomain) {
        return new JBossSecurityHelper() {
            @Override
            public SecurityDomain getSecurityDomain(String securityDomainName) {
                if (securityDomainName.equals(domainName)) {
                    return securityDomain;
                }
                return super.getSecurityDomain(securityDomainName);
            }
        };
    }

    public void testAuthenticate() throws Exception {
        Credentials credentials = new Credentials("pass1");

        String domains = "testFile";

        final SecurityDomain securityDomain = makeSecurityDomain("testFile", new UserPrincipal("user1"), new PasswordGuessEvidence("pass1".toCharArray()));

        JBossSecurityHelper ms = buildSecurityHelper(domains, securityDomain);

        SecurityIdentity securityIdentity = ms.authenticate(domains, "user1", credentials, null); //$NON-NLS-1$
        assertNotNull(securityIdentity);
        assertEquals("user1", securityIdentity.getPrincipal().getName());
    }

    public void validateSession() throws Exception {
        final SecurityDomain securityDomain = makeSecurityDomain("some_domain", new UserPrincipal("steve"), new PasswordGuessEvidence("pass1".toCharArray()));
        SessionServiceImpl jss = new SessionServiceImpl() {
            @Override
            protected VDBMetaData getActiveVDB(String vdbName, String vdbVersion) {
                return Mockito.mock(VDBMetaData.class);
            }
        };
        jss.setSecurityHelper(buildSecurityHelper("some_domain", securityDomain));
        jss.setSecurityDomain("some_domain");

        try {
            jss.validateSession(String.valueOf(1));
            fail("exception expected"); //$NON-NLS-1$
        } catch (InvalidSessionException ignore) {

        }

        SessionMetadata info = jss.createSession("x", "1", AuthenticationType.USERPASSWORD, "steve", new Credentials("pass1"), "foo", new Properties()); //$NON-NLS-1$ //$NON-NLS-2$

        String id1 = info.getSessionId();
        jss.validateSession(id1);

        assertEquals(1, jss.getActiveSessionsCount());
        assertEquals(0, jss.getSessionsLoggedInToVDB(new VDBKey("a", 1)).size()); //$NON-NLS-1$

        jss.closeSession(id1);

        try {
            jss.validateSession(id1);
            fail("exception expected"); //$NON-NLS-1$
        } catch (InvalidSessionException ignore) {
        }

        try {
            jss.closeSession(id1);
            fail("exception expected"); //$NON-NLS-1$
        } catch (InvalidSessionException ignore) {

        }
    }

    public void testValidateSession() throws Exception {
        validateSession();
    }

    private SecurityDomain makeSecurityDomain(String realmName, Principal principal, PasswordGuessEvidence evidence) {
        SecurityDomain.Builder securityDomain = SecurityDomain.builder();
        securityDomain.addRealm(realmName, makeSecurityRealm(realmName, principal, evidence)).build();
        securityDomain.setDefaultRealmName(realmName);
        securityDomain.setPermissionMapper((permissionMappable, roles) -> PermissionVerifier.ALL);

        return securityDomain.build();
    }

    private SecurityRealm makeSecurityRealm(String realmName, Principal userPrincipal, PasswordGuessEvidence evidence) {
        RealmIdentity realmIdentity = new TestRealmIdentity(realmName, new TestCredential(evidence));
        return new SecurityRealm() {
            @Override
            public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName, AlgorithmParameterSpec parameterSpec) {
                return SupportLevel.UNSUPPORTED;
            }

            @Override
            public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) {
                return SupportLevel.SUPPORTED;
            }

            @Override
            public RealmIdentity getRealmIdentity(Principal principal) {
                if (principal instanceof NamePrincipal && principal.getName().equals(userPrincipal.getName())) {
                    return new TestRealmIdentity(principal.getName(), new TestCredential(evidence));
                }
                return realmIdentity;
            }

            @Override
            public void handleRealmEvent(RealmEvent event) {
                if (event instanceof RealmIdentitySuccessfulAuthorizationEvent) {
                    System.out.println("Realm Identity successful authentication");
                } else if (event instanceof RealmSuccessfulAuthenticationEvent) {
                    System.out.println("Realm successful authentication");
                } else if (event instanceof RealmIdentityFailedAuthorizationEvent) {
                    System.out.println("Realm Identity: You failed.");
                } else if (event instanceof RealmFailedAuthenticationEvent) {
                    System.out.println("Realm: You failed.");
                } else {
                    System.out.println("Something else happened.");
                }
            }
        };
    }

    private static class TestRealmIdentity implements RealmIdentity {

        private final String name;
        private final Credential credential;

        TestRealmIdentity(final String name, Credential credential) {
            this.name = name;
            this.credential = credential;
        }

        public Principal getRealmIdentityPrincipal() {
            return new NamePrincipal(name);
        }

        @Override
        public SupportLevel getCredentialAcquireSupport(final Class<? extends Credential> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec) {
            return SupportLevel.SUPPORTED;
        }

        @Override
        public <C extends Credential> C getCredential(final Class<C> credentialType) throws RealmUnavailableException {
            return getCredential(credentialType, null);
        }

        @Override
        public SupportLevel getEvidenceVerifySupport(final Class<? extends Evidence> evidenceType, final String algorithmName) {
            return SupportLevel.SUPPORTED;
        }

        @Override
        public boolean verifyEvidence(final Evidence evidence) {
            return this.credential.verify(evidence);
        }

        @Override
        public boolean exists() throws RealmUnavailableException {
            return true;
        }
    }

    private static class TestCredential implements Credential {

        private final PasswordGuessEvidence evidence;

        public TestCredential(PasswordGuessEvidence evidence) {
            this.evidence = evidence;
        }

        @Override
        public boolean canVerify(Evidence evidence) {
            return evidence instanceof PasswordGuessEvidence;
        }

        @Override
        public boolean verify(Evidence evidence) {
            return evidence instanceof PasswordGuessEvidence && Arrays.equals(this.evidence.getGuess(), ((PasswordGuessEvidence) evidence).getGuess());
        }

        @Override
        public Credential clone() {
            try {
                return (Credential) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
