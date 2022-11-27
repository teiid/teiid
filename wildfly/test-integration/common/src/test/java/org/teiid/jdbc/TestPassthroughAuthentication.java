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
package org.teiid.jdbc;

import static org.junit.Assert.*;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;

@SuppressWarnings("nls")
public class TestPassthroughAuthentication {

    static FakeServer server = new FakeServer(false);
    static TestableSecurityHelper securityHelper = new TestableSecurityHelper();

    @AfterClass public static void oneTimeTearDown() {
        server.stop();
    }

    @BeforeClass public static void oneTimeSetup() throws Exception {
        server.setUseCallingThread(true);
        server.start(new EmbeddedConfiguration() {
            @Override
            public SecurityHelper getSecurityHelper() {
                return securityHelper;
            }
        }, false);
    }

    @Test
    public void test() throws Exception {
        try {
            server.deployVDB("not_there", UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
            server.getSessionService().setTrustAllLocal(false);
            try {
                server.createConnection("jdbc:teiid:not_there.1;passthroughAuthentication=true");
                fail();
            } catch (Exception e) {
            }

            server.getSessionService().setTrustAllLocal(true);
            server.createConnection("jdbc:teiid:not_there.1;passthroughAuthentication=true");

            securityHelper.associateSecurityContext("testSC");
            try {
                server.createConnection("jdbc:teiid:not_there.1;passthroughAuthentication=true");
            } catch (Exception e) {
                fail();
            }
        } finally {
            server.undeployVDB("not_there");
        }
    }

    private static class TestableSecurityHelper implements SecurityHelper {
        Object ctx;
        @Override
        public Object associateSecurityContext(Object context) {
            return ctx = context;
        }
        @Override
        public void clearSecurityContext() {
            ctx = null;
        }
        @Override
        public Object getSecurityContext(String securityDomain) {
            if (securityDomain.equals("teiid-security")) {
                return this.ctx;
            }
            return null;
        }

        @Override
        public Subject getSubjectInContext(Object context) {
            if (context != null) {
                return new Subject();
            }
            return null;
        }

        @Override
        public Object authenticate(String securityDomain, String baseUserName,
                Credentials credentials, String applicationName) throws LoginException {
            return ctx;
        }
        @Override
        public GSSResult negotiateGssLogin(String securityDomain, byte[] serviceTicket) throws LoginException {
            return null;
        }
    };
}
