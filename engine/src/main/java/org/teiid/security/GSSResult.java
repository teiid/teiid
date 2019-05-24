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

import java.util.concurrent.atomic.AtomicLong;

import org.ietf.jgss.GSSCredential;


public class GSSResult {
    private static String NULL_TOKEN = "Auth validated with no further peer token ";
    private static AtomicLong COUNT = new AtomicLong(0);

    private byte[] serviceToken;
    private boolean authenticated;
    private Object securityContext;
    private String userName;
    private GSSCredential delegationCredential;

    public GSSResult(byte[] token, boolean authenticated, GSSCredential cred) {
        this.serviceToken = token;
        this.authenticated = authenticated;
        this.delegationCredential = cred;
    }

    public GSSResult(boolean authenticated, GSSCredential cred) {
        this.serviceToken = (NULL_TOKEN + COUNT.getAndIncrement()).getBytes();
        this.authenticated = authenticated;
        this.delegationCredential = cred;
    }

    public boolean isNullContinuationToken() {
        String token = new String(this.serviceToken);
        return token.startsWith(NULL_TOKEN);
    }

    public boolean isAuthenticated() {
        return this.authenticated;
    }

    public byte[] getServiceToken() {
        return this.serviceToken;
    }

    public void setSecurityContext(Object sc) {
        this.securityContext = sc;
    }

    public Object getSecurityContext() {
        return this.securityContext;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String name) {
        this.userName = name;
    }

    public GSSCredential getDelegationCredential() {
        return delegationCredential;
    }

    public void setDelegationCredential(GSSCredential delegationCredentail) {
        this.delegationCredential = delegationCredentail;
    }
}