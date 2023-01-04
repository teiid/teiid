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


import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.auth.server.SecurityRealm;
import org.wildfly.security.auth.server.event.RealmAuthenticationEvent;
import org.wildfly.security.auth.server.event.RealmAuthorizationEvent;
import org.wildfly.security.auth.server.event.RealmEvent;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.evidence.Evidence;

import java.security.Principal;
import java.security.spec.AlgorithmParameterSpec;

/**
 * A simple server login realm to creates subject with passed in name and null password
 * Note: Conversion to SecurityRealm is not complete
 */
public class SimpleLoginModule implements SecurityRealm { //extends UsernamePasswordLoginModule {


    @Override
    public RealmIdentity getRealmIdentity(Principal principal) throws RealmUnavailableException {
        return RealmIdentity.ANONYMOUS;
    }

    @Override
    public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> aClass, String s, AlgorithmParameterSpec algorithmParameterSpec) {
        return SupportLevel.UNSUPPORTED;
    }

    @Override
    public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> aClass, String s) {
        return SupportLevel.UNSUPPORTED;
    }

    public void handleRealmEvent(RealmEvent event) {
        if(event instanceof RealmAuthenticationEvent){

        } else if(event instanceof RealmAuthorizationEvent){

        } else {
            SecurityRealm.super.handleRealmEvent(event);
        }
    }
}