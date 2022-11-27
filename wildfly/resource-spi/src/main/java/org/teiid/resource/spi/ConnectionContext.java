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
package org.teiid.resource.spi;

import java.security.Principal;
import java.security.acl.Group;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Set;

import javax.resource.spi.security.PasswordCredential;
import javax.security.auth.Subject;

/**
 * Thread local class to access the Subject in the Connector code. This is set just before the
 * connector connection is created.
 */
public class ConnectionContext {
    private static ThreadLocal<Subject> SUBJECT = new ThreadLocal<Subject>() {
        @Override
        protected Subject initialValue() {
            return null;
        }
    };

    public static Subject getSubject() {
        return SUBJECT.get();
    }

    public static void setSubject(Subject subject) {
        SUBJECT.set(subject);
    }

    public static String getUserName(Subject subject, BasicManagedConnectionFactory mcf, String defalt) {
        Set<PasswordCredential> creds = subject.getPrivateCredentials(PasswordCredential.class);
        if ((creds != null) && (creds.size() > 0)) {
            for (PasswordCredential cred : creds) {
                if (cred.getManagedConnectionFactory().equals(mcf)) {
                    if (cred.getUserName() != null) {
                        return cred.getUserName();
                    }
                }
            }
        }
        return defalt;
    }

    public static String getPassword(Subject subject, BasicManagedConnectionFactory mcf, String userName, String defalt) {
        Set<PasswordCredential> creds = subject.getPrivateCredentials(PasswordCredential.class);
        if ((creds != null) && (creds.size() > 0)) {
            for (PasswordCredential cred : creds) {
                if (cred.getManagedConnectionFactory().equals(mcf)) {
                    if (cred.getUserName().equals(userName)) {
                        if (cred.getPassword() != null) {
                            return new String(cred.getPassword());
                        }
                    }
                }
            }
        }
        return defalt;
    }

    public static String[] getRoles(Subject subject, String[] defalt) {
        ArrayList<String> roles = new ArrayList<String>();
        Set<Group> principals = subject.getPrincipals(Group.class);
        if ((principals != null) && (principals.size() > 0)) {
            for (Group group : principals) {
                if (group.getName().equalsIgnoreCase("roles")) { //$NON-NLS-1$
                    Enumeration<? extends Principal> members = group.members();
                    while(members.hasMoreElements()) {
                        Principal member = members.nextElement();
                        roles.add(member.getName());
                    }
                }
            }
            return roles.toArray(new String[roles.size()]);
        }
        return defalt;
    }

    // can not associate with MCF, as AS framework only identifies the PasswordCredential as known credential
    // and assigns the MCF. So, we just take the first credential.
    public static <T> T getSecurityCredential(Subject subject, Class<T> clazz) {
        Set<T> creds = subject.getPrivateCredentials(clazz);
        if ((creds != null) && (creds.size() > 0)) {
            return creds.iterator().next();
        }
        return null;
    }
}
