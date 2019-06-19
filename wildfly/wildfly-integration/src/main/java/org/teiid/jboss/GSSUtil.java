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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.jboss.logging.Logger;
import org.jboss.security.negotiation.Constants;

/**
 * Utility class for converting a {@link GSSCredential} to a {@link Subject}
 *
 * @author <a href="mailto:darran.lofthouse@jboss.com">Darran Lofthouse</a>
 */
class GSSUtil {
    private static final Logger log = Logger.getLogger(GSSUtil.class);
    private static final Method CREATE_SUBJECT_METHOD = getCreateSubjectMethod();
    private static final String SUN_GSSUTIL = "com.sun.security.jgss.GSSUtil";
    private static final String CREATE_SUBJECT = "createSubject";

    /**
     * Populate the supplied {@link Subject} based on the supplied {@link GSSCredential}
     *
     * @param delegatedCredential - The GSSCredential to use for population.
     * @param privateCredential The optional {@link GSSCredential} to add to the private credentials of the {@link Subject}.
     * @return A {@link Subject} that was created from the GSSCredential so that we can identify the content to remove later.
     */
    static Subject createGssSubject(GSSCredential delegatedCredential, GSSCredential privateCredential) throws LoginException {
        Subject intermediateSubject = null;
        if (CREATE_SUBJECT_METHOD != null) {
            try {
                GSSName name = delegatedCredential.getName(Constants.KERBEROS_V5);
                intermediateSubject = invokeCreateSubject(CREATE_SUBJECT_METHOD, name, delegatedCredential);
                log.trace("Delegated credential converted to Subject.");
            } catch (GSSException e) {
                log.debug(e);
                throw new LoginException("Unable to use supplied GSSCredential to populate Subject.");
            }
        } else if (privateCredential == null) {
            throw new LoginException(
                    "Utility not available to convert from GSSCredential and adding GSSCredential to Subject disabled - this would just result in an empty Subject!");
        }
        return intermediateSubject;
    }



    @SuppressWarnings({ "rawtypes", "unchecked" })
    static Method createSubjectMethod() {
        try {
            Class sunGssUtil = GSSUtil.class.getClassLoader().loadClass(SUN_GSSUTIL);
            return sunGssUtil.getMethod(CREATE_SUBJECT, GSSName.class, GSSCredential.class);
        } catch (ClassNotFoundException e) {
            log.debug(e);
            return null;
        } catch (NoSuchMethodException e) {
            log.debug(e);
            return null;
        } catch (SecurityException e) {
            log.debug(e);
            return null;
        }
    }

    static Method getCreateSubjectMethod() {
        if (System.getSecurityManager() == null) {
            return createSubjectMethod();
        }
        return AccessController.doPrivileged(new PrivilegedAction<Method>() {
            public Method run() {
                return createSubjectMethod();
            }
        });
    }

    static Subject invokeCreateSubject(Method createSubjectMethod,
            GSSName gssName, GSSCredential gssCredential) throws GSSException {
        try {
            return (Subject)createSubjectMethod.invoke(null, gssName, gssCredential);
        } catch (IllegalAccessException e) {
            log.debug(e);
            return null;
        } catch (IllegalArgumentException e) {
            log.debug(e);
            return null;
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof GSSException) {
                throw (GSSException) cause;
            }
            log.debug(cause);
            return null;
        }
    }

    static Subject createSubjectMethod(final Method createSubjectMethod,
            final GSSName gssName, final GSSCredential gssCredential) throws GSSException {
        if (System.getSecurityManager() == null) {
            return invokeCreateSubject(createSubjectMethod, gssName, gssCredential);
        }
        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<Subject>() {
                public Subject run() throws Exception {
                    return invokeCreateSubject(createSubjectMethod, gssName, gssCredential);
                }
            });
        } catch (PrivilegedActionException e) {
            throw (GSSException) e.getCause();
        }
    }
}
