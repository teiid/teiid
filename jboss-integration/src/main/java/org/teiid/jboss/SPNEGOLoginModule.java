/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.jboss;

import java.security.AccessController;
import java.security.PrivilegedAction;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.jboss.security.negotiation.common.NegotiationContext;

public class SPNEGOLoginModule  extends org.jboss.security.negotiation.spnego.SPNEGOLoginModule {
    private GSSCredential storedCredential = null;
    
    @Override
    public boolean commit() throws LoginException {
        boolean result = super.commit();
        if (result) {
            NegotiationContext negotiationContext = NegotiationContext.getCurrentNegotiationContext();
            if(negotiationContext != null) {
                GSSContext gssContext = (GSSContext)negotiationContext.getSchemeContext();
                if (gssContext != null && gssContext.getCredDelegState()) {
                    try {
                        GSSCredential credential = gssContext.getDelegCred();
                        if (credential != null) {
                            addPrivateCredential(subject, credential);
                            log.trace("Added private credential.");
                            this.storedCredential = credential;
                        }
                    } catch (GSSException e) {
                        log.warn("Unable to obtain delegation credential.", e);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public boolean logout() throws LoginException {
        if (System.getSecurityManager() == null) {
            if (storedCredential != null) {
                removePrivateCredential(subject, storedCredential);
                log.trace("Remove GSSCredential to the Subject");
                storedCredential = null;
            }
        } else {
            AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
                public Boolean run() {
                    if (storedCredential != null) {
                        removePrivateCredential(subject, storedCredential);
                        log.trace("Remove GSSCredential to the Subject");
                        storedCredential = null;
                    }
                    return null;
                }
            });
        }
        return super.logout();
    }
    
    static void addPrivateCredential(final Subject subject, final Object obj) {
        if (System.getSecurityManager() == null) {
            subject.getPrivateCredentials().add(obj);
        } else {
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                public Object run() {
                    subject.getPrivateCredentials().add(obj);
                    return null;
                }
            });
        }
    }
    
    static void removePrivateCredential(final Subject subject, final Object obj) {
        if (System.getSecurityManager() == null) {
            subject.getPrivateCredentials().remove(obj);
        }
        else {
        AccessController.doPrivileged(new PrivilegedAction<Object>() { 
            public Object run() {
                subject.getPrivateCredentials().remove(obj);
                return null;
            }
        });   
        }
    } 
}
