/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.resource.adapter.ws;

import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Set;

import javax.crypto.SecretKey;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.cxf.Bus;
import org.apache.cxf.helpers.DOMUtils;
import org.apache.cxf.ws.security.kerberos.KerberosClient;
import org.apache.cxf.ws.security.tokenstore.SecurityToken;
import org.apache.ws.security.WSConstants;
import org.apache.ws.security.WSSConfig;
import org.apache.ws.security.WSSecurityException;
import org.apache.ws.security.message.token.KerberosSecurity;
import org.apache.ws.security.util.Base64;
import org.apache.ws.security.util.WSSecurityUtil;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.resource.spi.ConnectionContext;
import org.w3c.dom.Document;
/**
 * Parts of this code as been borrowed from apache CXF later versions to support Teiid.
 */
public class DelegateKerberosClient extends KerberosClient {
    
    private static final String KERBEROS_OID = "1.2.840.113554.1.2.2";//$NON-NLS-1$
    private GSSCredential credential;
    
    private WSSConfig wssConfig = WSSConfig.getNewInstance();
    
    @Deprecated
    public DelegateKerberosClient(Bus b) {
        super(b);
    }    

    @SuppressWarnings("deprecation")
    public DelegateKerberosClient() {
        super(null);
    }
    
    @Override
    public SecurityToken requestSecurityToken() throws Exception {
        DelegatedKerberosSecurity bst = new DelegatedKerberosSecurity(DOMUtils.createDocument());
        bst.retrieveServiceTicket(getContextName(), getServiceName(), getGssCredential());
        
        bst.addWSUNamespace();
        bst.setID(wssConfig.getIdAllocator().createSecureId("BST-", bst)); //$NON-NLS-1$
        
        SecurityToken st = new SecurityToken(bst.getID());
        st.setToken(bst.getElement());
        st.setWsuId(bst.getID());
        SecretKey secretKey = bst.getSecretKey();
        if (secretKey != null) {
            st.setSecret(secretKey.getEncoded());
        }
        String sha1 = Base64.encode(WSSecurityUtil.generateDigest(bst.getToken()));
        st.setSHA1(sha1);
        st.setTokenType(bst.getValueType());
        return st;
    }     
    
    public GSSCredential getGssCredential() {
        if (this.credential != null) {
            return this.credential;
        }
        
        // Teiid Specific
        Subject subject = ConnectionContext.getSubject();
        if (subject != null) {
            return ConnectionContext.getSecurityCredential(subject, GSSCredential.class);
        }        
        return null;
    }
    
    public void setGssCredential(GSSCredential credential) {
        this.credential = credential;
    }    

    /**
     * Modified Kerberos Security for delegation from later versions of WSS4J
     * 
     * Licensed to the Apache Software Foundation (ASF) under one
     * or more contributor license agreements. See the NOTICE file
     * distributed with this work for additional information
     * regarding copyright ownership. The ASF licenses this file
     * to you under the Apache License, Version 2.0 (the
     * "License"); you may not use this file except in compliance
     * with the License. You may obtain a copy of the License at
     *
     * http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing,
     * software distributed under the License is distributed on an
     * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
     * KIND, either express or implied. See the License for the
     * specific language governing permissions and limitations
     * under the License.
     */
    static class DelegatedKerberosSecurity extends KerberosSecurity {
        private SecretKey secretKey;
        
        public DelegatedKerberosSecurity(Document doc) {
            super(doc);
        }

        @Override
        public SecretKey getSecretKey() {
            return secretKey;
        }
        
        public void retrieveServiceTicket(String jaasName, String serviceName, GSSCredential delegatedCredential)
                throws WSSecurityException {
            // Get a TGT from the KDC using JAAS
            LoginContext loginContext = null;
            try {
                loginContext = new LoginContext(jaasName);
                loginContext.login();
            } catch (LoginException ex) {
                throw new WSSecurityException(WSSecurityException.FAILURE, "kerberosLoginError",//$NON-NLS-1$
                        new Object[] { ex.getMessage() }, ex);
            }

            Subject clientSubject = loginContext.getSubject();
            Set<Principal> clientPrincipals = clientSubject.getPrincipals();
            if (clientPrincipals.isEmpty()) {
                throw new WSSecurityException(WSSecurityException.FAILURE, "kerberosLoginError",//$NON-NLS-1$
                        new Object[] { "No Client principals found after login" });//$NON-NLS-1$
            }
            
            // Store the TGT
            KerberosTicket tgt = getKerberosTicket(clientSubject, null);

            // Get the service ticket
            KerberosClientAction action = new KerberosClientAction(serviceName, delegatedCredential);
            byte[] ticket = Subject.doAs(clientSubject, action);
            if (ticket == null) {
                throw new WSSecurityException(WSSecurityException.FAILURE, "kerberosServiceTicketError");//$NON-NLS-1$
            }

            // Get the Service Ticket (private credential)
            KerberosTicket serviceTicket = getKerberosTicket(clientSubject, tgt);
            if (serviceTicket != null) {
                secretKey = serviceTicket.getSessionKey();
            }

            setToken(ticket);

            if ("".equals(getValueType())) { //$NON-NLS-1$
                setValueType(WSConstants.WSS_GSS_KRB_V5_AP_REQ);
            }
        }
        
        private KerberosTicket getKerberosTicket(Subject clientSubject, KerberosTicket previousTicket) {
            Set<KerberosTicket> privateCredentials = clientSubject.getPrivateCredentials(KerberosTicket.class);
            if (privateCredentials == null || privateCredentials.isEmpty()) {
                return null;
            }
            
            for (KerberosTicket privateCredential : privateCredentials) {
                if (!privateCredential.equals(previousTicket)) {
                    return privateCredential;
                }
            }
            return null;
        }        
    }
    
    static class KerberosClientAction implements PrivilegedAction<byte[]> {
        private String serviceName;
        private GSSCredential delegatedCredential;
        
        public KerberosClientAction(String serviceName, GSSCredential delegatedCredential) {
            this.serviceName = serviceName;
            this.delegatedCredential = delegatedCredential;
        }
        
        public byte[] run() {
            try {
                GSSManager gssManager = GSSManager.getInstance();
            
                Oid kerberos5Oid = new Oid(KERBEROS_OID);
                GSSCredential credentials = this.delegatedCredential;
                
                GSSName gssService = gssManager.createName(this.serviceName, GSSName.NT_HOSTBASED_SERVICE);
                GSSContext secContext = gssManager.createContext(gssService, kerberos5Oid, credentials,
                        GSSContext.DEFAULT_LIFETIME);
     
                secContext.requestMutualAuth(false);
                byte[] token = new byte[0];
                byte[] returnedToken = secContext.initSecContext(token, 0, token.length);
                secContext.dispose();
                return returnedToken;
            } catch (GSSException e) {
                if (LogManager.isMessageToBeRecorded(LogConstants.CTX_WS, MessageLevel.DETAIL)) {
                    LogManager.logDetail(LogConstants.CTX_WS, "Error in obtaining a Kerberos token"); //$NON-NLS-1$
                }
            }
            return null;
        }
    }    
}


