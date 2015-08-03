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

import javax.security.auth.callback.CallbackHandler;

import org.apache.ws.security.handler.WSHandlerConstants;

/**
 * SAML Profile based authentication using WSS4J.
 *
 * @param signed - if true must accompany with {@link SignatureProfile}
 *
 * Reference http://ws.apache.org/wss4j/config.html
 * format for saml.properties file
 * org.apache.ws.security.saml.issuerClass=org.apache.ws.security.saml.SAMLIssuerImpl
 * org.apache.ws.security.saml.issuer=www.example.com
 * org.apache.ws.security.saml.issuer.cryptoProp.file=outsecurity.properties
 * org.apache.ws.security.saml.issuer.key.name=myalias
 * org.apache.ws.security.saml.issuer.key.password=myAliasPassword
 * org.apache.ws.security.saml.issuer.sendKeyValue=
 * org.apache.ws.security.saml.issuer.signAssertion=
 * org.apache.ws.security.saml.callback=
 */
public class SAMLTokenProfile extends WSSecurityToken {
    private boolean signed = false;
    private String samlPropFile;
    private CallbackHandler handler;

    public SAMLTokenProfile(boolean signed, String samlPropFile, CallbackHandler handler){
        this.signed = signed;
        this.samlPropFile = samlPropFile;
        this.handler = handler;
    }

    @Override
    public void addSecurity(WSSecurityCredential credential) {
        if (this.signed) {
            setAction(credential, WSHandlerConstants.SAML_TOKEN_SIGNED);
        }
        else {
            setAction(credential, WSHandlerConstants.SAML_TOKEN_UNSIGNED);
        }
        credential.getRequestPropterties().put(WSHandlerConstants.SAML_PROP_FILE, this.samlPropFile);
        credential.getRequestPropterties().put(WSHandlerConstants.SAML_CALLBACK_REF, this.handler);
    }
}