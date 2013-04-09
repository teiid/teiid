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

import java.io.IOException;
import java.security.MessageDigest;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.apache.ws.security.WSConstants;
import org.apache.ws.security.WSPasswordCallback;
import org.apache.ws.security.handler.WSHandlerConstants;
import org.apache.ws.security.message.token.UsernameToken;
import org.apache.ws.security.util.Base64;
import org.teiid.logging.LogManager;

/**
 * This class uses the WS-Security using standard OASIS Web Services Security
 * implemented by apache "WSS4J" Implements "Username Token Profile"
 */
public class UsernameTokenProfile extends WSSecurityToken implements CallbackHandler {
    private boolean encryptedPassword;
    protected String passwd;
    protected String user;

    public UsernameTokenProfile(String user, String passwd, boolean encryptedPassword){
        this.encryptedPassword = encryptedPassword;
        this.passwd = passwd;
        this.user = user;
        LogManager.logDetail(WSManagedConnectionFactory.UTIL.getString("using_username_profile")); //$NON-NLS-1$
    }

    @Override
    public void addSecurity(WSSecurityCredential credential) {
        setAction(credential, WSHandlerConstants.USERNAME_TOKEN);

        credential.getRequestPropterties().put(WSHandlerConstants.USER, this.user);
        if (this.encryptedPassword) {
        	credential.getRequestPropterties().put(UsernameToken.PASSWORD_TYPE, WSConstants.PW_DIGEST);
        }
        else {
        	credential.getRequestPropterties().put(UsernameToken.PASSWORD_TYPE, WSConstants.PW_TEXT);
        }
        credential.getRequestPropterties().put(WSHandlerConstants.PW_CALLBACK_REF, this);
    }

    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof WSPasswordCallback) {
                WSPasswordCallback pc = (WSPasswordCallback)callbacks[i];
                if (this.encryptedPassword) {
                    pc.setPassword(encrypt(this.passwd));
                }
                else {
                    pc.setPassword(this.passwd);
                }
            } else {
                throw new UnsupportedCallbackException(callbacks[i], "unrecognized_callback"); //$NON-NLS-1$
            }
        }
    }

    String encrypt(String clearText) {
        String sha1Hash = null;
        try {
          MessageDigest md = MessageDigest.getInstance("SHA1"); //$NON-NLS-1$
          byte[] digest = md.digest(clearText.getBytes());
          sha1Hash = new String(Base64.encode(digest));
        } catch (Exception e) {
          e.printStackTrace();
        }
        return sha1Hash;
    }

    @Override
    void handleCallback(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    	handle(callbacks);
    }
}