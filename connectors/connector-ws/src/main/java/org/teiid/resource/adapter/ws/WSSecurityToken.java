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

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.apache.ws.security.handler.WSHandlerConstants;
import org.teiid.core.TeiidRuntimeException;


/**
 * a base class handle WSSecurity
 */
public abstract class WSSecurityToken {
    private WSSecurityToken nextToken;
    private WSSecurityToken prevToken;

    Object getProperty(WSSecurityCredential credential, String name) {
        return credential.getRequestPropterties().get(name);
    }

    public void handleSecurity(WSSecurityCredential credential) {
        addSecurity(credential);
        if (this.nextToken != null) {
            this.nextToken.handleSecurity(credential);
        }
    }

    WSSecurityToken getNextToken() {
        return this.nextToken;
    }

    WSSecurityToken setNextToken(WSSecurityToken token) {
        this.nextToken = token;
        this.nextToken.prevToken = this;
        return token;
    }

    abstract void addSecurity(WSSecurityCredential credential);

    void setAction(WSSecurityCredential credential, String action) {
        String prev = (String)credential.getRequestPropterties().get(WSHandlerConstants.ACTION);
        if ((prev == null) || (prev.length() == 0)) {
        	credential.getRequestPropterties().put(WSHandlerConstants.ACTION, action);
        }
        else {
        	credential.getRequestPropterties().put(WSHandlerConstants.ACTION, prev+" "+action); //$NON-NLS-1$
        }
        credential.getResponsePropterties().put(WSHandlerConstants.ACTION, WSHandlerConstants.NO_SECURITY);
    }

    void handleCallback(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    	if (this.prevToken != null) {
    		this.prevToken.handleCallback(callbacks);
    	}
    	else {
    		throw new TeiidRuntimeException("No passwords defined for the profile"); //$NON-NLS-1$
    	}
    }
}
