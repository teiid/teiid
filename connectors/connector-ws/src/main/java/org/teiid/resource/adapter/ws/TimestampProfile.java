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

import org.apache.ws.security.handler.WSHandlerConstants;
import org.teiid.logging.LogManager;

/**
 * Timestamp Profile using WSS4J
 */
public class TimestampProfile extends WSSecurityToken {
	private boolean inMilli = true;
	private int ttl;

    public TimestampProfile(int ttl, boolean inMilli) {
        this.inMilli = inMilli;
        this.ttl = ttl;

        LogManager.logDetail(WSManagedConnectionFactory.UTIL.getString("using_timestamp_profile")); //$NON-NLS-1$
    }

    @Override
    public void addSecurity(WSSecurityCredential credential) {
        setAction(credential, WSHandlerConstants.TIMESTAMP);

        // How long ( in seconds ) message is valid since send.
        credential.getRequestPropterties().put(WSHandlerConstants.TTL_TIMESTAMP, this.ttl);

        // if you do want to use millisecond precision set this to false; default true;
       	credential.getRequestPropterties().put(WSHandlerConstants.TIMESTAMP_PRECISION, Boolean.toString(this.inMilli));
    }
}