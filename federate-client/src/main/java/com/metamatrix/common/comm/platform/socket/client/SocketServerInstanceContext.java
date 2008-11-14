/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.comm.platform.socket.client;

import java.util.StringTokenizer;

import com.metamatrix.common.comm.api.ServerInstanceContext;

/**
 * Context information that identifies a ServerInstance to connect to.
 * @since 4.3
 */
public class SocketServerInstanceContext implements ServerInstanceContext {
    
    private String host;
    private int port;
    private boolean ssl;
    
    public SocketServerInstanceContext(String host, int port, boolean ssl) {
        this.host = host;
        this.port = port;
        this.ssl = ssl;
    }
    
    public SocketServerInstanceContext(String portableString) {
        StringTokenizer tokenizer = new StringTokenizer(portableString, ":");  //$NON-NLS-1$
        tokenizer.nextToken();
        this.host = tokenizer.nextToken();
        this.port = Integer.parseInt(tokenizer.nextToken());
        this.ssl = Boolean.valueOf(tokenizer.nextToken()).booleanValue();
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
	public boolean isSsl() {
		return this.ssl;
	}
    
    /** 
     * Get a string that can be passed around to represent this context. 
     * @return Portable string value representing the context
     * @since 4.3
     */
    public String getPortableString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("SSIContext:"); //$NON-NLS-1$
        buffer.append(host);
        buffer.append(":");  //$NON-NLS-1$
        buffer.append(port);
        return buffer.toString();
    }

}
