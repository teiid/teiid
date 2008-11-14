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

package com.metamatrix.dqp.client.impl;

import java.util.Properties;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.dqp.client.PortableContext;


/** 
 * @since 4.2
 */
class ServerSessionContext implements PortableContext {
    
    private static final String HEADER = "ServerSessionContext:"; //$NON-NLS-1$
    
    private ServerConnectionInfo connectionInfo;
    private String connectionContext;
    /* Optional instance context*/
    private String instanceContext;
    
    
    ServerSessionContext(ServerConnectionInfo connInfo, String connectionContext) {
        this.connectionInfo = connInfo;
        this.connectionContext = connectionContext;
    }
    
    ServerSessionContext(PortableContext context) throws MetaMatrixProcessingException {
        String[] parts = PortableStringUtil.getParts(context.getPortableString(), PortableStringUtil.CTX_SEPARATOR);
        if (parts == null || parts.length !=2 || !parts[0].startsWith(HEADER)) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerSessionContext.invalid_context", context.getPortableString())); //$NON-NLS-1$
        }
        parts[0] = parts[0].substring(HEADER.length());
        
        this.connectionInfo = new ServerConnectionInfo(parts[0]);
        this.connectionContext = PortableStringUtil.unescapeString(parts[1]);
        if (parts.length == 3) {
            this.instanceContext = PortableStringUtil.unescapeString(parts[2]);
        }
    }
    
    String getConnectionContext() {
        return connectionContext;
    }
    
    Properties getConnectionProperties() {
        return connectionInfo.getConnectionProperties();
    }
    
    void setInstanceContext(String context) {
        this.instanceContext = context;
    }
    
    String getInstanceContext() {
        return instanceContext;
    }
    
    String getUserName() {
        return connectionInfo.getUser();
    }
    
    String getVDBName() {
        return connectionInfo.getVDBName();
    }
    
    String getVDBVersion() {
        return connectionInfo.getVDBVersion();
    }
    
    
    public String getPortableString() {
        StringBuffer buf = new StringBuffer(HEADER)
            .append(connectionInfo.getPortableString())
            .append(PortableStringUtil.CTX_SEPARATOR)
            .append(PortableStringUtil.escapeString(connectionContext));
        if (instanceContext != null) {
            buf.append(PortableStringUtil.CTX_SEPARATOR)
            .append(PortableStringUtil.escapeString(instanceContext));

        }
        return buf.toString();
    }
    
    public int hashCode() {
        int hash = HashCodeUtil.hashCode(0, connectionInfo);
        return HashCodeUtil.hashCode(hash, connectionContext);
    }
    
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof ServerSessionContext)) {
            return false;
        }
        ServerSessionContext context = (ServerSessionContext)obj;
        return EquivalenceUtil.areEqual(this.connectionInfo, context.connectionInfo) &&
                EquivalenceUtil.areEqual(this.connectionContext, context.connectionContext);
    }
    
    static ServerSessionContext createSessionContextFromPortableContext(PortableContext context) throws MetaMatrixProcessingException {
        if (context == null) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerSessionContext.null_context")); //$NON-NLS-1$
        } else if (context instanceof ServerSessionContext) {
            return (ServerSessionContext)context;
        }
        return new ServerSessionContext(context);
    }
}
