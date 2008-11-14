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

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.dqp.client.PortableContext;
import com.metamatrix.dqp.message.RequestID;


/** 
 * @since 4.2
 */
class RequestContext implements PortableContext {
    
    private static final String HEADER = "RequestContext:"; //$NON-NLS-1$
    private RequestID requestID;
    private boolean update;
    
    RequestContext(RequestID id, boolean isUpdate) {
        this.requestID = id;
        this.update = isUpdate;
    }
    
    RequestContext(PortableContext context) throws MetaMatrixProcessingException {
        String[] parts = PortableStringUtil.getParts(context.getPortableString(), PortableStringUtil.PROP_SEPARATOR);
        if (parts == null || parts.length < 2 || parts.length > 3 || !parts[0].startsWith(HEADER)) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("RequestContext.invalid_context", context.getPortableString())); //$NON-NLS-1$
        }
        parts[0] = parts[0].substring(HEADER.length());
        update = Boolean.valueOf(PortableStringUtil.getParts(parts[0], PortableStringUtil.EQUALS)[1]).booleanValue();
        long executionID = Long.parseLong(PortableStringUtil.getParts(parts[1], PortableStringUtil.EQUALS)[1]);
        String connectionID = null;
        if (parts.length == 3) {
            connectionID = PortableStringUtil.unescapeString(PortableStringUtil.getParts(parts[2], PortableStringUtil.EQUALS)[1]);
        }
        this.requestID = new RequestID(connectionID, executionID);
    }

    /** 
     * @see com.metamatrix.dqp.client.PortableContext#getPortableString()
     * @since 4.2
     */
    public String getPortableString() {
        StringBuffer buf = new StringBuffer(HEADER)
        .append("update").append(PortableStringUtil.EQUALS).append(Boolean.toString(update)) //$NON-NLS-1$
        .append(PortableStringUtil.PROP_SEPARATOR).append("executionID").append(PortableStringUtil.EQUALS).append(Long.toString(requestID.getExecutionID())); //$NON-NLS-1$
        if (requestID.getConnectionID() != null) {
            buf.append(PortableStringUtil.PROP_SEPARATOR)
               .append("connectionID") //$NON-NLS-1$
               .append(PortableStringUtil.EQUALS)
               .append(PortableStringUtil.escapeString(requestID.getConnectionID()));
        }
        return buf.toString();
    }
    
    RequestID getRequestID() {
        return requestID;
    }
    
    boolean isUpdate() {
        return update;
    }
    
    static RequestContext createRequestContextFromPortableContext(PortableContext context) throws MetaMatrixProcessingException {
        if (context == null) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("RequestContext.null_context")); //$NON-NLS-1$
        } else if (context instanceof RequestContext) {
            return (RequestContext)context;
        }
        return new RequestContext(context);
    }

}
