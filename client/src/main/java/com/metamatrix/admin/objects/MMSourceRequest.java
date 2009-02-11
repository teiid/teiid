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

package com.metamatrix.admin.objects;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.admin.api.objects.SourceRequest;

/**
 * Dataholder for information about SourceRequests.
 */
public class MMSourceRequest extends MMRequest implements SourceRequest {
	
	
    
    /**
     * Construct a new MMSourceRequest 
     * @param identifierParts
     * @since 4.3
     */
    public MMSourceRequest(String[] identifierParts) {
        super(identifierParts);
    }
    
	
    
    /** 
     * @return Returns whether this is a Source Request.
     * @since 4.3
     */
    public boolean isSource() {
        return true;
    }
    
    
    
	/**
	 * @see java.lang.Object#toString()
     * @return String for display purposes
	 */
	public String toString() {
		StringBuffer result = new StringBuffer();
		result.append(AdminPlugin.Util.getString("MMSourceRequest.MMSourceRequest")).append(getIdentifier());  //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMRequest.requestID")).append(getRequestID()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMRequest.userName")).append(userName); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMRequest.sessionID")).append(sessionID); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMRequest.created")).append(getCreatedString()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMRequest.processing")).append(getProcessingDateString()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMRequest.transactionID")).append(transactionID); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMRequest.command")).append(sqlCommand);  //$NON-NLS-1$
		return result.toString();
	}
	
	
   
   
}
