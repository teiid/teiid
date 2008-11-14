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
import com.metamatrix.admin.api.objects.Resource;

/**
 * Dataholder for a resource.
 */
public class MMResource extends MMAdminObject implements Resource {

    private String resourceType;
    private String connectionPoolIdentifier;
	    
    /**
     * Constructor
     * @param identifierParts of the resource
     */
    public MMResource(String[] identifierParts) {
        super(identifierParts);        
    }
    
    
	/**
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuffer result = new StringBuffer();
		result.append(AdminPlugin.Util.getString("MMResource.MMResource")).append(getIdentifier());  //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMResource.Type")).append(resourceType); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMResource.ConnectionPoolIdentifier")).append(connectionPoolIdentifier); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMResource.Created")).append(getCreatedDate()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMResource.Created_By")).append(getCreatedBy()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMResource.Updated")).append(getLastChangedDate()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMResource.Updated_By")).append(getLastChangedBy()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMResource.Properties")).append(getPropertiesAsString()); //$NON-NLS-1$
		return result.toString();
	}


    
    /** 
     * Return the Identifier for the Connection Pool Assigned
     * 
     * @return Returns the String Identifier for the Assigned Connection Pool.
     * @since 4.3
     */
    public String getConnectionPoolIdentifier() {
        return this.connectionPoolIdentifier;
    }


    
    /**
     * Set the Connection Pool Assigned Identifier
     *   
     * @param connectionPoolIdentifier The Identifier of the Connection Pool to set.
     * @since 4.3
     */
    public void setConnectionPoolIdentifier(String connectionPoolIdentifier) {
        this.connectionPoolIdentifier = connectionPoolIdentifier;
    }


    
    /** 
     * Get the Resource Type for this Resource 
     * @return Returns the String value of the Resource Type for this Resouce.
     * @since 4.3
     */
    public String getResourceType() {
        return this.resourceType;
    }


    
    /** 
     * Set the Resource Type 
     * @param resourceType The Resource Type to set.
     * @since 4.3
     */
    public void setResourceType(String resourceType) {
        this.resourceType = resourceType;
    }
    
    
}
