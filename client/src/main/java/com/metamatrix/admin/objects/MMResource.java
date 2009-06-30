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

package com.metamatrix.admin.objects;

import org.teiid.adminapi.Resource;

import com.metamatrix.admin.AdminPlugin;

/**
 * Dataholder for a resource.
 */
public class MMResource extends MMAdminObject implements Resource {

    private String resourceType;
	    
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
        result.append(AdminPlugin.Util.getString("MMResource.Created")).append(getCreatedDate()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMResource.Created_By")).append(getCreatedBy()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMResource.Updated")).append(getLastChangedDate()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMResource.Updated_By")).append(getLastChangedBy()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMResource.Properties")).append(getPropertiesAsString()); //$NON-NLS-1$
		return result.toString();
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
