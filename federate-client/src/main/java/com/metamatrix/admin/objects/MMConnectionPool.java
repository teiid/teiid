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
import com.metamatrix.admin.api.objects.ConnectionPool;

/**
 * Dataholder for information about a Connection Pool 
 */
public final class MMConnectionPool extends MMAdminObject implements ConnectionPool {
	
    private String poolType;
	private boolean active;
    
   
    
    /**
     * Construct a new MMConnectionPool 
     * @param identifierParts
     * @since 4.3
     */
    public MMConnectionPool(String[] identifierParts) {
        super(identifierParts);
    }
    

    
    /**
     * Get the pool type. 
     * @return the pool type. 
     * @since 4.3
     */
	public String getType() {
		return poolType;
	}
    
    /**
     * Set the pool type. 
     * @param poolType
     * @since 4.3
     */
    public void setType(String poolType) {
        this.poolType = poolType;
    }
    
    
    /**
     * Get whether the pool is active. 
     * @return whether the pool is active. 
     * @since 4.3
     */    
    public boolean isActive() {
    	return active;
    }
    
    /**
     * Set the whether the pool is active.
     * @param active 
     * @since 4.3
     */    
    public void setActive(boolean active) {
        this.active = active;
    }
    
       
    

    /**
     * @see java.lang.Object#toString()
     * @since 4.3
     */
	public String toString() {
		StringBuffer result = new StringBuffer();
		result.append(AdminPlugin.Util.getString("MMConnectionPool.MMConnectionPool")).append(getIdentifier());  //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMConnectionPool.Type")).append(getType()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMConnectionPool.Active")).append(isActive()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMConnectionPool.Properties")).append(getPropertiesAsString());  //$NON-NLS-1$
        return result.toString();
	}
    
   
    /**
     * @see java.lang.Object#equals(java.lang.Object)
     * @since 4.3
     */
	public boolean equals(Object object) {
		boolean equals;
		if (object == null) {
			equals = false;
		} else if (object == this) {
			equals = true;
		} else if (!(object instanceof MMConnectionPool)) {
			equals = false;
		} else {
			MMConnectionPool cp = (MMConnectionPool)object;
			equals = (this.getName().equals(cp.getName()) && 
			          this.poolType.equals(cp.getType())  &&
			          this.active == cp.isActive());
		}
		return equals;
	}
  
    
}
