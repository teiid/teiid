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

import org.teiid.adminapi.Host;

import com.metamatrix.admin.AdminPlugin;


/**
 * Simple Host object for the Admin API as represented in the Configuration
 */
public final class MMHost extends MMAdminObject implements Host {

	private boolean running = false;
	
   
	
    /**
     * Constructor for creating a MMHost
     * 
     * @param identifierParts of the Host
     */
    public MMHost(String[] identifierParts) {
        super(identifierParts);
    }
    
	

	/**
     * Create a String for this MMHost
     *  
	 * @see java.lang.Object#toString()
	 * @since 4.3
	 */
	public String toString() {
		StringBuffer result = new StringBuffer();
		result.append(AdminPlugin.Util.getString("MMHost.MMHost")).append(getName()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMHost.Properties")).append(getPropertiesAsString()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMHost.Created")).append(getCreatedDate()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMHost.Created_By")).append(getCreatedBy()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMHost.Updated")).append(getLastChangedDate()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMHost.Updated_By")).append(getLastChangedBy()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMHost.IsRunning")).append(isRegistered()); //$NON-NLS-1$
		return result.toString();
	}

    
    
    /**
     *  Return true if this Host is running
     * @return if this Host is running
     * @since 4.3
     */
	public boolean isRunning() {
		return running;
	}
    
    /**
     * Set if this Host has been deployed and is executing
     * @param running
     * @since 4.3
     */
    public void setRunning(boolean running) {
        this.running = running;
    }
    
  
    
}