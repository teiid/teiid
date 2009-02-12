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

package com.metamatrix.console.models;

import java.util.Collection;
import java.util.Iterator;

import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.pools.PoolConfigTableRowData;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;

public class ResourceManager extends Manager {
    public final static int NEXT_STARTUP_CONFIG = 1;
    public final static int STARTUP_CONFIG = 2;
    
    public ResourceManager(ConnectionInfo connection) {
        super(connection);
    }
    
    public PropertiedObject getPropertiedObjectForResourceDescriptor(
    		ComponentObject rd) {
        ConfigurationManager mgr  = ModelManager.getConfigurationManager(getConnection());
    	PropertiedObject po = mgr.getPropertiedObjectForComponentObject(rd);
    	return po;	
    }
    
	public ConfigurationID getConfigurationID(int type) 
			throws AuthorizationException, ExternalException {
	    ConfigurationID configID = null;
	    ConfigurationAdminAPI capi = ModelManager.getConfigurationAPI(
	    		getConnection());
	    try {
	        switch (type) {
				case NEXT_STARTUP_CONFIG:
                    configID = capi.getNextStartupConfigurationID();
					break;
				case STARTUP_CONFIG:
                    configID = capi.getStartupConfigurationID();
					break;
	    	}
            
	    } catch (AuthorizationException ex) {
	        throw ex;
	    } catch (Exception ex) {
	        throw new ExternalException(ex);
	    }
	    return configID;	        		            
	}
    
	public void updateResourceProperties(ResourcePropertiedObjectEditor editor)
			throws Exception {
        editor.apply();
	}
	
	public ResourcePropertiedObjectEditor getResourcePropertiedObjectEditor() 
			throws AuthorizationException, ExternalException {
	    ConfigurationID configID = getConfigurationID(NEXT_STARTUP_CONFIG);
		return new ResourcePropertiedObjectEditor(getConnection(), configID);
	}
	
	public SharedResource[] getResources() 
			throws AuthorizationException, ExternalException {
	    Collection /*<ResourceDescriptor>*/ r = readResources();

	    SharedResource[] rd = new SharedResource[r.size()];
	    Iterator it = r.iterator();
	    for (int i = 0; it.hasNext(); i++) {
	        rd[i] = (SharedResource)it.next();
	    }
	    return rd;
	}
    
//******************************
// Private internal methods
//******************************

    private Collection /*<ResourceDescriptor>*/ readResources()  
            throws AuthorizationException, ExternalException {
        ConfigurationAdminAPI capi = ModelManager.getConfigurationAPI(
        		getConnection());
        Collection /*<ResourceDescriptor>*/ r = null;
        try {
            r = capi.getResources();
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
        return r;
    }
    
    
	private boolean containsPool(
			Collection /*<ResourceDescriptor or PoolConfigTableRowData>*/pools,
			String poolName, String poolType) {
		boolean found = false;
		Iterator it = pools.iterator();
		while ((!found) && it.hasNext()) {
		    Object obj = it.next();
		    String curPoolName;
		    String curPoolType;
		    if (obj instanceof ResourceDescriptor) {
		        ResourceDescriptor rd = (ResourceDescriptor)obj;
    	    	curPoolName = rd.getName();
    	    	curPoolType = rd.getComponentTypeID().getFullName();
		    } else {
		        PoolConfigTableRowData row = (PoolConfigTableRowData)obj;
		        curPoolName = row.getPoolName();
		        curPoolType = row.getPoolType();
		    }
		    if (poolName.equals(curPoolName) && poolType.equals(curPoolType)) {
		        found = true;
		    }
		}
		return found;
	}
}//end PoolManager
