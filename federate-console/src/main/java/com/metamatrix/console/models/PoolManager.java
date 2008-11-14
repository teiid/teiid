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

package com.metamatrix.console.models;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ResourceDescriptorID;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.util.ConfigUtil;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.pooling.api.ResourcePool;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.pools.PoolConfigTableRowData;
import com.metamatrix.console.ui.views.pools.PoolNameAndType;
import com.metamatrix.console.ui.views.pools.PoolPropertiedObjectAndEditor;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.StaticQuickSorter;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;
import com.metamatrix.platform.admin.api.RuntimeStateAdminAPI;
import com.metamatrix.platform.admin.api.runtime.ResourcePoolStats;

public class PoolManager extends Manager {
    public final static int NEXT_STARTUP_CONFIG = 1;
    public final static int STARTUP_CONFIG = 2;
    
    private RuntimeStateAdminAPI api;
    
    public PoolManager(ConnectionInfo connection) {
        super(connection);
        api = ModelManager.getRuntimeStateAPI(getConnection());
    }
    
    public ResourcePoolStats[] getPoolStats() throws AuthorizationException,
    		ExternalException {
    	Collection /*<ResourcePoolStats>*/ poolStatsColl = null;
    	try {
			poolStatsColl = api.getResourcePoolStatistics();
		} catch (AuthorizationException ex) {
    	    throw ex;
    	} catch (Exception ex) {
    	    throw new ExternalException(ex);
    	}
    	ResourcePoolStats[] poolStats = new ResourcePoolStats[
    			poolStatsColl.size()];
		Iterator it = poolStatsColl.iterator();
    	for (int i = 0; it.hasNext(); i++) {
    	    poolStats[i] = (ResourcePoolStats)it.next();
		}
		return poolStats;
    }
    
    public PoolConfigTableRowData[] getPoolConfigData() 
    		throws AuthorizationException, ExternalException {
		Collection /*<ResourceDescriptor>*/ allNextStartupResourceDescriptors = 
				null;
		Collection /*<ResourceDescriptor>*/ allStartupResourceDescriptors =
				null;
		Map /*<String (name) to ResourceDescriptor>*/ startupRDsMap =
				new HashMap();
		Map /*<String (name) to ResourceDescriptor>*/ nextStartupRDsMap =
				new HashMap();
		Collection /*<ResourcePoolStats>*/ activePoolStats = null;
    	try {
    		ConfigurationID nextStartupConfigID = getConfigurationID(
    				NEXT_STARTUP_CONFIG);
   			allNextStartupResourceDescriptors = readConnectionPools(
   					nextStartupConfigID);
   			Iterator it = allNextStartupResourceDescriptors.iterator();
   			while (it.hasNext()) {
   				ResourceDescriptor rd = (ResourceDescriptor)it.next();
   				String key = rd.getName() + 
   						rd.getComponentTypeID().getFullName();
				nextStartupRDsMap.put(key, rd);
   			}
   			ConfigurationID startupConfigID = getConfigurationID(
   					STARTUP_CONFIG);
   			allStartupResourceDescriptors = readConnectionPools(
   					startupConfigID);
   			it = allStartupResourceDescriptors.iterator();
   			while (it.hasNext()) {
   				ResourceDescriptor rd = (ResourceDescriptor)it.next();
   				String key = rd.getName() + 
   						rd.getComponentTypeID().getFullName();
				startupRDsMap.put(key, rd);
   			}
   			activePoolStats = api.getResourcePoolStatistics();
		} catch (AuthorizationException ex) {
    	    throw ex;
    	} catch (Exception ex) {
    	    throw new ExternalException(ex);
    	}
    	Collection /*<PoolConfigTableRowData>*/ rows = new ArrayList(
    			allStartupResourceDescriptors.size() +
    			allNextStartupResourceDescriptors.size());
    	for (int i = 0; i <= 1; i++) {
    		Iterator it = null;
    		switch (i) {
    			case 0:
    				it = allStartupResourceDescriptors.iterator();
    				break;
    			case 1:
    				it = allNextStartupResourceDescriptors.iterator();
    				break;
    		}
    		while (it.hasNext()) {
    	    	ResourceDescriptor rd = (ResourceDescriptor)it.next();
    	    	String poolName = rd.getName();
    	    	String poolType = rd.getComponentTypeID().getFullName();
    	    	boolean alreadyInList = containsPool(rows, poolName, poolType);
				if (!alreadyInList) {
    	    		ResourcePoolStats stats = findStatsForPool(activePoolStats,
    	    				poolName, poolType, null);
    	    		boolean active = (stats != null);
    	    		String key = poolName + poolType;
    	    		ResourceDescriptor nextStartupRD = 
    	    				(ResourceDescriptor)nextStartupRDsMap.get(key);
					ResourceDescriptor startupRD =
    	    				(ResourceDescriptor)startupRDsMap.get(key);
					PoolConfigTableRowData rowData = new PoolConfigTableRowData(
    	        			poolName, poolType, active, nextStartupRD, 
    	        			startupRD);
    	        	rows.add(rowData);
    	    	}
    		}
    	}
    	PoolConfigTableRowData[] rowsArray = new PoolConfigTableRowData[rows.size()];
    	Iterator it = rows.iterator();
    	for (int i = 0; it.hasNext(); i++) {
    	    rowsArray[i] = (PoolConfigTableRowData)it.next();
    	}
    	return rowsArray;
    }
    
    /** 
     * returns the active resource pools
     */
    public PoolNameAndType[] getPools() throws AuthorizationException,
    		ExternalException {
    	PoolNameAndType[] pnt = null;
    	try {
    	    Collection /*<ResourceDescriptor>*/ descs = readMonitoredPools();
    	    pnt = new PoolNameAndType[descs.size()];
    	    Iterator it = descs.iterator();
    	    for (int i = 0; it.hasNext(); i++) {
    	        ResourceDescriptor rd = (ResourceDescriptor)it.next();
    	       	String poolName = rd.getName();
    	        String poolType = rd.getComponentTypeID().getFullName();
    	        pnt[i] = new PoolNameAndType(poolName, poolType, rd);
    	    }
    	} catch (AuthorizationException ex) {
    	    throw ex;
    	} catch (Exception ex) {
    	    throw new ExternalException(ex);
    	}
    	return pnt;
    }
    
    public String[] getPoolTypes() throws AuthorizationException,
    		ExternalException {
                
        ConfigurationAdminAPI capi = ModelManager.getConfigurationAPI(
        		getConnection());
        
        // which configuration to pass does not matter because the
        // types are based on configuration
        Collection poolTypes = null;
        try {
			poolTypes = capi.getPoolableResourcePoolTypes(
					Configuration.NEXT_STARTUP_ID);
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
        
        String[] unsorted = new String[poolTypes.size()];  
        int i = 0;             
        for (Iterator ptIt=poolTypes.iterator(); ptIt.hasNext(); ) {
            ComponentType type = (ComponentType) ptIt.next();
            unsorted[i] = type.getName();
            ++i;                            
        }
        
//    	PoolNameAndType[] pnt = getPools();
//    	java.util.List /*<String>*/ poolTypesColl = new ArrayList(pnt.length);
/*
    	for (int i = 0; i < pnt.length; i++) {
    	   	String poolType = pnt[i].getType();
    	   	if (poolTypesColl.indexOf(poolType) < 0) {
    	   	    poolTypesColl.add(poolType);
    	   	}
    	}
    	String[] unsorted = new String[poolTypesColl.size()];
    	Iterator it = poolTypesColl.iterator();
    	for (int i = 0; it.hasNext(); i++) {
    	    unsorted[i] = (String)it.next();
    	}
*/        
    	String[] sorted = StaticQuickSorter.quickStringSort(unsorted);
    	return sorted;
    }
    
    public PropertiedObject getPropertiedObjectForResourceDescriptor(
    		ComponentObject rd) {
        ConfigurationManager mgr  = ModelManager.getConfigurationManager(getConnection());
    	PropertiedObject po = mgr.getPropertiedObjectForComponentObject(rd);
    	return po;	
    }
    
    public PropertiedObjectEditor getPropertiedObjectEditorForPool(
    		ModificationActionQueue maq) {

        PropertiedObjectEditor poe = new ConfigurationPropertiedObjectEditor(getConnection());
        return poe;
    }
    
    public ConfigurationObjectEditor getConfigurationObjectEditor() 
    		throws AuthorizationException, ExternalException {
        ConfigurationAdminAPI capi = ModelManager.getConfigurationAPI(
        		getConnection());
        ConfigurationObjectEditor coe = null;
        try {
            coe = capi.createEditor();
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
        return coe;
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
//	
//	public void updatePoolProperties(ConfigurationObjectEditor editor)
//			throws Exception {
//	   	ModificationActionQueue maq = editor.getDestination();
//	   	java.util.List actions = maq.popActions();
//	   	ConfigurationAdminAPI capi = ModelManager.getConfigurationAPI(
//	   			getConnection());
//	   	capi.executeTransaction(actions);
//        
//    }
    
    public void updatePoolProperties(ConfigurationObjectEditor editor,PropertiedObject propertiedObject)
    throws Exception {
        ComponentObject co = (ComponentObject) propertiedObject;
        
        editor.modifyProperties(co, co.getProperties(), NEXT_STARTUP_CONFIG);

        ModificationActionQueue maq = editor.getDestination();
        java.util.List actions = maq.popActions();
        ConfigurationAdminAPI capi = ModelManager.getConfigurationAPI(
            getConnection());
        capi.executeTransaction(actions);

}    
            
    public void applyPropertiesToActivePool(PropertiedObject propertiedObject)
            throws Exception {    
                
        ComponentObject co = (ComponentObject) propertiedObject;
        ResourceDescriptorID id = (ResourceDescriptorID) co.getID();  
        
        api.updateResourcePool(id, co.getProperties());  
        
        
	}
	
	public void updateResourceProperties(ResourcePropertiedObjectEditor editor)
			throws Exception {
        editor.apply();
	}
	
    /**
     * tries to determine if the pool already exist in the configuration
     */
	public boolean poolExists(String poolName, ConfigurationID configID) 
			throws AuthorizationException, ExternalException {
	    boolean found = false;
	    Collection /*<ResourceDescriptor>*/ descs = null;
    	try {
    	    descs = readConnectionPools(configID);
    	} catch (AuthorizationException ex) {
    	    throw ex;
    	} catch (Exception ex) {
    	    throw new ExternalException(ex);
    	}
		Iterator it = descs.iterator();
    	while ((!found) && it.hasNext()) {
    	    ResourceDescriptor rd = (ResourceDescriptor)it.next();
    	    String name = rd.getName();
    	    if (poolName.equals(name)) {
    	        found = true;
    	    }
    	}
    	return found;
    }
    
	public ResourceDescriptor getConnectionPool(String poolName, ConfigurationID configID) 
			throws AuthorizationException, ExternalException {
		boolean found = false;
		Collection /*<ResourceDescriptor>*/ descs = null;
		try {
			descs = readConnectionPools(configID);
		} catch (AuthorizationException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new ExternalException(ex);
		}
		Iterator it = descs.iterator();
		while ((!found) && it.hasNext()) {
			ResourceDescriptor rd = (ResourceDescriptor)it.next();
			if (poolName.equals(rd.getName())) {
				return rd;
			}
		}
		return null;
	}
	
	public ComponentType getPoolType(String poolTypeName) throws AuthorizationException,
			ExternalException {
                
		ConfigurationManager mgr  = ModelManager.getConfigurationManager(getConnection());

		ComponentType type = mgr.getConfigModel(Configuration.NEXT_STARTUP_ID).getComponentType(poolTypeName);        
		return type;
	}
	
    
	
	public PoolPropertiedObjectAndEditor createPropertiedObjectForPool(
			String poolName, String poolType, ConfigurationID nextStartup)
			throws AuthorizationException, ExternalException {
		ConfigurationObjectEditor coe = getConfigurationObjectEditor();	
		
		ConfigurationManager mgr  = ModelManager.getConfigurationManager(getConnection());

		ModificationActionQueue maq = coe.getDestination();

		ComponentType type = this.getPoolType(poolType);
						
		ComponentTypeID poolTypeID = (ComponentTypeID) type.getID(); 

        ResourceDescriptor pool = coe.createResourceDescriptor(nextStartup, 
				poolTypeID, poolName);
				
	    Properties props = ConfigUtil.buildDefaultPropertyValues(poolTypeID, mgr.getConfigModel(Configuration.NEXT_STARTUP_ID) );
				
		pool = (ResourceDescriptor) coe.modifyProperties(pool, props, ConfigurationObjectEditor.ADD);
						
		PropertiedObject poolPO = mgr.getPropertiedObjectForComponentObject(pool);
		
		PropertiedObjectEditor poolPOE = new ConfigurationPropertiedObjectEditor(getConnection(), maq);
		
		PoolPropertiedObjectAndEditor result = new PoolPropertiedObjectAndEditor(
				poolName, poolType, poolPO, poolPOE, pool, maq);

		return result;
	}
	
	public PoolNameAndType createPool(PoolPropertiedObjectAndEditor data, 
			ResourceDescriptor[] resourcesToAddToPool)
			throws AuthorizationException, ExternalException {

//		PropertiedObjectEditor poe =
            data.getEditor();
		try {
    		ConfigurationAdminAPI capi = ModelManager.getConfigurationAPI(getConnection());
            ConfigurationManager mgr  = ModelManager.getConfigurationManager(getConnection());

    		ModificationActionQueue maq = data.getModificationActionQueue();
    		//clone pool object
        	ConfigurationObjectEditor coe = getConfigurationObjectEditor();
        	coe.setDestination(maq);
        	ResourceDescriptor pool = data.getPool();
        	
//*** DO NOT try to create the resource pool here because the create
// 	actually occured when {@see #createPropertiedObjectForPool} was called
//  otherwise a duplicate pool exception will be thrown
         	
//    		ConfigurationID nextStartupID = getConfigurationID(NEXT_STARTUP_CONFIG);
//			ResourceDescriptor pool2 = coe.createResourceDescriptor(nextStartupID, 
//    				pool.getComponentTypeID(), pool.getName());
    		coe.modifyProperties(pool, pool.getProperties(), 
    				ConfigurationObjectEditor.ADD);
    		capi.executeTransaction(maq.popActions());
    		if (resourcesToAddToPool != null) {
                String poolName = data.getPoolName();
    			for (int i = 0; i < resourcesToAddToPool.length; i++) {
    				mgr.updateResourcePropertyValue(resourcesToAddToPool[i],
                            ResourcePool.RESOURCE_POOL, poolName);
                }
            }
		} catch (AuthorizationException ex) {
			throw ex;
		} catch (Exception ex) {
		    throw new ExternalException(ex);
		}
		PoolNameAndType pnt = new PoolNameAndType(data.getPoolName(),
				data.getPoolType(), data.getPool());
		return pnt;
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
    
    public ResourceDescriptor[] getResourceDescriptors() 
    		throws AuthorizationException, ExternalException {
    	Collection /*<ResourceDescriptor>*/ r = readResources();
    	ResourceDescriptor[] rd = new ResourceDescriptor[r.size()];
    	Iterator it = r.iterator();
    	for (int i = 0; it.hasNext(); i++) {
    		rd[i] = (ResourceDescriptor)it.next();
    	}
    	return rd;
    }
    
    /**
     * This method will perform the following validation to confirm
     * if the resource pool can be deleted:
     * <li> Is a resource currently assigned to this connection pool
     * <li> Is the connection pool currently active and monitored
     * If either one is true, then the pool cannot be deleted.
     * @param id is the id of the pool
     * @return String containing a message of why the pool cannot be deleted,
     * otherwise the String will be null if the pool can be deleted.
     */
//    private static final String POOL_IS_ASSIGNED_ERROR_MSG = "Cannot delete pool, it is currently assigned to resource ";
//    private static final String POOL_IS_ACTIVE_ERROR_MSG = "Cannot delete pool, it is actively being used.";
/*
    public String canConnectionPoolConfigBeDeleted(String connectionPoolName) 
            throws AuthorizationException, ExternalException {
                                              
        Collection  r = readResources();    //<ResourceDescriptor>
        Iterator it = r.iterator();
        for (int i = 0; it.hasNext(); i++) {
            ResourceDescriptor rd = (ResourceDescriptor)it.next();
            String poolName = rd.getProperty(ResourcePool.RESOURCE_POOL);
            // not all resources use pools
            if (poolName == null) {
                continue;
            }
            
            if (poolName.equals(connectionPoolName)) {
                return POOL_IS_ASSIGNED_ERROR_MSG + rd.getName();
            }
        }
        
        Collection  m = readMonitoredPools();    // <ResourceDescriptor>
        Iterator itm = m.iterator();
        for (int i = 0; itm.hasNext(); i++) {
            ResourceDescriptor rd = (ResourceDescriptor)itm.next();
             
            if (connectionPoolName.equals(rd.getName())) {
                return POOL_IS_ACTIVE_ERROR_MSG;
            }
        }
        
        
        return null;
                
    }                
 */       
	
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
    
    
    private Collection readConnectionPools(ConfigurationID configID) 
            throws AuthorizationException, ExternalException {
        ConfigurationAdminAPI capi = ModelManager.getConfigurationAPI(
        		getConnection());
        Collection /*<ResourceDescriptor>*/ r = null;
        try {
            r = capi.getResourcePools(configID);
            return r;
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
        
    }  
    
    private Collection readMonitoredPools() 
            throws AuthorizationException, ExternalException {
        Collection /*<ResourceDescriptor>*/ descs = null;          
        try {
          descs = api.getResourceDescriptors();
          return descs;
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
        
    }       
    
	private ResourcePoolStats findStatsForPool(
			Collection /*<ResourcePoolStats>*/ pools, String poolName,
			String poolType, String stripPrefix) {
		ResourcePoolStats result = null;
		Iterator it = pools.iterator();
		while (it.hasNext() && (result == null)) {
		    ResourcePoolStats pool = (ResourcePoolStats)it.next();
		    String curPoolName = pool.getPoolName();
		    if ((stripPrefix != null) && (curPoolName.indexOf(stripPrefix) == 
		    		0)) {
		        curPoolName = curPoolName.substring(stripPrefix.length());
		    }
		    String curPoolType = pool.getPoolType();
		    if (poolName.equals(curPoolName) && poolType.equals(curPoolType)) {
		        result = pool;
		    }
		}
		return result;
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
