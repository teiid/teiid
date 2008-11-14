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

package com.metamatrix.common.pooling.impl;


import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ResourceDescriptorID;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.pooling.api.Resource;
import com.metamatrix.common.pooling.api.ResourceAdapter;
import com.metamatrix.common.pooling.api.ResourcePool;
import com.metamatrix.common.pooling.api.ResourcePoolMgr;
import com.metamatrix.common.pooling.api.ResourcePoolPropertyNames;
import com.metamatrix.common.pooling.api.ResourcePoolStatistics;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.common.pooling.api.exception.ResourceWaitTimeOutException;
import com.metamatrix.common.pooling.util.PoolingUtil;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.PropertiesUtils;


/**
 * <p>
 * The ResourcePoolMgr is the center point for managing and sharing a common
 * object type among common users.  The benefit of managing an object as a
 * resource is so that common users can share the object without the overhead
 * of reoccurring object creation, desctruction and cleanup.  To utilize the
 * resource pooling feature the following needs to be done:
 * <ol>k
 *    <li><ul>Create {@link ResourceType}:</ul> the resource type categorically
 *        identifies the resource (e.g., JDBCConnectionResourceType).
 *        The resource type can provide defaults that will be applied to all the specific
 *        {@link ResourceDescriptor} implementations that are of that type.
 *        Therefore, there is a one-to-many relationship between the
 *        resource type and descriptor, respectfully.</li>
 *    <li><ul>Create {@link ResourceDescriptor}:/<ul> the descriptor describes the
 *        the resource and how it should be managed in the pool.  The descriptor
 *        also represents the identifier the ResourcePoolMgr uses to identify
 *        each pool.  The combination of the descriptor's name and
 *        {@link ResourceType} provides the unique identifier.</li>
 *    <li><ul>Create {@link Resource}:</ul> the resource is a wrapper class that will
 *        expose the necessary methods of the physical resource object.
 *        <b>The resource should be implemented by extending {@link
 *        com.metamatrix.common.pooling.impl.BaseResource BaseResource}.</b></li>
 *    <li><ul>Create {@link ResourceAdapter}:</ul> the adpater will be used by
 *        the {@link ResourcePool} to create new resource object instances, as
 *        well as, close existing instances. </li>
 * </ol>
 * </p>
 * <p>
 *  Once the appropriate set of classes have been implemented for a resource, the
 *  following are the steps for using a resource:
 *  <li>Instantiate the specific {@link ResourceDescriptor}
 *      and call {@link ResourcePoolMgr#getResource} to obtain a resource instance from the pool. </li>
 *  <li><b>When the user is finished with the resource, {@link Resource#closeResource} method
 *      must be called in order to release the resource back to the pool for reuse.</b></li>
 * </p>
 *
 *
 */
public class ResourcePoolMgrImpl implements ResourcePoolMgr {
/**
 * @link
 * @shapeType PatternLink
 * @pattern Singleton
 * @supplierRole Singleton factory
 */
    /*# private ResourcePoolMgr _resourcePoolMgr; */
    private static ResourcePoolMgrImpl instance = null;
    /**
     *@link aggregation
     *      @associates <b>ResourcePool</b>
     * @supplierCardinality 1..*
     */
  // provides a mapping between the ResourceDescriptor and the resource pool
  private Map resourcePools;

    // List locking
  private ReadWriteLock lock = new ReentrantReadWriteLock();

  // provides a mapping between the ResourceDescriptorID and the descriptor
  private Map idToDescriptorMap;

  private boolean shutdownRequested;

  private static final String CONTEXT = LogCommonConstants.CTX_POOLING;


public ResourcePoolMgrImpl() {
    // Exported from registry
}
/**
 * Call to get a resource from the pool defined by descriptor.
 * @param descriptor that describes the resource to obtain
 * @param userName of the one requesting the resource
 * @return Resource that is defined by the descriptor
 * @exception ResourcePoolException is thrown if an error ocurrs.
 *    Check for type {@link ResourceWaitTimeOutException}
 */
public Resource getResource(ResourceDescriptor descriptor, String userName) throws ResourcePoolException, RemoteException  {
	  return getInstance().r1$(descriptor, userName);

}


/**
 * Call to get id's for all the current resource pools.
 * @return Collection of type ResourcePoolIDs for all the resource pools.
 * @exception ResourcePoolException is thrown if an error ocurrs.
 *    Check for type {@link ResourceWaitTimeOutException}
 */
public Collection getAllResourceDescriptorIDs() throws ResourcePoolException, RemoteException   {
	  return getInstance().rd2$();

}


public Collection getAllResourceDescriptors() throws ResourcePoolException, RemoteException   {
	  return getInstance().rd1$();

}

public ResourceDescriptor getResourceDescriptor(ResourceDescriptorID descriptorID) throws ResourcePoolException, RemoteException {
      return getInstance().rd3$(descriptorID);
}



public void updateResourcePool(final ResourceDescriptorID descriptorID, Properties props) throws ResourcePoolException, RemoteException {

      LogManager.logTrace(CONTEXT, "Updating ResourcePool " + descriptorID.getName()); //$NON-NLS-1$

      ResourcePool rp = getResourcePool(descriptorID);

      if (rp != null) {

        rp.update(props);
        LogManager.logDetail(CONTEXT, "Update of ResourcePool " + descriptorID.getName() + " completed."); //$NON-NLS-1$ //$NON-NLS-2$

      } else {
        LogManager.logTrace(CONTEXT, "Unable to update resource pool, no resource pool found for " + descriptorID.getName() ); //$NON-NLS-1$

      }


}


public Collection getResourcePoolStatistics() throws ResourcePoolException, RemoteException {
    return getInstance().rps1$();
}


public ResourcePoolStatistics getResourcePoolStatistics(ResourceDescriptorID descriptorID) throws ResourcePoolException, RemoteException   {
    return getInstance().rps2$(descriptorID);
}

public Collection getResourcesStatisticsForPool(ResourceDescriptorID descriptorID) throws ResourcePoolException, RemoteException   {
    return getInstance().rsp1$(descriptorID);
}

public void shutDown(ResourceDescriptorID descriptorID) throws ResourcePoolException, RemoteException {
    LogManager.logTrace(CONTEXT, "Shutting down ResourcePool " + descriptorID.getName()); //$NON-NLS-1$


    getInstance().sd2$(descriptorID);

    LogManager.logDetail(CONTEXT, "Shut down ResourcePool " + descriptorID.getName() + " completed"); //$NON-NLS-1$ //$NON-NLS-2$

}

public void shutDown() throws ResourcePoolException, RemoteException  {
    LogManager.logTrace(CONTEXT, "Shutting down all ResourcePools"); //$NON-NLS-1$

    getInstance().sd1$();

    LogManager.logDetail(CONTEXT, "Shut down all ResourcePools completed"); //$NON-NLS-1$

}

/**
* Do Not call this method to update the resource pool
* This method was made available for testing
*/
public ResourcePool getResourcePool(ResourceDescriptorID descriptorID) throws ResourcePoolException {
      return getInstance().rp2$(descriptorID);

}


/**
* Do Not call this method to update the resource pool
* This method was made available for testing
*/
public ResourcePool getResourcePool(ResourceDescriptor descriptor) throws ResourcePoolException {

     return getInstance().rp1$(descriptor);

}

/**
* Do Not call this method to update the resource pool
* This method was made available for testing
*/
public Collection getResourcePools() throws ResourcePoolException {
      return getInstance().rp4$();
}


protected synchronized ResourcePoolMgrImpl getInstance() throws ResourcePoolException {
        if (instance == null) {

             ResourcePoolMgrImpl rpm = new ResourcePoolMgrImpl();
             rpm.init();
             instance = rpm;

        } else {
            if ( instance.isBeingShutDown() ) {
               throw new ResourcePoolException(ErrorMessageKeys.POOLING_ERR_0036, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0036));

            }
        }



        return instance;
    }

protected void init() throws ResourcePoolException {
    resourcePools = Collections.synchronizedMap(new HashMap());

    idToDescriptorMap = Collections.synchronizedMap(new HashMap());

    shutdownRequested = false;

}


private boolean isBeingShutDown() {
    return shutdownRequested;
}

protected void setIsBeingShutDown(boolean shutDown) {
    this.shutdownRequested = shutDown;
}


private final Resource r1$(final ResourceDescriptor descriptor, String userName) throws ResourcePoolException  {
    ResourcePool rp = null;
    LogManager.logTrace(CONTEXT, "Getting resource " + descriptor.getName() + " from pool for user " + userName); //$NON-NLS-1$ //$NON-NLS-2$

    if (resourcePools.isEmpty()) {
          LogManager.logTrace(CONTEXT, "Pool Is Empty"); //$NON-NLS-1$
    }

    // Acquire write lock
        lock.writeLock().lock();


        if (resourcePools.containsKey(descriptor.getID())) {
            LogManager.logTrace(CONTEXT, "Found Resource " + descriptor.getName() + " in pool."); //$NON-NLS-1$ //$NON-NLS-2$

            Object obj = resourcePools.get(descriptor.getID());
            rp = (ResourcePool) obj;

            lock.writeLock().unlock();


        } else {


    	    ResourceDescriptor cloneDescriptor = null;
        	try {


	        // clone the descriptor to ensure the requestor cannot change the
    	    // values later
        	    cloneDescriptor = (ResourceDescriptor) descriptor.clone();
	        } catch (Throwable t) {
                lock.writeLock().unlock();
                throw new ResourcePoolException(t, ErrorMessageKeys.POOLING_ERR_0037, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0037));
            }

            String poolClassName = cloneDescriptor.getProperty(ResourcePoolPropertyNames.RESOURCE_POOL_CLASS_NAME);

            try {

				rp = (ResourcePool) PoolingUtil.create(poolClassName, null);
/*
                Class poolFactoryClass = Class.forName(poolClassName);
     	        rp = (ResourcePool) poolFactoryClass.newInstance();
*/
                // initialize the pool
   	            rp.init(cloneDescriptor);
       	        resourcePools.put(cloneDescriptor.getID(), rp);

                idToDescriptorMap.put(cloneDescriptor.getID(), cloneDescriptor);

                LogManager.logDetail(CONTEXT, "New ResourcePool " + poolClassName + " created.\n" + //$NON-NLS-1$ //$NON-NLS-2$
                         PropertiesUtils.prettyPrint(cloneDescriptor.getProperties()));

	        } catch(ClassCastException e) {
            	throw new ResourcePoolException(e, ErrorMessageKeys.POOLING_ERR_0038, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0038,
            				new Object[] {poolClassName,ResourcePool.class.getName()} ));

  	        } finally {

                lock.writeLock().unlock();

            }
        }

    return rp.checkOut(userName);
}



// return a resource pool
protected ResourcePool rp1$(ResourceDescriptor descriptor) {


    ResourcePool rp = null;
    if (resourcePools.containsKey(descriptor.getID())) {
        Object obj = resourcePools.get(descriptor.getID());
        rp = (ResourcePool) obj;
    }

    return rp;

}

// return a resource pool
protected ResourcePool rp2$(ResourceDescriptorID descriptorID) {
        lock.readLock().lock();

    ResourcePool rp = null;

    if (resourcePools.containsKey(descriptorID)) {
        rp = (ResourcePool) resourcePools.get(descriptorID);
    }

    // Release read lock
    lock.readLock().unlock();

    return rp;

}

// return all resource descriptor ids
protected Collection rd2$() throws ResourcePoolException {

        lock.readLock().lock();

    Collection ids = new ArrayList(idToDescriptorMap.size());

    ids.addAll(idToDescriptorMap.keySet());

    lock.readLock().unlock();


    return ids;

}

// return all resource descriptors
protected Collection rd1$() throws ResourcePoolException {

        lock.readLock().lock();

    Collection ids = new ArrayList(idToDescriptorMap.size());

    ids.addAll(idToDescriptorMap.values());

    lock.readLock().unlock();

    return ids;

}

// return a specific resource descriptor
protected ResourceDescriptor rd3$(ResourceDescriptorID descriptorID) throws ResourcePoolException {

        lock.readLock().lock();

    ResourceDescriptor rd = null;


    if (idToDescriptorMap.containsKey(descriptorID)) {
         rd = (ResourceDescriptor) idToDescriptorMap.get(descriptorID);
    }


    lock.readLock().unlock();


    return rd;

}

// return all the resource pools
protected Collection rp4$() {
        lock.readLock().lock();

    Collection pools = resourcePools.values();

        // Release read lock
    lock.readLock().unlock();

    return pools;
}

// return all the resource pool statistics
protected Collection rps1$() throws ResourcePoolException {

    // Acquire write lock
        lock.readLock().lock();

    Collection stats = new ArrayList(resourcePools.size());

    Iterator it = resourcePools.keySet().iterator();
    while (it.hasNext()) {
        ResourceDescriptorID rdID = (ResourceDescriptorID) it.next();
        ResourcePool rp = (ResourcePool) resourcePools.get(rdID);
        stats.add( rp.getResourcePoolStatistics() );

    }


    lock.readLock().unlock();

    return stats;

}

// return the specific resource pool statistics for the descriptor ID
protected ResourcePoolStatistics rps2$(ResourceDescriptorID descriptorID) throws ResourcePoolException {

    ResourcePoolStatistics rps = null;
        lock.readLock().lock();

    ResourcePool rp = null;

    if (resourcePools.containsKey(descriptorID)) {
        rp = (ResourcePool) resourcePools.get(descriptorID);
        rps = rp.getResourcePoolStatistics();
    }


   lock.readLock().unlock();


    return rps;

}

// return the specific resource pool statistics for the descriptor ID
protected Collection rsp1$(ResourceDescriptorID descriptorID) throws ResourcePoolException {

    Collection rsp = null;
        lock.readLock().lock();

    ResourcePool rp = null;

    if (resourcePools.containsKey(descriptorID)) {
        rp = (ResourcePool) resourcePools.get(descriptorID);
        rsp = rp.getPoolResourceStatistics();
    }


   lock.readLock().unlock();


    return rsp;

}

// shutdown all the pools
protected void sd1$() throws ResourcePoolException {
    shutdownRequested = true;
    // Acquire write lock
        lock.writeLock().lock();


    Iterator it = resourcePools.keySet().iterator();
    while (it.hasNext()) {
        ResourceDescriptorID rdID = (ResourceDescriptorID) it.next();
        ResourcePool rp = (ResourcePool) resourcePools.get(rdID);
        rp.shutDown();
    }

    resourcePools.clear();
    idToDescriptorMap.clear();

    shutdownRequested = false;


    lock.writeLock().unlock();

}

// shutdow a specific pool
protected void sd2$(ResourceDescriptorID descriptorID) throws ResourcePoolException {


            // Acquire write lock
            lock.writeLock().lock();

        ResourcePool rp = null;

        if (resourcePools.containsKey(descriptorID)) {
            rp = (ResourcePool) resourcePools.get(descriptorID);

            rp.shutDown();
            resourcePools.remove(descriptorID);
            idToDescriptorMap.remove(descriptorID);
        }


        lock.writeLock().unlock();


}

}
