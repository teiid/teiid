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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ResourceDescriptorID;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.pooling.api.PoolStatistic;
import com.metamatrix.common.pooling.api.Resource;
import com.metamatrix.common.pooling.api.ResourceAdapter;
import com.metamatrix.common.pooling.api.ResourceContainer;
import com.metamatrix.common.pooling.api.ResourcePool;
import com.metamatrix.common.pooling.api.ResourcePoolPropertyNames;
import com.metamatrix.common.pooling.api.ResourcePoolStatisticNames;
import com.metamatrix.common.pooling.api.ResourcePoolStatistics;
import com.metamatrix.common.pooling.api.ResourceStatistics;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.common.pooling.api.exception.ResourceWaitTimeOutException;
import com.metamatrix.common.pooling.impl.statistics.CounterStat;
import com.metamatrix.common.pooling.impl.statistics.HighestValueStat;
import com.metamatrix.common.pooling.util.PoolingUtil;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.ArgCheck;

/**
 *  NOTE: Currently, the pool does not use the ConcurrentUser option
 * The moving of a container from the resourcepool to the inuseresourcepool
 * is done when a resource is checkedout and backin
 * The logic doesn't consider the concurrent user option
 *
 * History:
 * Who          When    Why
 * =================================================================
 * V.Halbert    5/15/03  remove the isAlive checking due to db overhead
 *                       just let the cleanup thread perform the isAlive cleanup
 */

public class BasicResourcePool implements ResourcePool {

    // descriptor used to create the pool
    protected ResourceDescriptor resourceDescriptor;

    // these properties will be the complete editable set
    // because the resourceDescriptor returns an unmodifiable set
    private Properties properties;

    // this will an unmodifiable set of properties to be passed to each resource instance

//***************************************************
    // variables that control resource management
    private int minimum_pool_size;
    private int maximum_pool_size;
    private int num_concurrent_users_allowed;
    private long liveAndUsedTime;
    private long shrinkPeriod;
    private int shrinkIncrement;
    private boolean allowsShrinking = true;
    private boolean extendMode = false;
    private double extendPercent = 0.0;
    private int extend_maximum_pool_size;
    private long waitForResourceTime;
//***************************************************

    private long containerCounter = 0;

    // adapter for creating physical resource instances
    private ResourceAdapter resourceAdapter;

    // List locking
    ReadWriteLock lock = new ReentrantReadWriteLock();


    private CleanUpThread cleanerThread;
    private boolean shutdownRequested = false;

    //contains all the containers in the bool
    private Set  resourcePool               = Collections.synchronizedSet( new HashSet(25) );
    private Set  inuseResourcePool           = Collections.synchronizedSet( new HashSet(25) );

    protected static final String CONTEXT = LogCommonConstants.CTX_POOLING;


    // variables used in statistics
    private BasicResourcePoolStatistics poolStatistics;

//    private static Number addOne;
//    private static Number subtractOne;

    private static BasicConfigurationObjectEditor editor = new BasicConfigurationObjectEditor();

    public void init(ResourceDescriptor descriptor) throws ResourcePoolException {
        if(descriptor == null){
            ArgCheck.isNotNull(descriptor, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0015));
        }


        lock.writeLock().lock();

        try {
            this.resourceDescriptor = descriptor;

            initProperties();

            initStatistics();

            initResources();


            // Create the cleaner thread
            cleanerThread = new CleanUpThread( this, this.shrinkPeriod );
            cleanerThread.start();


        } catch (ResourcePoolException e) {
               LogManager.logError(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0016,this.resourceDescriptor.getName()));
               throw e;
        } finally {
              lock.writeLock().unlock();
        }

    }



    public ResourceAdapter getResourceAdapter() {
        return this.resourceAdapter;
    }

    public int getResourcePoolSize() {
        return this.resourcePool.size() + this.inuseResourcePool.size();
    }

/**
 * Returns the {@link ResourceDescriptor descriptor} from which resources in the were created.
 * @return ResourceDescriptor used to create the resources in the pool
 */
    public ResourceDescriptor getResourceDescriptor() {
        return resourceDescriptor;
    }

    public ResourceDescriptorID getResourceDescriptorID() {
    	return (ResourceDescriptorID) resourceDescriptor.getID();
    }

    public ComponentTypeID getComponentTypeID() {
        return resourceDescriptor.getComponentTypeID();
    }

/**
 * Returns the monitor for this pool.
 * @return ResourcePoolMonitor that monitors this pool
 */
    public ResourcePoolStatistics getResourcePoolStatistics(){
        return this.poolStatistics;
    }

/**
 * Returns list of resourceStatistics for all resources in the pool.
 * @return List
 */
    public Collection getPoolResourceStatistics() {
        Collection stats = new ArrayList();

        Set inusePool = new HashSet(this.inuseResourcePool);
        Set pool = new HashSet(this.resourcePool);

        Iterator iter = inusePool.iterator();
        while (iter.hasNext()) {
            ResourceContainer rc = (ResourceContainer) iter.next();
            stats.add(rc.getStats());
        }
        iter = pool.iterator();
        while (iter.hasNext()) {
            ResourceContainer rc = (ResourceContainer) iter.next();
            stats.add(rc.getStats());
        }
        return stats;
    }

/**
 * Call to shutDown all the resources in the pool.
 * Each resource will have the method {@link Resource.shutDown} called.
 */
    public synchronized void shutDown(){
        String pn = resourceDescriptor.getName();
        this.shutdownRequested = true;

        try {
            if (cleanerThread != null) {
                cleanerThread.stopCleanup();

            }
        } catch (Exception e) {
               LogManager.logError(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0017,pn));
        } finally {
        	cleanerThread = null;
        }

        try {
            if (this.resourcePool != null) {
                closeContainers(this.resourcePool);
            }
        } catch (Exception e) {
              	LogManager.logError(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0017,pn));
        } finally {

            this.resourcePool.clear();

        }




        try {
            if (this.inuseResourcePool != null) {
                closeContainers(this.inuseResourcePool);
            }
        } catch (Exception e) {
        		LogManager.logError(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0017,pn));
        } finally {
        	this.inuseResourcePool.clear();

            resourceDescriptor = null;
            poolStatistics = null;
            resourceAdapter = null;

        }

    }


    public synchronized void update(Properties properties) throws ResourcePoolException{
        if (properties == null || properties.isEmpty()) {
                return;
        }

        try {
            Properties props = PropertiesUtils.clone(properties, false);

            props = updatePoolProperties(props);

            editor.modifyProperties(resourceDescriptor, props, BasicConfigurationObjectEditor.ADD);


            // save the changes to the keeper of the properties
            this.properties.putAll(props);

        } catch (Exception e) {
        		LogManager.logError(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0018, this.resourceDescriptor.getName()));
               throw new ResourcePoolException(e, ErrorMessageKeys.POOLING_ERR_0018, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0018, this.resourceDescriptor.getName()));
        }

    }

    /**
     * The update of pool properties applies a change only for those properties
     * passed.  Otherwise, the current value remains the active value used.
     */
    private Properties updatePoolProperties(Properties props) {
		Properties changes = new Properties();
        // Check for the required parameters ...
        String maximumSizeParm = props.getProperty(ResourcePoolPropertyNames.MAXIMUM_RESOURCE_POOL_SIZE);
        if ( maximumSizeParm != null && maximumSizeParm.length() > 0) {
 		       try {
    		        this.maximum_pool_size = Integer.parseInt(maximumSizeParm);
    		    } catch ( Exception e ) {
      		      throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {maximumSizeParm, ResourcePoolPropertyNames.MAXIMUM_RESOURCE_POOL_SIZE, "long"})); //$NON-NLS-1$
       			}
        }


        String minimumSizeParm = props.getProperty(ResourcePoolPropertyNames.MINIMUM_RESOURCE_POOL_SIZE);
        if ( minimumSizeParm != null && minimumSizeParm.length() > 0) {
        		try {
            		this.minimum_pool_size = Integer.parseInt(minimumSizeParm);
        		} catch ( Exception e ) {
            		throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {minimumSizeParm, ResourcePoolPropertyNames.MINIMUM_RESOURCE_POOL_SIZE, "long"})); //$NON-NLS-1$
        		}
        }

        if (this.minimum_pool_size > this.maximum_pool_size) {
            int tmp = this.maximum_pool_size;
            this.maximum_pool_size = this.minimum_pool_size;
            this.minimum_pool_size = tmp;
        }

        changes.setProperty(ResourcePoolPropertyNames.MINIMUM_RESOURCE_POOL_SIZE, String.valueOf(this.minimum_pool_size));
        changes.setProperty(ResourcePoolPropertyNames.MAXIMUM_RESOURCE_POOL_SIZE, String.valueOf(this.maximum_pool_size));


        String liveAndUsedTimeParm = props.getProperty(ResourcePoolPropertyNames.LIVE_AND_UNUSED_TIME);
        if ( liveAndUsedTimeParm != null && liveAndUsedTimeParm.length() > 0) {

        		try {
            		this.liveAndUsedTime = Long.parseLong(liveAndUsedTimeParm);
                    changes.setProperty(ResourcePoolPropertyNames.LIVE_AND_UNUSED_TIME, liveAndUsedTimeParm);
        		} catch ( Exception e ) {
            		throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {liveAndUsedTimeParm, ResourcePoolPropertyNames.LIVE_AND_UNUSED_TIME, "long"})); //$NON-NLS-1$
         		}
        }


        String shrinkPeriodParm = props.getProperty(ResourcePoolPropertyNames.SHRINK_PERIOD);
        if ( shrinkPeriodParm != null && shrinkPeriodParm.length() > 0) {
        	try {
            	this.shrinkPeriod = Long.parseLong(shrinkPeriodParm);

                cleanerThread.setSleepTime(shrinkPeriod);
                changes.setProperty(ResourcePoolPropertyNames.SHRINK_PERIOD, shrinkPeriodParm);
        	} catch ( Exception e ) {
            	throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {shrinkPeriodParm, ResourcePoolPropertyNames.SHRINK_PERIOD, "long"})); //$NON-NLS-1$
        	}
        }


        String allowShrinkParm = props.getProperty(ResourcePoolPropertyNames.ALLOW_SHRINKING);
        if ( allowShrinkParm != null && allowShrinkParm.length() > 0) {
        	try {
            	this.allowsShrinking = Boolean.valueOf(allowShrinkParm).booleanValue();
        	    changes.setProperty(ResourcePoolPropertyNames.ALLOW_SHRINKING, allowShrinkParm);

            } catch ( Exception e ) {
            	throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {allowShrinkParm, ResourcePoolPropertyNames.ALLOW_SHRINKING, "boolean"})); //$NON-NLS-1$
        	}
        }


        String shrinkIncrementParm = props.getProperty(ResourcePoolPropertyNames.SHRINK_INCREMENT);
        if ( shrinkIncrementParm != null && shrinkIncrementParm.length() > 0) {
        	try {
            	this.shrinkIncrement = Integer.parseInt(shrinkIncrementParm);
                changes.setProperty(ResourcePoolPropertyNames.SHRINK_INCREMENT, shrinkIncrementParm);
        	} catch ( Exception e ) {
            	throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {shrinkIncrementParm, ResourcePoolPropertyNames.SHRINK_INCREMENT, "integer"})); //$NON-NLS-1$
        	}
        }



        String waitTimeParm = props.getProperty(ResourcePoolPropertyNames.WAIT_TIME_FOR_RESOURCE);
        if ( waitTimeParm != null && waitTimeParm.length() > 0) {
        	try {
            	this.waitForResourceTime = Long.parseLong(waitTimeParm);
                changes.setProperty(ResourcePoolPropertyNames.WAIT_TIME_FOR_RESOURCE, waitTimeParm);
        	} catch ( Exception e ) {
            	throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {waitTimeParm, ResourcePoolPropertyNames.WAIT_TIME_FOR_RESOURCE, "long"})); //$NON-NLS-1$
        	}
        }


        String concurrentUsersValue = props.getProperty(ResourcePoolPropertyNames.NUM_OF_CONCURRENT_USERS);
        if ( concurrentUsersValue != null && concurrentUsersValue.length() > 0) {
        	try {
            	this.num_concurrent_users_allowed = Integer.parseInt(concurrentUsersValue);
                changes.setProperty(ResourcePoolPropertyNames.NUM_OF_CONCURRENT_USERS, concurrentUsersValue);
        	} catch ( Exception e ) {
            	throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {concurrentUsersValue, ResourcePoolPropertyNames.NUM_OF_CONCURRENT_USERS, "integer"})); //$NON-NLS-1$
        	}
        }


        String extendModeParm = props.getProperty(ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_MODE);
        if ( extendModeParm != null && extendModeParm.length() > 0) {
        	try {
            	this.extendMode = new Boolean(extendModeParm).booleanValue();
                changes.setProperty(ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_MODE, extendModeParm);

        	} catch ( Exception e ) {
            	throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {extendModeParm, ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_MODE, "boolean"})); //$NON-NLS-1$
        	}
        }


        String extendPercentParm = props.getProperty(ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_PERCENT);
        if ( extendPercentParm != null && extendPercentParm.length() > 0) {
        	try {
            	this.extendPercent = Double.parseDouble(extendPercentParm);
                changes.setProperty(ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_PERCENT, extendPercentParm);

        	} catch ( Exception e ) {
            	throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {extendPercentParm, ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_PERCENT, "long"})); //$NON-NLS-1$
        	}
        }

        // calculate the extend max size
        this.extend_maximum_pool_size = (new Double(this.maximum_pool_size * this.extendPercent)).intValue() + this.maximum_pool_size;


        return changes;

    }

    protected void finalize() {
        this.shutDown();
    }

    protected Resource checkResourceCache() {
          return null;

    }


   /**
   *  Initialize the pool properties based on 3 different sources
   *  of properties (applied in the order):
   *    1.  ResourceType defined overrides, this allow for the property
   *          to be set for all descriptors of this type.
   *    2.  ResourceDescriptor / PoolInfo, this is resource pool specific
   */
    private final void initProperties() throws ResourcePoolException {

        Properties descriptorProperties = resourceDescriptor.getProperties();


        // Create the proper factory instance
        String resourceAdapterClassName = descriptorProperties.getProperty(ResourcePoolPropertyNames.RESOURCE_ADAPTER_CLASS_NAME);
        try {
			this.resourceAdapter = (ResourceAdapter) PoolingUtil.create(resourceAdapterClassName, null);
        } catch(ClassCastException e) {
            throw new ResourcePoolException(e, ErrorMessageKeys.POOLING_ERR_0038, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0038,
            				new Object[] {resourceAdapterClassName,ResourceAdapter.class.getName()} ));
        }

        setupPoolProperties(descriptorProperties);
    }

    /**
     * The update of the properties here utilize the internal resource
     * pool defaults if a property is not passed
     */
    private void setupPoolProperties(Properties descriptorProperties) {

        properties = PropertiesUtils.clone(descriptorProperties, false);

        // Check for the required parameters ...
        String maximumSizeParm = properties.getProperty(ResourcePoolPropertyNames.MAXIMUM_RESOURCE_POOL_SIZE);
        if ( maximumSizeParm == null ) {
            maximumSizeParm = ResourcePool.Defaults.DEFAULT_MAXIMUM_RESOURCE_SIZE;
        }
        try {
            this.maximum_pool_size = Integer.parseInt(maximumSizeParm);
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {maximumSizeParm, ResourcePoolPropertyNames.MAXIMUM_RESOURCE_POOL_SIZE, "long"})); //$NON-NLS-1$
        }


        String minimumSizeParm = properties.getProperty(ResourcePoolPropertyNames.MINIMUM_RESOURCE_POOL_SIZE);
        if ( minimumSizeParm == null ) {
            minimumSizeParm = ResourcePool.Defaults.DEFAULT_MINIMUM_RESOURCE_SIZE;
        }
        try {
            this.minimum_pool_size = Integer.parseInt(minimumSizeParm);
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {minimumSizeParm, ResourcePoolPropertyNames.MINIMUM_RESOURCE_POOL_SIZE, "long"})); //$NON-NLS-1$
        }

        if (this.minimum_pool_size > this.maximum_pool_size) {
            this.maximum_pool_size = this.minimum_pool_size;
        }

        properties.setProperty(ResourcePoolPropertyNames.MINIMUM_RESOURCE_POOL_SIZE, minimumSizeParm);
        properties.setProperty(ResourcePoolPropertyNames.MAXIMUM_RESOURCE_POOL_SIZE, String.valueOf(this.maximum_pool_size));


        String liveAndUsedTimeParm = properties.getProperty(ResourcePoolPropertyNames.LIVE_AND_UNUSED_TIME);
        if ( liveAndUsedTimeParm == null ) {
            liveAndUsedTimeParm = ResourcePool.Defaults.DEFAULT_LIVE_AND_UNUSED_TIME;
        }
        try {
            this.liveAndUsedTime = Long.parseLong(liveAndUsedTimeParm);
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {liveAndUsedTimeParm, ResourcePoolPropertyNames.LIVE_AND_UNUSED_TIME, "long"})); //$NON-NLS-1$
        }

        properties.setProperty(ResourcePoolPropertyNames.LIVE_AND_UNUSED_TIME, liveAndUsedTimeParm);


        String shrinkPeriodParm = properties.getProperty(ResourcePoolPropertyNames.SHRINK_PERIOD);
        if ( shrinkPeriodParm == null ) {
            shrinkPeriodParm = ResourcePool.Defaults.DEFAULT_SHRINK_PERIOD;
        }
        try {
            this.shrinkPeriod = Long.parseLong(shrinkPeriodParm);
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {shrinkPeriodParm, ResourcePoolPropertyNames.SHRINK_PERIOD, "long"})); //$NON-NLS-1$
        }

        properties.setProperty(ResourcePoolPropertyNames.SHRINK_PERIOD, shrinkPeriodParm);


        String allowShrinkParm = properties.getProperty(ResourcePoolPropertyNames.ALLOW_SHRINKING);
        if ( allowShrinkParm == null ) {
            allowShrinkParm = ResourcePool.Defaults.DEFAULT_ALLOW_SHRINKING;
        }
        try {
            this.allowsShrinking = Boolean.valueOf(allowShrinkParm).booleanValue();
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {allowShrinkParm, ResourcePoolPropertyNames.ALLOW_SHRINKING, "boolean"})); //$NON-NLS-1$
        }

        properties.setProperty(ResourcePoolPropertyNames.ALLOW_SHRINKING, allowShrinkParm);


        String shrinkIncrementParm = properties.getProperty(ResourcePoolPropertyNames.SHRINK_INCREMENT);
        if ( shrinkIncrementParm == null ) {
            shrinkIncrementParm = ResourcePool.Defaults.DEFAULT_SHRINK_INCREMENT;
        }
        try {
            this.shrinkIncrement = Integer.parseInt(shrinkIncrementParm);
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {shrinkIncrementParm, ResourcePoolPropertyNames.SHRINK_INCREMENT, "integer"})); //$NON-NLS-1$
        }

        properties.setProperty(ResourcePoolPropertyNames.SHRINK_INCREMENT, shrinkIncrementParm);


        String waitTimeParm = properties.getProperty(ResourcePoolPropertyNames.WAIT_TIME_FOR_RESOURCE);
        if ( waitTimeParm == null ) {
            waitTimeParm = ResourcePool.Defaults.DEFAULT_WAIT_FOR_RESOURCE_TIME;
        }
        try {
            this.waitForResourceTime = Long.parseLong(waitTimeParm);
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {waitTimeParm, ResourcePoolPropertyNames.WAIT_TIME_FOR_RESOURCE, "long"})); //$NON-NLS-1$
        }

        properties.setProperty(ResourcePoolPropertyNames.WAIT_TIME_FOR_RESOURCE, waitTimeParm);

        String concurrentUsersValue = properties.getProperty(ResourcePoolPropertyNames.NUM_OF_CONCURRENT_USERS);
        if ( concurrentUsersValue == null ) {
            concurrentUsersValue = ResourcePool.Defaults.DEFAULT_NUM_OF_CONCURRENT_USERS;
        }
        try {
            this.num_concurrent_users_allowed = Integer.parseInt(concurrentUsersValue);
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {concurrentUsersValue, ResourcePoolPropertyNames.NUM_OF_CONCURRENT_USERS, "integer"})); //$NON-NLS-1$
        }

        properties.setProperty(ResourcePoolPropertyNames.NUM_OF_CONCURRENT_USERS, concurrentUsersValue);


        String extendModeParm = properties.getProperty(ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_MODE);
        if ( extendModeParm == null ) {
            extendModeParm = ResourcePool.Defaults.DEFAULT_EXTEND_MODE;
        }
        try {
            this.extendMode = new Boolean(extendModeParm).booleanValue();
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {extendModeParm, ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_MODE, "boolean"})); //$NON-NLS-1$
        }

        properties.setProperty(ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_MODE, extendModeParm);


        String extendPercentParm = properties.getProperty(ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_PERCENT);
        if ( extendPercentParm == null ) {
            extendPercentParm = ResourcePool.Defaults.DEFAULT_EXTEND_PERCENT;
        }
        try {
            this.extendPercent = Double.parseDouble(extendPercentParm);
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0019,
      		      			new Object[] {extendPercentParm, ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_PERCENT, "long"})); //$NON-NLS-1$
        }

        properties.setProperty(ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_PERCENT, extendPercentParm);

        // calculate the extend max size
        this.extend_maximum_pool_size = (new Double(this.maximum_pool_size * this.extendPercent)).intValue() + this.maximum_pool_size;

    }

    private void initStatistics() {

        // creating the pool statistics has to be done before
        // the resources are inited because stats are needed at that time.
        this.poolStatistics = new BasicResourcePoolStatistics(this);

        PoolStatistic ps = null;

        /** DONE */
        ps = new CounterStat(ResourcePoolStatisticNames.NUM_OF_RESOURCES_REQUESTED,
                              ResourcePoolStatisticNames.getDisplayName(ResourcePoolStatisticNames.NUM_OF_RESOURCES_REQUESTED),
                              ResourcePoolStatisticNames.getDescription(ResourcePoolStatisticNames.NUM_OF_RESOURCES_REQUESTED),
                                        PoolStatistic.SUM_AGGREGATE_TYPE);
        this.poolStatistics.addStatistic(ps);

        /** DONE */
        ps = new CounterStat(ResourcePoolStatisticNames.NUM_OF_SUCCESSFUL_REQUESTS,
                              ResourcePoolStatisticNames.getDisplayName(ResourcePoolStatisticNames.NUM_OF_SUCCESSFUL_REQUESTS),
                              ResourcePoolStatisticNames.getDescription(ResourcePoolStatisticNames.NUM_OF_SUCCESSFUL_REQUESTS),
                                        PoolStatistic.SUM_AGGREGATE_TYPE);
        this.poolStatistics.addStatistic(ps);

        /** DONE */
        ps = new CounterStat(ResourcePoolStatisticNames.NUM_OF_UNSUCCESSFUL_REQUESTS,
                              ResourcePoolStatisticNames.getDisplayName(ResourcePoolStatisticNames.NUM_OF_UNSUCCESSFUL_REQUESTS),
                              ResourcePoolStatisticNames.getDescription(ResourcePoolStatisticNames.NUM_OF_UNSUCCESSFUL_REQUESTS),
                                        PoolStatistic.SUM_AGGREGATE_TYPE);
        this.poolStatistics.addStatistic(ps);

//        ps = new CounterStat(ResourcePoolStatistics.NUM_OF_RESOURCES_AVAILABLE, BasicPoolStatistic.TYPE_SUM);
//        this.poolStatistics.addStatistic(ps);

        /** DONE */
        ps = new CounterStat(ResourcePoolStatisticNames.TOTAL_PHYSICAL_RESOURCES_USED,
                              ResourcePoolStatisticNames.getDisplayName(ResourcePoolStatisticNames.TOTAL_PHYSICAL_RESOURCES_USED),
                              ResourcePoolStatisticNames.getDescription(ResourcePoolStatisticNames.TOTAL_PHYSICAL_RESOURCES_USED),
                                         PoolStatistic.SUM_AGGREGATE_TYPE);
        this.poolStatistics.addStatistic(ps);

        /** DONE */
        ps = new CounterStat(ResourcePoolStatisticNames.NUM_OF_TIMEOUTS,
                              ResourcePoolStatisticNames.getDisplayName(ResourcePoolStatisticNames.NUM_OF_TIMEOUTS),
                              ResourcePoolStatisticNames.getDescription(ResourcePoolStatisticNames.NUM_OF_TIMEOUTS),
                                        PoolStatistic.SUM_AGGREGATE_TYPE);
        this.poolStatistics.addStatistic(ps);

        /** DONE */
        ps = new HighestValueStat(ResourcePoolStatisticNames.MAX_NUM_OF_RESOURCES_IN_POOL,
                              ResourcePoolStatisticNames.getDisplayName(ResourcePoolStatisticNames.MAX_NUM_OF_RESOURCES_IN_POOL),
                              ResourcePoolStatisticNames.getDescription(ResourcePoolStatisticNames.MAX_NUM_OF_RESOURCES_IN_POOL),
                                         PoolStatistic.MAX_AGGREGATE_TYPE);
        this.poolStatistics.addStatistic(ps);

        /** DONE */
        ps = new CounterStat(ResourcePoolStatisticNames.NUM_OF_RESOURCES_IN_POOL,
                              ResourcePoolStatisticNames.getDisplayName(ResourcePoolStatisticNames.NUM_OF_RESOURCES_IN_POOL),
                              ResourcePoolStatisticNames.getDescription(ResourcePoolStatisticNames.NUM_OF_RESOURCES_IN_POOL),
                                        PoolStatistic.MAX_AGGREGATE_TYPE);
        this.poolStatistics.addStatistic(ps);

/*
        stats.put(ResourcePoolStatistics.AVG_TIME_RESOURCE_USED,
                          BasicPoolStatistic.getInstance(ResourcePoolStatistics.AVG_TIME_RESOURCE_USED, BasicPoolStatistic.TYPE_AVG));
        stats.put(ResourcePoolStatistics.HIGHEST_TIME_RESOURCE_USED,
                          BasicPoolStatistic.getInstance(ResourcePoolStatistics.HIGHEST_TIME_RESOURCE_USED, BasicPoolStatistic.TYPE_HIGHEST));
        stats.put(ResourcePoolStatistics.LOWEST_TIME_RESOURCE_USED,
                          BasicPoolStatistic.getInstance(ResourcePoolStatistics.LOWEST_TIME_RESOURCE_USED, BasicPoolStatistic.TYPE_LOWEST));
*/
    }

    protected void initResources() throws ResourcePoolException {

        if (this.minimum_pool_size == 0) {
            return;
        }

        // preload the pool
        for (int i = 0; i < this.minimum_pool_size; i++ ) {
            createResourceContainer();
        }

    }

    public Resource checkOut(String userName) throws ResourcePoolException {

        if ( this.shutdownRequested ) {
            throw new ResourcePoolException(ErrorMessageKeys.POOLING_ERR_0026, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0026,
            				new Object[] {userName} ));
        }

      LogManager.logTrace(CONTEXT, "Perform Checkout Resource " + resourceDescriptor.getName()); //$NON-NLS-1$
      poolStatistics.increment(ResourcePoolStatisticNames.NUM_OF_RESOURCES_REQUESTED);


// The order of processing is as follows:
// 1. First check if
// 1. Check the resourcePool if any available based on concurrent user count, if use
// 2. Then if max pool size not reached, create new resource

        ResourceContainer container = null;
        long startTime = System.currentTimeMillis();
        long timeWaited = 0;
        int poolSize =0;

        while(timeWaited < waitForResourceTime) {

            lock.writeLock().lock();

            try {

                poolSize = this.getResourcePoolSize();
            // Lock the read connection pool and the availability list (always in the same order to prevent deadlock!)
                // If there is a read-locked connection available ...
                Iterator iter = this.resourcePool.iterator();
                while ( iter.hasNext() ) {
                    container = (ResourceContainer) iter.next();
/* vah 05/15/03 remove the isAlive checking due to db overhead
                just let the cleanup thread perform the isAlive cleanup

                    if (! container.isAlive()) {

                        closeContainer(container);
                        iter.remove();
                        container = null;
                        poolSize = this.getResourcePoolSize();

                        LogManager.logDetail(CONTEXT, "Resource in pool " + resourceDescriptor.getName() + " is not alive and will be removed.");

                      // cleanup is not done here, let the cleanup thread handle it
                        continue; // find the next usable resource
                    }
*/

                    // if the number of current users has not been reached for this resource, the use this one
                    if (container.hasAvailableResource()) {
    //                             LogManager.logTrace(CONTEXT, "Checkout existing Resource " + resourceDescriptor.getName());
    //                             LogManager.logTrace(CONTEXT, "Num concurrent users allowed " + num_concurrent_users_allowed + "  Used: " + container.getStats().getConcurrentUserCount());
                            Resource resource = checkOutFromPool(container, userName);
                            return resource;

                    }

                }

                // If the maximum size of the pool has not been reached, the create another container
                if (poolSize < maximum_pool_size) {
    //                LogManager.logTrace(CONTEXT, (poolSize < this.maximum_pool_size ? "CREATE" : "DONTCREATE") + " Max Pool Size " + this.maximum_pool_size + "  Pool Size: " + poolSize);

                    try {
                        container = createResourceContainer();

                        if (container != null) {
                             Resource resource = checkOutFromPool(container, userName);
                             return resource;
                        }


                    // catch the creation exception and log it and
                    // let the wait process allow for retrying
                    } catch (ResourcePoolException rpe) {
                        LogManager.logCritical(CONTEXT, rpe, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0027, this.resourceDescriptor.getName(), userName) ) ;

                    }
                }

            } catch (ResourcePoolException e) {
                   LogManager.logCritical(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0028, this.resourceDescriptor.getName(), userName) ) ;
                   poolStatistics.increment(ResourcePoolStatisticNames.NUM_OF_UNSUCCESSFUL_REQUESTS);
                   throw e;
            } finally {
                  lock.writeLock().unlock();
            }

            try {
                Thread.sleep(1000); // sleep for 1 second
            } catch (Exception e) {
            }
            timeWaited = System.currentTimeMillis() - startTime;


        } // end of waiting


        poolStatistics.increment(ResourcePoolStatisticNames.NUM_OF_TIMEOUTS);


        // if extended mode enabled then create a new resource if extended max not reached
        if (this.extendMode) {
            if (poolSize > 0 && poolSize < this.extend_maximum_pool_size) {
                LogManager.logDetail(CONTEXT, "Extended Mode create new Resource " + resourceDescriptor.getName()); //$NON-NLS-1$

                lock.writeLock().lock();

                try {

                    container = createResourceContainer();
                    Resource resource = checkOutFromPool(container, userName);
                    return resource;

                } catch (ResourcePoolException e) {
                       LogManager.logError(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0029, this.resourceDescriptor.getName(), userName) ) ;
                       poolStatistics.increment(ResourcePoolStatisticNames.NUM_OF_UNSUCCESSFUL_REQUESTS);
                       throw e;
                } finally {
                      lock.writeLock().unlock();
                }
            }
        }

        poolStatistics.increment(ResourcePoolStatisticNames.NUM_OF_UNSUCCESSFUL_REQUESTS);

        LogManager.logError(CONTEXT, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0030, this.resourceDescriptor.getName(), userName, Long.toString(waitForResourceTime)));

        throw new ResourceWaitTimeOutException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0030, new Object[] {this.resourceDescriptor.getName(), userName, Long.toString(waitForResourceTime)} ));
    }

    private Resource checkOutFromPool(ResourceContainer container, String userName) throws ResourcePoolException {

           Resource r = container.checkOut(userName);
      //****
      // commenting out the logic that enable concurrent users of the same resource
//           if (!container.hasAvailableResource()) {
                this.resourcePool.remove(container);
                this.inuseResourcePool.add(container);
//           }
           poolStatistics.increment(ResourcePoolStatisticNames.NUM_OF_SUCCESSFUL_REQUESTS);

//           poolStatistics.increment(ResourcePoolStatisticNames.NUM_OF_RESOURCES_IN_USE);

           return r;
    }

    private ResourceContainer createResourceContainer() throws ResourcePoolException {
        LogManager.logDetail(CONTEXT, "Creating New Resource in pool " + resourceDescriptor.getName()); //$NON-NLS-1$

        Object resource = resourceAdapter.createPhysicalResourceObject(resourceDescriptor);

        ResourceContainer container = new BasicResourceContainer(++containerCounter);
        container.init(this, resource, num_concurrent_users_allowed);

        // vah 5/15/03 remove the isAlive checking
//        if (container.isAlive()) {

            resourcePool.add(container);

            poolStatistics.increment(ResourcePoolStatisticNames.TOTAL_PHYSICAL_RESOURCES_USED);
            poolStatistics.increment(ResourcePoolStatisticNames.NUM_OF_RESOURCES_IN_POOL);
            poolStatistics.increment(ResourcePoolStatisticNames.MAX_NUM_OF_RESOURCES_IN_POOL, this.getResourcePoolSize());

            LogManager.logTrace(CONTEXT, "Created Resource " + resourceDescriptor.getName() + " Max Pool Size " + this.maximum_pool_size + "  Pool Size: " + this.resourcePool.size()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

//        } else {
 //           container.shutDown();
 //           container = null;

//            I18nLogManager.logWarning(CONTEXT, ErrorMessageKeys.POOLING_ERR_0031, this.resourceDescriptor.getName() ) ;

//        }

        return container;

    }

//    public synchronized boolean checkIn( Resource resource ) throws ResourcePoolException {

    public void checkIn(ResourceContainer resourceContainer, String userName) throws ResourcePoolException {

    // Acquire write lock
        lock.writeLock().lock();

//        try {

			// remove from the inuse pool
            this.inuseResourcePool.remove(resourceContainer);
            this.resourcePool.add(resourceContainer);
/* vah - 05/15/03 - removed the checking isAlive due to the database overhead
                    just let the cleaner thread manage the isAlive process

            // if resource is still alive, then add back to the pool
            if (resourceContainer.isAlive()) {

                this.resourcePool.add(resourceContainer);

            } else {
            // otherwise close
                closeContainer(resourceContainer);

            }
*/

/*
        } catch (ResourcePoolException e) {
               I18nLogManager.logError(CONTEXT, ErrorMessageKeys.POOLING_ERR_0032, e, new Object[] {userName, this.resourceDescriptor.getName()} ) ;
               throw e;
        } finally {
              lock.writeLock().release();
        }
 */
        lock.writeLock().unlock();
        LogManager.logTrace(CONTEXT, "Checkin Resource " + getResourceDescriptor().getName() + " for user " + userName); //$NON-NLS-1$ //$NON-NLS-2$


    }


    public void cleanUp() {

        List connectionsToBeClosed= null;
        Iterator iter = null;

        // track the minimum connection to keep while closing the others
        ResourceContainer container = null;

        lock.writeLock().lock();

        try {
            if (this.shutdownRequested) {

                connectionsToBeClosed = new ArrayList(this.resourcePool.size());

                iter = this.resourcePool.iterator();
                while ( iter.hasNext() ) {
                    container = (ResourceContainer) iter.next();

                    iter.remove();
                    connectionsToBeClosed.add(container);
               }

            } else {

                long unusedPeriod = System.currentTimeMillis() + this.liveAndUsedTime;

                ResourceStatistics stats = null;
                int shrinkCnt = 0;
                int poolSize = this.resourcePool.size();

                connectionsToBeClosed = new ArrayList(poolSize);


           // First pass remove all non-alive resources
                iter = this.resourcePool.iterator();
                while ( iter.hasNext() ) {
                // the process keeps the first unused resource as the minimum connection
                    container = (ResourceContainer) iter.next();

                    boolean isAlive = false;
                    try {

                        isAlive = container.isAlive();

                    } catch (ResourcePoolException rpe) {
						// dont log here, it will be logged due to not alive
                    }

                    if (!isAlive) {
                        // if resource is not alive, then remove from pool.

                        //ISSUE: if container has users but is not alive,
                        //      should it wait until no more users - yes

                        connectionsToBeClosed.add(container);
                        iter.remove();
                        LogManager.logCritical(CONTEXT,CommonPlugin.Util.getString( ErrorMessageKeys.POOLING_ERR_0033, this.resourceDescriptor.getName() )) ;

                        ++shrinkCnt;
/* Dont need this logic because concurrent users is not being used
                        if (container.hasAvailableResource()) {
                            LogManager.logCritical(CONTEXT, "Resource " + this.resourceDescriptor.getName() + " is not alive but will not be shutdown because it has current users.");
                        } else {

                        }
*/
                    }

                } // end of while

               // Second, if the pool hasnt reached the minimum size
               // then check the resources to see if the
               // unused ones have timed out
                if (this.allowsShrinking) {
                  if  ( ( (poolSize - shrinkCnt) > this.minimum_pool_size) ||
                      (shrinkCnt < this.shrinkIncrement ) ) {

                    iter = this.resourcePool.iterator();
                    while ( iter.hasNext() ) {
                    // the process keeps the first unused resource as the minimum connection
                        container = (ResourceContainer) iter.next();
                        stats = container.getStats();


                        // if the minimum number of resources have been reached,
                        // then don't remove any more
                        if ( (poolSize - shrinkCnt) <= this.minimum_pool_size) {
                            break;
                        }

                        // if all the following conditions exist then remove (shrink) resource
                        //    - resource has not been used since the cutoff for live and unused
                        //          AND
                        //    -  shrinkIncrement is unlimited (0) or
                        //            the number of resources removed has not maxed to
                        //            equal shinkIncrement ( shrinkCnt < shrinkIncrement )

                        if ((stats.getLastUsed() < unusedPeriod) &&
                              (shrinkIncrement == 0 || shrinkCnt < this.shrinkIncrement) )  {
                            connectionsToBeClosed.add(container);
                            iter.remove();
                            ++shrinkCnt;
                            continue;
                        }

                    } // end of while
                      }
                } // end of if allowshrinking


          }  // end of not being shutdown
        } catch (Exception e) {
               LogManager.logCritical(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0034, this.resourceDescriptor.getName()) ) ;

        } finally {
              lock.writeLock().unlock();
        }


    if (connectionsToBeClosed != null && connectionsToBeClosed.size() > 0) {
        closeContainers(connectionsToBeClosed);
    }

  } // end of method


  private void closeContainers(Collection containers) {

        if (containers != null) {
            ResourceContainer container;
            for (Iterator it=containers.iterator(); it.hasNext(); ) {
                container = (ResourceContainer) it.next();
                closeContainer(container);
            }
        }


  }

  private void closeContainer(ResourceContainer container) {

        synchronized(container) {

            try {

               	LogManager.logDetail(CONTEXT,"Shutting down resource in pool " + resourceDescriptor.getName()); //$NON-NLS-1$
                container.shutDown();
                poolStatistics.decrement(ResourcePoolStatisticNames.NUM_OF_RESOURCES_IN_POOL);

            } catch (Exception e) {
                // log the exception and keep closing
                 LogManager.logWarning(CONTEXT, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0034, this.resourceDescriptor.getName()));
            }

        }

  }

  public String toString() {

        int size = getResourcePoolSize();

        StringBuffer sb = new StringBuffer();
        sb.append("Resource " + resourceDescriptor.getName() + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        sb.append("\tCurrent Pool Size: " + size + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        sb.append("\tMin Pool Size: " + minimum_pool_size + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        sb.append("\tMax Pool Size: " + maximum_pool_size + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        sb.append("\tAllow Shrinking: " + allowsShrinking + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        sb.append("\tShrink Increment: " + shrinkIncrement + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        sb.append("\tShrink Period: " + shrinkPeriod  + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        sb.append("\tExtend Mode: " + extendMode + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        sb.append("\tExtend %: " + extendPercent + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        sb.append("\tExtend Max Pool Size: " + extend_maximum_pool_size + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        sb.append("\tWait for Resource Time: " + waitForResourceTime + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        sb.append("\tAlive And Used Time: " + liveAndUsedTime + "\n"); //$NON-NLS-1$ //$NON-NLS-2$

        return sb.toString();
  }



} // end of class

class CleanUpThread extends Thread
{
    private BasicResourcePool pool;
    private long sleepTime;
    private boolean continueChecks = true;
    private  String poolName;

    CleanUpThread( BasicResourcePool pool, long sleepTime )   {
        super("CleanUpThread" +pool.getResourceDescriptor().getName()); //$NON-NLS-1$
        this.pool = pool;
        this.sleepTime = sleepTime;
    	this.poolName = pool.getResourceDescriptor().getName();
    }

    public void stopCleanup() {
        this.continueChecks = false;
        pool = null;
    }

    public synchronized void setSleepTime(long newSleepTime) {
        this.sleepTime = newSleepTime;
    }

    public void run()
    {
        while( this.continueChecks ) {
            try {
                sleep( sleepTime );
            } catch( InterruptedException e ) {
                // ignore it
            }
            if ( pool != null ) {
                LogManager.logTrace(BasicResourcePool.CONTEXT, "ResourcePool Clean-up of resource " + poolName); //$NON-NLS-1$

//System.out.println("Clean-up thread is instructing the pool to clean up");
                pool.cleanUp();
            }
        }
    }
}



