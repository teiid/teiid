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

package com.metamatrix.common.pooling.api;

import java.util.HashMap;
import java.util.Map;

public final class ResourcePoolStatisticNames {


  /** the total number physical resources instantiated, regardless if still available */
  public static final String TOTAL_PHYSICAL_RESOURCES_USED = "totalPhysicalResourcesUsed";  //$NON-NLS-1$
  /** the number timeouts that have occured.*/
  public static final String NUM_OF_TIMEOUTS = "numOfTimeouts";  //$NON-NLS-1$
  /** the total number resources currently available */
//  public static final String NUM_OF_RESOURCES_AVAILABLE = "resourcesAvailable";
  /** the total number resources requested for use */
  public static final String NUM_OF_RESOURCES_REQUESTED = "resourcesRequested";  //$NON-NLS-1$
  /** the total number of successfull requests */
  public static final String NUM_OF_SUCCESSFUL_REQUESTS = "successfulRequests";  //$NON-NLS-1$
  /** the total number of unsuccessful requests for use */
  public static final String NUM_OF_UNSUCCESSFUL_REQUESTS = "unsuccessfulRequests";  //$NON-NLS-1$
  /** the number resources currently in use */
//  public static final String NUM_OF_RESOURCES_IN_USE = "resourcesRequested";
  /** Maximumn number of resources in the pool at one time - high watermark */
  public static final String MAX_NUM_OF_RESOURCES_IN_POOL = "maxNumInPool";  //$NON-NLS-1$
  /** Number of resources currently in the pool */
  public static final String NUM_OF_RESOURCES_IN_POOL = "numInPool";  //$NON-NLS-1$

//--------------------
// the following stats give an indication how fast or slow the resources are being used.
//--------------------
  /** the average time a resource was used */
//  public static final String AVG_TIME_RESOURCE_USED = "avgTimeResourceUsed";
  /** the highest amount of time a resource was used */
//  public static final String HIGHEST_TIME_RESOURCE_USED = "highestTimeResourceUsed";
  /** the lowest amount of time a resource was used */
//  public static final String LOWEST_TIME_RESOURCE_USED = "lowestTimeResourceUsed";


  /** the last time the resource was used */
//  public static final String LAST_TIME_RESOURCE_USED = "lastTimeResourceUsed";


  /** the max size of the ResourcePool.MAXIMUM_RESOURCE_SIZE */
  public static final String MAX_RESOURCE_SIZE = "maxResourceLimit"; //$NON-NLS-1$
  /** the max size of the ResourcePool.MINIMUM_RESOURCE_SIZE */  
  public static final String MIN_RESOURCE_SIZE = "minResourceLimit"; //$NON-NLS-1$


  private interface DisplayNames {
      /** the total number physical resources instantiated, regardless if still available */
      public static final String TOTAL_PHYSICAL_RESOURCES_USED = "Connections Created";  //$NON-NLS-1$
      /** the number timeouts that have occured.*/
      public static final String NUM_OF_TIMEOUTS = "Timeouts";  //$NON-NLS-1$
      /** the total number resources requested for use */
      public static final String NUM_OF_RESOURCES_REQUESTED = "Total Requests";  //$NON-NLS-1$
      /** the total number of successfull requests */
      public static final String NUM_OF_SUCCESSFUL_REQUESTS = "Successful Requests";  //$NON-NLS-1$
      /** the total number of unsuccessful requests for use */
      public static final String NUM_OF_UNSUCCESSFUL_REQUESTS = "Failed Requests";  //$NON-NLS-1$
      /** the number resources currently in use */
      public static final String MAX_NUM_OF_RESOURCES_IN_POOL = "Pool Watermark";  //$NON-NLS-1$
      /** Number of resources currently in the pool */
      public static final String NUM_OF_RESOURCES_IN_POOL = "Connections in Pool";  //$NON-NLS-1$
      
      
  }
  
  private interface Descriptions {
      /** the total number physical resources instantiated, regardless if still available */
      public static final String TOTAL_PHYSICAL_RESOURCES_USED = "The number of times a connection was created";  //$NON-NLS-1$
      /** the number timeouts that have occured.*/
      public static final String NUM_OF_TIMEOUTS = "The number of times a request timed out waiting for a connection";  //$NON-NLS-1$
      /** the total number resources requested for use */
      public static final String NUM_OF_RESOURCES_REQUESTED = "The number of times a connection was requested";  //$NON-NLS-1$
      /** the total number of successfull requests */
      public static final String NUM_OF_SUCCESSFUL_REQUESTS = "The number of times a connection was successfully requested";  //$NON-NLS-1$
      /** the total number of unsuccessful requests for use */
      public static final String NUM_OF_UNSUCCESSFUL_REQUESTS = "The number of times a connection request failed";  //$NON-NLS-1$
      /** the number resources currently in use */
      public static final String MAX_NUM_OF_RESOURCES_IN_POOL = "The highest number of connections ever in the pool";  //$NON-NLS-1$
      /** Number of resources currently in the pool */
      public static final String NUM_OF_RESOURCES_IN_POOL = "The number of connections currently in the pool";  //$NON-NLS-1$
      
      
  }
  

  private static Map displayNames = null;
  private static Map descriptions = null;
  
  
  private static final int num_of_stats = 7;

  static {
    displayNames = new HashMap(num_of_stats);
    displayNames.put(NUM_OF_RESOURCES_REQUESTED, DisplayNames.NUM_OF_RESOURCES_REQUESTED);
    displayNames.put(NUM_OF_SUCCESSFUL_REQUESTS, DisplayNames.NUM_OF_SUCCESSFUL_REQUESTS);
    displayNames.put(NUM_OF_UNSUCCESSFUL_REQUESTS, DisplayNames.NUM_OF_UNSUCCESSFUL_REQUESTS);
    displayNames.put(TOTAL_PHYSICAL_RESOURCES_USED, DisplayNames.TOTAL_PHYSICAL_RESOURCES_USED);
    displayNames.put(NUM_OF_TIMEOUTS, DisplayNames.NUM_OF_TIMEOUTS);
    displayNames.put(MAX_NUM_OF_RESOURCES_IN_POOL, DisplayNames.MAX_NUM_OF_RESOURCES_IN_POOL);
    displayNames.put(NUM_OF_RESOURCES_IN_POOL, DisplayNames.NUM_OF_RESOURCES_IN_POOL);                
     
     
    descriptions = new HashMap(num_of_stats);
    descriptions.put(NUM_OF_RESOURCES_REQUESTED, Descriptions.NUM_OF_RESOURCES_REQUESTED);
    descriptions.put(NUM_OF_SUCCESSFUL_REQUESTS, Descriptions.NUM_OF_SUCCESSFUL_REQUESTS);
    descriptions.put(NUM_OF_UNSUCCESSFUL_REQUESTS, Descriptions.NUM_OF_UNSUCCESSFUL_REQUESTS);
    descriptions.put(TOTAL_PHYSICAL_RESOURCES_USED, Descriptions.TOTAL_PHYSICAL_RESOURCES_USED);
    descriptions.put(NUM_OF_TIMEOUTS, Descriptions.NUM_OF_TIMEOUTS);
    descriptions.put(MAX_NUM_OF_RESOURCES_IN_POOL, Descriptions.MAX_NUM_OF_RESOURCES_IN_POOL);
    descriptions.put(NUM_OF_RESOURCES_IN_POOL, Descriptions.NUM_OF_RESOURCES_IN_POOL);                
      
  }

  public static final String getDisplayName(String statisticName) {
      return (String) displayNames.get(statisticName);
  }
  
  public static final String getDescription(String statisticName) {
      return (String) descriptions.get(statisticName);
  }
  
  
  

}
