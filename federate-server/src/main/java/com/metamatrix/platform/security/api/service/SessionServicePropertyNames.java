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

package com.metamatrix.platform.security.api.service;


public class SessionServicePropertyNames {

    /**
     * The environment property name for the class that is to be used for the ManagedConnectionFactory implementation.
     * This property is required (there is no default).
     */
    public static final String CONNECTION_FACTORY = "security.session.connection.Factory"; //$NON-NLS-1$

    /**
     * The environment property name for the class of the driver.
     * This property is optional.
     */
    public static final String CONNECTION_DRIVER = "security.session.connection.Driver"; //$NON-NLS-1$

    /**
     * The environment property name for the protocol for connecting to the session store.
     * This property is optional.
     */
    public static final String CONNECTION_PROTOCOL = "security.session.connection.Protocol"; //$NON-NLS-1$

    /**
     * The environment property name for the name of the session store database.
     * This property is optional.
     */
    public static final String CONNECTION_DATABASE = "security.session.connection.Database"; //$NON-NLS-1$

    /**
     * The environment property name for the username that is to be used for connecting to the session store.
     * This property is optional.
     */
    public static final String CONNECTION_USERNAME = "security.session.connection.User"; //$NON-NLS-1$

    /**
     * The environment property name for the password that is to be used for connecting to the session store.
     * This property is optional.
     */
    public static final String CONNECTION_PASSWORD = "security.session.connection.Password"; //$NON-NLS-1$

    
    /**
     * Comma delimeted string containing a list of SessionTerminationHandlers to be called when a session
     * is terminated.
     */
    public static final String SESSION_TERMINATION_HANDLERS = "security.session.terminationHandlers"; //$NON-NLS-1$

    /**
     * SessionCleanupThread property names
     * Defined in config.xml
     */
    public static final String ACTIVITY_INTERVAL = "security.session.oldsessionreaper.activityinterval"; //$NON-NLS-1$
    public static final String OLD_SESSION_TTL = "security.session.oldsessionreaper.oldsessionTTL"; //$NON-NLS-1$
    /** <p>Default values for SessionCleanupThread</p> */
    /** 1 HOUR */
    public static final String DEFAULT_ACTIVITY_INTERVAL = "1"; //$NON-NLS-1$
    /** 10 DAYS */
    public static final String DEFAULT_OLD_SESSION_TTL = "10"; //$NON-NLS-1$
    /** 1000 ms/s * 60 s/min */
    public static final long MILLIS_PER_MIN = 1000 * 60;
    /** MILLIS_PER_MIN * 60 min/hr */
    public static final long MILLIS_PER_HOUR = MILLIS_PER_MIN * 60;
    /** MILLIS_PER_HOUR * 24 hr/day */
    public static final long MILLIS_PER_DAY = MILLIS_PER_HOUR * 24;
    
    /** Session Cache property names
     * Defined in config.xml
     */
    public static final String CACHE_RESOURCE_RECAPTURE_MODE = "CacheResourceRecaptureMode";  //$NON-NLS-1$
    public static final String CACHE_RESOURCE_RECAPTURE_INTERVAL = "CacheResourceRecaptureInterval";  //$NON-NLS-1$
    public static final String CACHE_RESOURCE_RECAPTURE_INTERVAL_INCREMENT = "CacheResourceRecaptureIntervalIncrement";  //$NON-NLS-1$
    public static final String CACHE_RESOURCE_RECAPTURE_INTERVALRATE = "CacheResourceRecaptureIntervalRate";  //$NON-NLS-1$
    public static final String CACHE_RESOURCE_RECAPTURE_INTERVAL_CEILING = "CacheResourceRecaptureIntervalCeiling";  //$NON-NLS-1$
    public static final String CACHE_RESOURCE_RECAPTURE_INTERVAL_DECREMENT = "CacheResourceRecaptureIntervalDecrement";  //$NON-NLS-1$
    public static final String CACHE_RESOURCE_RECAPTURE_FRACTION = "CacheResourceRecaptureFraction";  //$NON-NLS-1$
    public static final String CACHE_POLICY_FACTORY = "CachePolicyFactory";  //$NON-NLS-1$
    public static final String CACHE_STATS_LOGINTERVAL = "CacheStatsLogInterval";  //$NON-NLS-1$
    public static final String CACHE_MAXIMUM_AGE = "CacheMaximumAge";  //$NON-NLS-1$
    public static final String CACHE_HELPER = "CacheHelper";  //$NON-NLS-1$
    public static final String CACHE_MAXIMUM_CAPACITY = "CacheMaximumCapacity";  //$NON-NLS-1$
    public static final String CACHE_EVENT_POLICY_NAME = "CacheEventPolicyName";  //$NON-NLS-1$
    public static final String CACHE_EVENT_FACTORY_NAME = "CacheEventFactoryName"; //$NON-NLS-1$

    /**
     * Client Monitor thread property names and their default values
     * <br></br>
     * Defined in config.xml
     */
    /** Prop name indicating whether client monitoring should take place */
    public static final String CLIENT_MONITORING_ENABLED = "security.session.clientMonitor.enabled"; //$NON-NLS-1$
    /** Prop value indicating client monitoring should take place by default */
    public static final String CLIENT_MONITORING_ENABLED_DEFAULT = "true"; //$NON-NLS-1$
    /** Prop name for how often client mointor should expect a ping from the client session */
    public static final String CLIENT_MONITOR_PING_INTERVAL = "security.session.clientMonitor.PingInterval"; //$NON-NLS-1$
    /** Default property value for Client Monitor ping interval: 5 Minutes */
    public static final String CLIENT_MONITOR_DEFAULT_PING_INTERVAL = "5"; //$NON-NLS-1$
    /** Default value for Client Monitor ping interval: 5 Minutes */
    public static final long CLIENT_MONITOR_DEFAULT_PING_INTERVAL_VAL = 5 * MILLIS_PER_MIN;
   
    
    /** Prop name for when client session should expire after no ping has been recieved */
    public static final String CLIENT_MONITOR_SESSION_EXPIRE_INTERVAL = "security.session.clientMonitor.ExpireInterval"; //$NON-NLS-1$
        
    /** Default property value for Client Monitor expire interval: 30 Minutes */
    public static final String CLIENT_MONITOR_DEFAULT_SESSION_EXPIRE_INTERVAL = "30"; //$NON-NLS-1$
    /** Default value for Client Monitor expire interval: 5 Minutes */
    public static final long CLIENT_MONITOR_DEFAULT_SESSION_EXPIRE_INTERVAL_VAL = 30 * MILLIS_PER_MIN;
    
    
    /** Prop name for how often client mointor will perform the ping interval check in order to expire sessions */
    public static final String CLIENT_MONITOR_ACTIVITY_INTERVAL = "security.session.clientMonitor.ActivityInterval"; //$NON-NLS-1$
    
    /** Default property value for Client Monitor Activity Check interval : 20 Minutes */    
    public static final String CLIENT_MONITOR_DEFAULT_ACTIVITY_INTERVAL = "20"; //$NON-NLS-1$
    /** Default value for Client Monitor ping interval Value: 3 Minutes */
    public static final long CLIENT_MONITOR_DEFAULT_ACTIVITY_INTERVAL_VAL = 20 * MILLIS_PER_MIN;

    
    /** Prop name for what algorithm to use on the client when obtaining resources 
     *  1 - random (default)
     *  2 - round-robin
     *  */
    public static final String CLIENT_RESOURCE_ALGORITHM = "security.session.clientMonitor.ResourceAlgorithm"; //$NON-NLS-1$
    
    /** Default property value for Client Resource Algorithm : 1 - random  */    
    public static final String CLIENT_RESOURCE_ALGORITHM_DEFAULT = "1"; //$NON-NLS-1$
    /** Default value for Client Monitor ping interval Value: 3 Minutes */
    public static final int CLIENT_RESOURCE_ALGORITHM_DEFAULT_VAL = 1;
    
}

