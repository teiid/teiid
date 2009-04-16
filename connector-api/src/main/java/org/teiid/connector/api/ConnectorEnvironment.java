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

package org.teiid.connector.api;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.teiid.connector.language.ILanguageFactory;


/**
 * The environment provided to a connector by the Connector Manager.  The 
 * environment provides access to external resources the Connector writer may
 * need.
 */
public interface ConnectorEnvironment extends Executor {

    /**
     * Get all configuration properties provided in the Connector Binding 
     * for this connector instance.
     * @return Properties for initializing the connector
     */
    Properties getProperties();

    /**
     * Get the name of the connector binding, as exposed in the console.
     * @return Connector binding name
     */
    String getConnectorName();

    /**
     * Obtain a reference to the logger that can be used to add messages to the 
     * MetaMatrix log files for debugging and error recovery.
     * @return The {@link ConnectorLogger} 
     */
    ConnectorLogger getLogger();
    
    /**
     * Obtain a reference to the default LanguageFactory that can be used to construct
     * new language interface objects.  This is typically needed when modifying the language
     * objects passed to the connector or for testing when objects need to be created. 
     */
    ILanguageFactory getLanguageFactory();
    
    /**
     * Obtain a reference to the type facility, which can be used to perform many type 
     * conversions supplied by the Connector API.
     */
    TypeFacility getTypeFacility();
    
    /**
     * Schedule a command for repeated execution with the same contract as 
     * {@link ScheduledThreadPoolExecutor#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}
     * Executions will not happen concurrently.  If an execution takes longer than a period,
     * the next execution will take place on the first period interval after completion.
     * @param command
     * @param initialDelay
     * @param period
     * @param unit
     * @return
     */
	ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
            long initialDelay,
            long period,
            TimeUnit unit);


	/**
	 * Get the item from cache based on the scope provided; The required information like session-id, or vdb-name etc 
	 * are gleaned from runtime context. If such information is not available then error will be raised.
	 * @param scope - scope of the cache; {@link CacheScope.REQUEST}, scope is not supported, as request information is not
     * visible. use ExecutionContext. 
	 * on {@link ExecutionContext}
	 * @param key
	 * @return
	 */
	Object getFromCache(CacheScope scope, Object key);

	/**
	 * Store the item in the cache based on the scope provided.The required information like session-id, or vdb-name etc 
	 * are gleaned from runtime context. If such information is not available then error will be raised.
	 * @param scope  - scope of the cache; {@link CacheScope.REQUEST}, scope is not supported. 
	 * on {@link ExecutionContext}
	 * @param key
	 * @param value
	 */
	void storeInCache(CacheScope scope, Object key, Object value);
}
