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

package com.metamatrix.platform.config;

import java.util.EventObject;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.log.DbLogListener;
import com.metamatrix.core.event.EventObjectListener;
import com.metamatrix.platform.config.event.ConfigurationChangeEvent;

/**
 * This class will listen for Changes to Configuration and 
 */
public class ConfigurationChangeListener implements EventObjectListener {
	
	/**
	 * The name of the System property that contains the set of comma-separated
	 * context names for messages <i>not</i> to be recorded.  A message context is simply
	 * some string that identifies something about the component that generates
	 * the message.  The value for the contexts is application specific.
	 * <p>
	 * This is an optional property that defaults to no contexts (i.e., messages
	 * with any context are recorded).
	 */
	public static final String LOG_CONTEXT_PROPERTY_NAME = "metamatrix.log.contexts"; //$NON-NLS-1$
	
	public static final String LOG_LEVEL_PROPERTY_NAME = "metamatrix.log"; //$NON-NLS-1$
    
    public static final String LOG_DB_ENABLED = "metamatrix.log.jdbcDatabase.enabled"; //$NON-NLS-1$
    
	Configuration currentConfig = null;
    
    private DbLogListener logger = null;
    
    public ConfigurationChangeListener() {

    }    
    
    public ConfigurationChangeListener(DbLogListener dblogger) {
        logger = dblogger;
    }

	/* (non-Javadoc)
	 * @see com.metamatrix.core.event.EventObjectListener#processEvent(java.util.EventObject)
	 */
	public void processEvent(EventObject obj) {
		if(obj instanceof ConfigurationChangeEvent){
			try {
				currentConfig = CurrentConfiguration.getInstance().getConfiguration();
                
                if (logger != null) {
                    logger.determineIfEnabled(currentConfig.getProperties());
                }                
                
			} catch( ConfigurationException ce ) {
				ce.printStackTrace();
				System.out.println(ce);
			}
		}
	}

	public void shutdown() {
	}
	
}
