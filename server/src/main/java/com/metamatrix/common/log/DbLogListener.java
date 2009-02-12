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

package com.metamatrix.common.log;

import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.log.LogMessage;

/**
 * Listener class to log messages to the Database
 */
public class DbLogListener implements LogListener {

    public static final String LOG_DB_ENABLED = "metamatrix.log.jdbcDatabase.enabled"; //$NON-NLS-1$

	private DbLogWriter writer = null;
    private boolean enabled = true;

	/**
	 * Listen for log messages and write them to a database.
	 */
	public DbLogListener(Properties prop) throws DbWriterException {
		if (prop == null) {
			final String msg = CommonPlugin.Util.getString("DbLogListener.The_Properties_reference_may_not_be_null");  //$NON-NLS-1$
			throw new IllegalArgumentException(msg);
		}
        writer = new DbLogWriter(prop);
        writer.initialize();
        enabled = PropertiesUtils.getBooleanProperty(prop, LOG_DB_ENABLED, true);

	}

    protected void init(Properties props) throws DbWriterException {

    }

    public void logMessage(LogMessage msg) {
        
        if (enabled) {
            writer.logMessage(msg);
        }
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.core.log.LogListener#shutdown()
	 */
	public void shutdown() {
		writer.shutdown();
	}
    
    public void determineIfEnabled(Properties props) {
        boolean isenabled = PropertiesUtils.getBooleanProperty(props, LOG_DB_ENABLED, true);
        enableDBLogging(isenabled);

    }
    
    public void enableDBLogging(boolean enable) {
        enabled = enable;
    }

}
