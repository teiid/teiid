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
import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.log.LogMessage;

/**
 * Listener class to log messages to the Database
 */
public class DbLogListener implements LogListener {

	private DbLogWriter writer = null;
    private boolean enabled = true;

	/**
	 * Listen for log messages and write them to a database.
	 */
	public DbLogListener(Properties prop, boolean enable){
		if (prop == null) {
			final String msg = CommonPlugin.Util.getString("DbLogListener.The_Properties_reference_may_not_be_null");  //$NON-NLS-1$
			throw new IllegalArgumentException(msg);
		}
        writer = new DbLogWriter(prop);
        writer.initialize();
        this.enabled = enable;
	}

    public void log(int level, String context, Object msg) {
        if (enabled) {
            writer.logMessage(level, context, msg, null);
        }
	}
    
    public void log(int level, String context, Throwable t, Object msg) {
        if (enabled) {
            writer.logMessage(level, context, msg, t);
        }    	
    }

	public void shutdown() {
		writer.shutdown();
	}

    public void enableDBLogging(boolean enable) {
        enabled = enable;
    }
}
