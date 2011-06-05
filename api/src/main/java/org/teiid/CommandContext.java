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

package org.teiid;

import java.io.Serializable;
import java.util.Properties;
import java.util.TimeZone;

import javax.security.auth.Subject;

import org.teiid.adminapi.Session;

/**
 * Context information for the currently executing command.
 * Can be used as an argument to UDFs.
 */
public interface CommandContext {
	
	/**
	 * Get the current user name
	 * @return
	 */
	String getUserName();
	
	/**
	 * Get the current vdb name
	 * @return
	 */
	String getVdbName();
	
	/**
	 * Get the current vdb version
	 * @return
	 */
	int getVdbVersion();
	
	/**
	 * Get the connection id
	 * @return
	 */
	String getConnectionID();
	
	/**
	 * Get the environment properties.  The returned properties are associated only with the currently executing command.
	 * The only built-in key/value in the properties is the key "sessionid" with the same value as getConnectionID()
	 * @return
	 * @deprecated
	 */
	Properties getEnvironmentProperties();
	
	/**
	 * Get the next random double value 
	 * @return
	 */
	double getNextRand();
	
	/**
	 * Sets the seed value and returns the next random double value.  
	 * Additional calls to {@link #getNextRand()} will be based upon the seed value.
	 * @param seed
	 * @return
	 */
	double getNextRand(long seed);
	
	/**
	 * Get the processor batch size set on the BufferManager
	 * @return
	 */
	int getProcessorBatchSize();
	
	/**
	 * Get the server {@link TimeZone}
	 * @return
	 */
	TimeZone getServerTimeZone();
	
	/**
	 * Get the current subject
	 * @return
	 */
	Subject getSubject();

	/**
	 * Get the current session
	 * @return
	 */
	Session getSession();

	/**
	 * Get the current command payload
	 * @return may be null if the client did not set a payload
	 */
	Serializable getCommandPayload();

	/**
	 * Get the current request id 
	 * @return
	 */
	String getRequestId();

}
