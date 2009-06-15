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

package com.metamatrix.platform.security.audit.destination;

import java.util.List;
import java.util.Properties;

import com.metamatrix.dqp.service.AuditMessage;

/**
 * A log destination can be used to print messages - typical examples are
 * a file, System.out, a database, a GUI console, etc.
 */
public interface AuditDestination {

    /**
     * The name of the System property that contains the name of the AuditMessageFormat
     * class that is used to format messages sent to the file destination.
     * This is an optional property; if not specified and the file destination
     * is used, then the {@link com.metamatrix.security.audit.format.DelimitedAuditMessageFormat DelimitedAuditMessageFormat}
     * is used.
     */
    static final String PROPERTY_PREFIX    = "metamatrix.audit."; //$NON-NLS-1$

    /**
     * Get a short description of this logging destination.  This is
     * used for simple reporting.
     * @return Short description
     */
    String getDescription();
    
    /**
	 * Initialize this destination with the specified properties.
     * @param props the properties that this destination should use to initialize
     * itself.
     * @throws AuditDestinationInitFailedException if there was an error during initialization.
     */
	void initialize(Properties props) throws AuditDestinationInitFailedException;

	/**
	 * Get names of all properties used for this destination.
	 */
	List getPropertyNames();

	/**
	 * Print the message to the log destination.
	 * @param message The message to print
	 */
	void record(AuditMessage message);

	/**
	 * Shutdown the log destination and clean up resources.
	 */
	void shutdown();
}
