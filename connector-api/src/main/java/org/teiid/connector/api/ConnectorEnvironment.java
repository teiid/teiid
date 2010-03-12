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

import org.teiid.connector.language.LanguageFactory;


/**
 * The environment provided to a connector by the Connector Manager.  The 
 * environment provides access to external resources the Connector writer may
 * need.
 */
public interface ConnectorEnvironment {

	/**
	 * Capabilities Class Name
	 * @return
	 */
	public String getCapabilitiesClass();
	
	/**
	 * Defines if the Connector is read-only connector 
	 * @return
	 */
	public boolean isImmutable();
	
	/**
	 * Throw exception if there are more rows in the result set than specified in the MaxResultRows setting.
	 * @return
	 */
	public boolean isExceptionOnMaxRows();

	/**
	 * Maximum result set rows to fetch
	 * @return
	 */
	public int getMaxResultRows();
	
	/**
	 * Shows the XA transaction capability of the Connector.
	 * @return
	 */
	public boolean isXaCapable();
	
    /**
     * Obtain a reference to the logger that can be used to add messages to the 
     * log files for debugging and error recovery.
     * @return The {@link ConnectorLogger} 
     */
    ConnectorLogger getLogger();
    
    /**
     * Obtain a reference to the default LanguageFactory that can be used to construct
     * new language interface objects.  This is typically needed when modifying the language
     * objects passed to the connector or for testing when objects need to be created. 
     */
    LanguageFactory getLanguageFactory();
    
    /**
     * Obtain a reference to the type facility, which can be used to perform many type 
     * conversions supplied by the Connector API.
     */
    TypeFacility getTypeFacility();
    
    /**
     * Get the Override capabilities for the connector
     * @return
     */
    Properties getOverrideCapabilities() throws ConnectorException;
}
