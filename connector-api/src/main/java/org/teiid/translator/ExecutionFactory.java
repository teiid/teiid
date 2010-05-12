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

package org.teiid.translator;

import java.util.Properties;

import org.teiid.language.Command;
import org.teiid.language.LanguageFactory;
import org.teiid.metadata.RuntimeMetadata;



/**
 * <p>The primary entry point for a Connector.  This interface should be implemented
 * by the connector writer.</p>
 * 
 * <p>The JCA Container will instantiate the implementation of this class. Once the class has been 
 * instantiated, the {@link #start()} method will be called
 * with all necessary connector properties. </p>  
 */
public interface ExecutionFactory {

	/**
	 * Initialize the connector with supplied configuration
	 */
	void start() throws ConnectorException;
	    
	/**
	 * Capabilities Class Name
	 * @return
	 */
	@TranslatorProperty(name="capabilities-class", display="Connector Capabilities",description="The class to use to provide the Connector Capabilities")
	public String getCapabilitiesClass();
	
	/**
	 * Defines if the Connector is read-only connector 
	 * @return
	 */
	@TranslatorProperty(name="immutable", display="Is Immutable",description="Is Immutable, True if the source never changes.",advanced=true, defaultValue="false")
	public boolean isImmutable();
	
	/**
	 * Throw exception if there are more rows in the result set than specified in the MaxResultRows setting.
	 * @return
	 */
	@TranslatorProperty(name="exception-on-max-rows", display="Exception on Exceeding Max Rows",description="Indicates if an Exception should be thrown if the specified value for Maximum Result Rows is exceeded; else no exception and no more than the maximum will be returned",advanced=true, defaultValue="true")
	public boolean isExceptionOnMaxRows();

	/**
	 * Maximum result set rows to fetch
	 * @return
	 */
	@TranslatorProperty(name="max-result-rows", display="Maximum Result Rows", description="Maximum Result Rows allowed", advanced=true, defaultValue="-1")
	public int getMaxResultRows();
	
	/**
	 * Shows the XA transaction capability of the Connector.
	 * @return
	 */
	@TranslatorProperty(name="xa-capable", display="Is XA Capable", description="True, if this connector supports XA Transactions", defaultValue="false")
	public boolean isXaCapable();
	    
    /**
     * Get the Override capabilities for the connector
     * @return
     */
    @TranslatorProperty(name="override-capabilities-file", display="Override capabilities file", description="Property file that defines the override capability properties")
    String getOverrideCapabilitiesFile() throws ConnectorException;
    
    /**
     * Flag that indicates if a underlying source connection required for this execution factory to work 
     * @return
     */
    @TranslatorProperty(name="source-required", display="Source Connection Required", description="Flag that indicates, a source required for the translator to work", readOnly= true)
    boolean isSourceRequired();    
    
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
    
    /**
     * Get the capabilities of this connector.  The capabilities affect what kinds of 
     * queries (and other commands) will be sent to the connector.
     * @return ConnectorCapabilities
     */
    ConnectorCapabilities getCapabilities() throws ConnectorException;
    
    /**
     * Create an execution object for the specified command  
     * @param command the command
     * @param executionContext Provides information about the context that this command is
     * executing within, such as the identifiers for the command being executed
     * @param metadata Access to runtime metadata if needed to translate the command
     * @param connection connection factory object to the data source
     * @return An execution object that can use to execute the command
     */
    Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory) throws ConnectorException;    
}
