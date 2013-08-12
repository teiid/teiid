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

package org.teiid.adminapi;

import java.util.List;

/**
 * Represents a Virtual Database in the Teiid System.
 * <br>A VDB has a name and a version.</br>
 * 
 * <p>The identifier pattern for a VDB is <CODE>"name<{@link #DELIMITER_CHAR}>version"</CODE>, 
 * where the name of the VDB and its version represent its unique identifier in the Teiid system.
 * There are no spaces allowed in a given VDB name, and VDB name must start with a letter. 
 * A version number is automatically assigned to a VDB when it is deployed into 
 * a system. A VDB is uniquely identified by <CODE>"name<{@link #DELIMITER_CHAR}>version"</CODE>. 
 * For example: <CODE>"Accounts<{@link #DELIMITER_CHAR}>1"</CODE>, <CODE>"UnifiedSales<{@link #DELIMITER_CHAR}>4</CODE>" etc. 
 * </p>
 * 
 * @since 4.3
 */
public interface VDB extends AdminObject, DomainAware {

    public enum Status{
    	/**
    	 * Initial state waiting for metadata to load
    	 */
    	LOADING,     
    	/**
    	 * In the vdb repository and querable, but not necessarily valid
    	 */
    	ACTIVE,  
    	/**
    	 * A vdb that cannot be successfully loaded - and cannot later transition to active
    	 */
    	FAILED,
    	REMOVED
    };
    
    public enum ConnectionType {NONE, BY_VERSION, ANY}
    
    /**
     * @return Collection of  Teiid Models
     */
    public List<Model> getModels();

    /**
     * @return the status
     */
    public Status getStatus();
    
    /**
     * @return the connection status
     */
    public ConnectionType getConnectionType();

    /**
     * @return the VDB version
     */
    public int getVersion();
    
    /**
     * Get the description of the VDB
     * @return
     */
    public String getDescription();
    
    /**
     * Shows any validity errors present in the VDB
     * @return
     */
    public List<String> getValidityErrors();
    
    /**
     * Shows if VDB is a valid entity
     * @return
     */
    public boolean isValid();
    
    /**
     * Get the data roles defined on this VDB
     * @return
     */
    public List<DataPolicy> getDataPolicies();
    
    /**
     * Get the list of translators defined in the VDB
     * @return
     */
    public List<Translator> getOverrideTranslators();
    
    /**
     * Get the list of vdb imports
     * @return
     */
    public List<? extends VDBImport> getVDBImports();
    
    /**
     * Get the list of other resources included in the VDB
     * @return
     */
    public List<? extends Entry> getEntries();
}
