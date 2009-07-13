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

/*
 */
package com.metamatrix.common.vdb.api;

import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This interface provides the model information.
 */
public interface ModelInfo {
	
    public final static short PRIVATE = 2;
    public final static short PUBLIC = 0;
    
    public final static String PRIVATE_VISIBILITY = "Private"; //$NON-NLS-1$
    public final static String PUBLIC_VISIBILITY = "Public"; //$NON-NLS-1$
    
    /**
     * Get this model's UUID.
     * @return The UUID as a String.
     */
    String getUUID();

    /**
     * Get this model's name.
     * @return The Model name.
     */
    String getName();
    
    /**
     * Get this model's description.
     * @return The Model description.
     */
    String getDescription();    

    /**
     * Get this model's verion.
     * @return The Model's version.
     */
    String getVersion();

    /**
     * Get the date this model was versioned.
     * @return The model's version date.
     */
    Date getDateVersioned();

    /**
     * Get the name of the person who versioned this model.
     * @return The name of the Model versioner.
     */
    String getVersionedBy();

    /**
     * Whether this is a physical model.
     * @return true if this is a physicalmodel.
     */
    boolean isPhysical();

    /**
     * Whether this model requires that a connector must be
     * bound to it.
     * @return true if this model requires a connector.
     */
    boolean requiresConnectorBinding();
            
    /**
     * Return the connector binding name(s).  The only time this list
     * would have more than one connector binding name, is if the model 
     * supports (@see #supportsMultiSourceBindings()).
     * @return
     * @since 4.2
     */
    List getConnectorBindingNames();
    
    /**
     * Returns true if the model, based on its model type,
     * supports multiple connector bindings.  
     * If true, {@see #isMultiSourceBindingEnabled()} to determine
     * if the model has been flagged so that the user can
     * actually assign multi-connector bindings.
     * @return
     * @since 4.2
     */
    boolean supportsMultiSourceBindings();    
    
    /**
     * Returns true if the model has been enabled to have
     * multiple connector bindings assigned. 
     * @return
     * @since 4.2
     */
    boolean isMultiSourceBindingEnabled();
    
    /**
     * Returns <code>true</code> if the model is visible to the use
     * for querying
     * @return boolean
     */       
    boolean isVisible();
    
    short getVisibility();
   
    /** 
     * Check whether this model is a materialization
     * of a virtual group.
     * @return Returns the isMaterialization.
     * @since 4.2
     */
    boolean isMaterialization();
    
    /**
     * Get the list of DDL file names that can be used to generate this
     * materialization.  
     * 
     * <p>If this model is not a materialization, the
     * returned String[] will be empty.</p>
     * @return A list of file names of DDL file names contained in the VDB.
     * @see #getDDLFileContentsAsStream(String)
     * @since 4.2
     */
    String[] getDDLFileNames();
    
    /**
     * Get the contents of the given DDL file name.  
     * @param ddlFileName The name for which to get the DDL.
     * @return The stream contents of the DDL file or null if this modle has
     * no DDL for the given file name or is not a materialization.
     * @see #getDDLFileNames()
     * @since 4.2
     */
    InputStream getDDLFileContentsAsStream(String ddlFileName);
    
    /**
     * Get the contents of the given DDL file name.  
     * @param ddlFileName The name for which to get the DDL.
     * @return The byte[] contents of the DDL file or null if this model has
     * no DDL for the given file name or is not a materialization.
     * @see #getDDLFileNames()
     * @since 4.2
     */
    byte[] getDDLFileContentsGetBytes(String ddlFileName);
    
    /**
     * Set the Map of DDL file names to DDL file byte arrays. 
     * @param ddlFileNamesToFiles The Map String->byte[];
     * 
     * @since 4.2
     */
    void setDDLFiles(Map ddlFileNamesToFiles);

    /** 
     * Return the type of model
     * @link {com.metamatrix.metamodels.core.ModelType ModelTypes}
     * @return
     */
    int getModelType();
    
    /**
     * Return the model uri.
     * @return
     */
    String getModelURI();
        
    /**
     * Returns the <code>String</code> version of the model type.
     * @return
     */
    String getModelTypeName();
    
    /**
     * Enable the model to have multiple connector bindings
     * associated by passing in <code>true</code>.  
     * @see #isMultiSourceBindingEnabled() to determine if the
     * model is enabled.  
     * 
     * Just because a model supports mutliple bindings, does not
     * mean it will be enabled. 
     * @param isEnabled
     * @since 4.2
     */
    void enableMutliSourceBindings(boolean isEnabled);
            
    
    String getPath();
    
    Properties getProperties();
    
}
