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

package com.metamatrix.metadata.runtime.api;

import java.util.Date;
import java.util.List;

import com.metamatrix.common.vdb.api.ModelInfo;


/**
 * <p>Instances of this interface represent Models in a Virtual Database.   
 * The values of a Model are analogous to a DataSource or a Virtual DataSource.</p>
 */
public interface Model extends MetadataObject {
    public final static short PRIVATE = ModelInfo.PRIVATE;
    public final static short PUBLIC = ModelInfo.PUBLIC;
    
    
    String getVersion();
    
	/**
	 * Return the description
	 * @return String 
	 */
    String getDescription();
    
	/**
	 * Return the global unique identifier for this Virtual Databse.
	 * @return String 
	 */
    String getGUID();
        
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
     * supports mutliple connector bindings.  
     * If true, {@see #isMultiSourceBindingEnabled()} to determine
     * if the model has been flagged so that the user can
     * actually assign multi connector bindngs.
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
	 * Return boolean indicating if this model is of a physical type.
     * @link {ModelType
	 * @return boolean true if it contains physical group(s).
	 */
    boolean isPhysical();
    
    
    /** 
     * Check whether this model is a materialization
     * of a virtual group.
     * @return Returns the isMaterialization.
     * @since 4.2
     */
    boolean isMaterialization();
    
    /**
     * Returns <code>true</code> if the model is visible to the use
     * for querying
     * @return boolean
     */ 
    boolean isVisible();
    
    
    short getVisibility();
    
    /**
	 * Return boolean indicating whether the model requires connector bindings.
	 * @return boolean 
	 */
    boolean requireConnectorBinding();
    
    /** 
     * Return the type of model
     * @link {com.metamatrix.metamodels.core.ModelType ModelTypes}
     * @return
     */
    int getModelType();
    

    /**
     * Returns the <code>String</code> version of the model type.
     * @return
     */
    String getModelTypeName();    
    

    /**
     * Return the model uri.
     * @return
     */
    String getModelURI();
    
    
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
      
    
}



