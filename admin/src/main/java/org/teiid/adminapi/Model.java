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
 * Represents a metadata model in the Teiid system.
 * 
 * @since 4.3
 */
public interface Model extends AdminObject {
	
	enum Type {PHYSICAL, VIRTUAL, FUNCTION, OTHER};
	
	/**
	 * Description about the model
	 * @return
	 */
	String getDescription();
	
    /**
     * Determine if this model is a Source model.
     * 
     * @return <code>true</code> if it contains physical group(s).
     */
    boolean isSource();

    /**
     * Determine whether this model is exposed for querying.
     * 
     * @return <code>true</code> if the model is visible
     * for querying.
     */
    boolean isVisible();

    /**
     * Retrieve the model type.
     * @return model type
     */
    Type getModelType();

    /** 
     * Determine whether this model can support more than one source.
     * 
     * @return <code>true</code> if this model supports multiple sources
     */
    boolean isSupportsMultiSourceBindings();
    
    /**
     * Associated Source Names for the Models
     * @return String
     */
    List<String> getSourceNames();
    
    /**
     * Get the configured JNDI name for the given source name.
     * @param sourceName - name of the source
     * @return null if none configured.
     */
    String getSourceConnectionJndiName(String sourceName);
    

    /**
     * Get the configured translator name for the given source
     * @param sourceName
     * @return
     */
    String getSourceTranslatorName(String sourceName);
}