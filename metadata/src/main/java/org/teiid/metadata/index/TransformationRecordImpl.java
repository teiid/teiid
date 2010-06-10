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

package org.teiid.metadata.index;

import java.util.List;

import org.teiid.metadata.AbstractMetadataRecord;

/**
 * TransformationRecordImpl
 */
public class TransformationRecordImpl extends AbstractMetadataRecord {

    public static interface Types {
	    public static final String MAPPING            = "Mapping"; //$NON-NLS-1$
	    public static final String SELECT             = "Select"; //$NON-NLS-1$
	    public static final String INSERT             = "Insert"; //$NON-NLS-1$
	    public static final String UPDATE             = "Update"; //$NON-NLS-1$
	    public static final String DELETE             = "Delete"; //$NON-NLS-1$
	    public static final String PROCEDURE          = "Procedure"; //$NON-NLS-1$
	}

	private String transformation;
    private String transformationType;
    private List bindings;
    private List schemaPaths;
    private String resourcePath;
    
    public String getTransformation() {
        return transformation;
    }

    public List getBindings() {
        return this.bindings;
    }

    public List getSchemaPaths() {
        return schemaPaths;
    }

    public String getTransformationType() {
        return transformationType;
    }

    public String getType() {
        return this.transformationType;
    }

    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================

    /**
     * @param string
     */
    public void setTransformation(final String string) {
        transformation = string;
    }

    /**
     * @param string
     */
    public void setTransformationType(String string) {
        transformationType = string;
    }

    /**
     * @param collection
     */
    public void setBindings(List bindings) {
        this.bindings = bindings;
    }

    /**
     * @param collection
     */
    public void setSchemaPaths(List collection) {
        schemaPaths = collection;
    }
    
    /**
     * @return
     */
    public String getResourcePath() {
        return resourcePath;
    }

    /**
     * @param path
     */
    public void setResourcePath(String path) {
        resourcePath = path;
    }

}