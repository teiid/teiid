/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
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