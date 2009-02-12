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

package com.metamatrix.modeler.core.metadata.runtime;

import java.util.List;

/**
 * TransformationRecord
 */
public interface TransformationRecord extends MetadataRecord {
	
    interface Types {
        public static final String MAPPING            = "Mapping"; //$NON-NLS-1$
        public static final String SELECT             = "Select"; //$NON-NLS-1$
        public static final String INSERT             = "Insert"; //$NON-NLS-1$
        public static final String UPDATE             = "Update"; //$NON-NLS-1$
        public static final String DELETE             = "Delete"; //$NON-NLS-1$
        public static final String PROCEDURE          = "Procedure"; //$NON-NLS-1$
    }

    /**
     * Get the transformation definition, which is typically an XML document containing the
     * tree of query nodes.
     * @return the string containing the definition of the transformation.
     */
    String getTransformation();

    /**
     * Get any bindings to the transformation, these could be inputset bindings.
     * @return a list of binding names
     */    
    List getBindings();

    /**
     * Get any paths to the various schemas that the XML document depends on.
     * @return a list of schema path names
     */    
    List getSchemaPaths();    

    /**
     * Get an identifier for the object that is the result of the transformation.
     * The transformed object is either a table or a procedure, depending upon the 
     * {@link MetadataConstants#getSqlTransformationTypeName(short)}.
     * @return an identifier for the virtual object
     */    
    Object getTransformedObjectID();

    /**
     * Get the transformation type, get the type 
     * {@link com.metamatrix.modeler.core.metamodel.aspect.sql.SqlTransformationAspect.Types}
     * @return the string containing the type of the transformation.
     */
    String getTransformationType();

    /**
     * Return the type of TRANSFORMATION it is. 
     * @return transformTyype
     *
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataConstants.SQL_TRANSFORMATION_TYPES
     */
    String getType();

}