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

package com.metamatrix.api.core.xmi;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
 * Class for managing information during the creation of Features.  An instance of this class represents
 * a particular feature and (optiona) value on an {@link EntityInfo owner object}.
 * <p>
 * The XMI processor is designed to hide as much as possible the semantics of XMI by simply constructing
 * {@link FeatureInfo} and {@link EntityInfo} instances as an XMI stream is processed.  These info classes are
 * thus similar to SAX events of a SAX XML document parser.
 * </p>
 */
public class FeatureInfo {
    /** The URI of the metamodel that contains the metamodel entity (or metaclass) for this entity */
    private String metamodelURI;

     /** The name (without metamodel prefix) of the metamodel entity (or metaclass) for this entity */
    private String metaClassName;

    /** The name of the feature */
    private String featureName;

    /** The (optional) value for the feature */
    private Object value;

    /** The EntityInfo for the entity that contains this feature */
    private final EntityInfo ownerEntity;

    /**
     * Construct a new instance of the FeatureInfo using the feature name, metamodel entity's name and
     * metamodel URI.
     * @param ownerEntity the EntityInfo for the entity on which this feature exists; may not be null
     * @param featureName the name of the feature
     * @param metaClassName the name (without metamodel namespace prefix) of the metamodel entity
     * to which this feature applies; may not be null or zero-length
     * @param uri the URI of the metamodel namespace; may be null or zero-length,
     * which means the metaClassName must unambigously identify a single metaclass within
     * one of the metamodels defined in the header.
     * @throws IllegalArgumentException if the supplied name is null or zero-length, feature
     * name is null or zero-length, or the ownerEntity reference is null
     */
    public FeatureInfo(EntityInfo ownerEntity, String featureName, String metaClassName, String uri) {
        if ( featureName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0017) );
        }
        if ( featureName.length() == 0 ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0018) );
        }
        if ( metaClassName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0019) );
        }
        if ( metaClassName.length() == 0 ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0020) );
        }
        if ( ownerEntity == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0021) );
        }
        this.featureName = featureName;
        this.metamodelURI = uri;
        this.metaClassName = metaClassName;
        this.ownerEntity = ownerEntity;
    }

    /**
     * Get the name of the feature.
     * @return Returns a String
     */
    public String getName() {
        return featureName;
    }

    /**
     * Set the name of the feature.
     * @param featureName The name of the feature
     */
    public void setName(String featureName) {
        if ( featureName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0017) );
        }
        if ( featureName.length() == 0 ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0018) );
        }
        this.featureName = featureName;
    }

    /**
     * Get the name of the metaclass to which this feature applies.
     * @return the name of the metaclass; never null or zero-length
     */
    public String getMetaClassName() {
        return metaClassName;
    }

    /**
     * Set the name of the metaclass to which this feature applies.
     * @param metaClassName the name (without metamodel namespace prefix) of the metamodel entity;
     * may not be null or zero-length
     * @throws IllegalArgumentException if the supplied name is null or zero-length
     */
    public void setMetaClassName(String metaClassName) {
        if ( metaClassName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0019) );
        }
        if ( metaClassName.length() == 0 ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0020) );
        }
        this.metaClassName = metaClassName;
    }

    /**
     * Get the URI of the namespace for the metamodel.
     * @return the URI of the metamodel namespace
     */
    public String getMetaModelURI() {
        return metamodelURI;
    }

    /**
     * Set the URI for the namespace for the metamodel.
     * @param uri the URI of the metamodel namespace; may be null or zero-length,
     * which means the metaClassName must unambigously identify a single metaclass within
     * one of the metamodels defined in the header.
     */
    public void setMetaModelURI(String uri) {
        this.metamodelURI = uri;
    }

    /**
     * Return the string representation for this entity.
     * @return String representation for this object, which is of the form
     * "<metaClassName>.<name>=<value>"
     */
    public String toString(){
        StringBuffer sb = new StringBuffer();
        if ( this.getMetaModelURI() != null ) {
            sb.append(this.getMetaModelURI());
            sb.append(":"); //$NON-NLS-1$
        }
        sb.append(this.getMetaClassName());
        sb.append("."); //$NON-NLS-1$
        sb.append(this.getName());
        sb.append("="); //$NON-NLS-1$
        sb.append(this.getValue());
        return sb.toString();
    }


    /**
     * Get the value of the feature
     * @return Returns the value
     */
    public Object getValue() {
        return value;
    }

    /**
     * Set the value of the feature
     * @param value The value to set
     */
    public void setValue(Object value) {
        this.value = value;
    }

    /**
     * Gets the EntityInfo for the feature on the entity that contains this entity.
     * @return Returns the entity info; never null.
     */
    public EntityInfo getOwnerEntityInfo() {
        return ownerEntity;
    }

}
