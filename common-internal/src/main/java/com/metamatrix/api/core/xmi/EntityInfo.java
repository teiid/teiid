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

import org.xml.sax.Attributes;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
 * Represents the minimal information required for XMI entities.
 * This class is used when reading in XMI files to manage the
 * identification information for an entity.
 * <p>
 * The XMI processor is designed to hide as much as possible the semantics of XMI by simply constructing
 * {@link FeatureInfo} and {@link EntityInfo} instances as an XMI stream is processed.  These info classes are
 * thus similar to SAX events of a SAX XML document parser.
 * </p>
 */
public class EntityInfo {
    /** The URI of the metamodel that contains the metamodel entity (or metaclass) for this entity */
    private String metamodelURI;

    /** The name (without metamodel URI) of the metamodel entity (or metaclass) for this entity */
    private String metaClassName;

     /** The fully qualified name (with metamodel URI) of the metamodel entity (or metaclass) for this entity */
    private String qMetaClassName;
    
    /** May be null... optional property to assist with object navigation **/
    private String parentId;

    /** The (optional) name of the entity */
    private String entityName;

    /** The (optional) stringified XMI ID for the entity, which is usually local to the file */
    private String entityID;

    /** The (optional) stringified XMI UUID for the entity */
    private String entityUUID;
    
    /** The (optional) complete set of XMI Attributes */
    private Attributes attributes;

    /** The (optional) locator for the model construct referenced via an XLink.  The
     * value should be of the form
     * <p>
     * <code>   &ltURI>|&ltNAME></code>
     * </p>
     * <p>
     * where
     * <ul>
     *    <li>&ltURI> locates the file that contains the model construct, or an empty string
     *        if the model construct is in the same document.</li>
     *    <li>&ltNAME> the value of an XPointer specifying the referenced model
     *        construct; generally, when the URI is not given, the XPointer must
     *        reference the ID attribute, such as <code>id(NAME)</code>, for which
     *        the shorthand notation is just <code>NAME</code>.</li>
     * </ul>
     * </p>
     * <p>
     * The <code>href</code> value "<code>mydoc.xml|descendent(1,type,attr,value)</code>" references
     * the model construct in "mydoc.xml" with the expected XML attribute "attr" under
     * the XML tag of "type" and where the attribute value is "value".
     * The constant <code>#element</code> can be used for any type.
     * </p>
     * <p>
     * For example, the <code>href</code> value "<code>mydoc.xml|descendant(1,#element,xmi.label,class1)<code>" ,
     * Also, the <code>href</code> value "<code>mydoc.xml|xxxx-yyyy</code>" references
     * (using the shorthand form) the model construct in "mydoc.xml" with the ID value of "xxxx-yyyy".
     * </p>
     */
    private String entityHref;

    /** The (optional) label for the entity */
    private String entityLabel;

    /** The (optional) reference by XMI ID to another entity within the same document */
    private String entityIDRef;

    /** The (possibly null) reference to the FeatureInfo on an entity that contains this entity */
    private final FeatureInfo ownerFeature;

    /**
     * Construct a new instance of the EntityInfo using the metamodel entity's name.
     * @param ownerFeature the feature of this entity's owner that describes the ownership; may be null
     * if there is no owner
     * @param metaClassName the name (without metamodel namespace prefix) of the metamodel entity;
     * may not be null or zero-length
     */
    public EntityInfo(FeatureInfo ownerFeature, String metaClassName) {
        this(ownerFeature,metaClassName,null);
    }

    /**
     * Construct a new instance of the EntityInfo using the metamodel entity's name and URI.
     * @param ownerFeature the feature of this entity's owner that describes the ownership; may be null
     * if there is no owner
     * @param metaClassName the name (without metamodel namespace prefix) of the metamodel entity;
     * may not be null or zero-length
     * @param uri the URI of the metamodel namespace; may be null or zero-length,
     * which means the metaClassName must unambigously identify a single metaclass within
     * one of the metamodels defined in the header.
     * @throws IllegalArgumentException if the supplied name is null or zero-length
     */
    public EntityInfo(FeatureInfo ownerFeature, String metaClassName, String uri) {
        if ( metaClassName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0013) );
        }
        if ( metaClassName.length() == 0 ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0014) );
        }
        this.metaClassName = metaClassName;
        this.qMetaClassName = uri + ":" + metaClassName; //$NON-NLS-1$
        this.metamodelURI = uri;
        this.ownerFeature = ownerFeature;
        this.entityName = metaClassName;
    }

    /**
     * Gets the metaModelEntityName.
     * @return the name of the metaclass; never null or zero-length
     */
    public String getMetaClassName() {
        return metaClassName;
    }
    
    /**
     * Accessor for the parentId field... may be null if not explicity set in content handler
     * @return parentId
     */
    public String getParentId(){
        return this.parentId;
    }
    
    /**
     * Setter for ParentId field.
     * @param id
     */
    public void setParentId(final String id){
        this.parentId = id;
    }
    
    /**
     * Gets the qMetaModelEntityName.
     * @return the MetaModel URI qualified name of the metaclass; never null or zero-length
     */
    public String getQMetaClassName() {
        return qMetaClassName;
    }

    /**
     * Sets the metaModelEntityName.
     * @param uri of the metamodel, may not be null or zero-length
     * @param metaClassName the name (without metamodel namespace prefix) of the metamodel entity;
     * may not be null or zero-length
     * @throws IllegalArgumentException if the supplied name is null or zero-length
     */
    public void setQMetaClassName(String uri, String metaClassName) {
        if ( metaClassName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0013) );
        }
        if ( metaClassName.length() == 0 ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0014) );
        }

        if ( uri == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0015) );
        }
        if ( uri.length() == 0 ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0016) );
        }
        this.qMetaClassName = uri + ":" + metaClassName; //$NON-NLS-1$
    }

    /**
     * Sets the metaModelEntityName.
     * @param metaClassName the name (without metamodel namespace prefix) of the metamodel entity;
     * may not be null or zero-length
     * @throws IllegalArgumentException if the supplied name is null or zero-length
     */
    public void setMetaClassName(String metaClassName) {
        if ( metaClassName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0013) );
        }
        if ( metaClassName.length() == 0 ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0014) );
        }

        this.metaClassName = metaClassName;
    }

    /**
     * Get the name of the metamodel entity without its metamodel namespace prefix.
     * @return the name (without prefix) of the metamodel entity
     */
    public String getName() {
        return entityName;
    }

    /**
     * Set the name of the metamodel entity.
     * @param name the metamodel entity name (without metamodel namespace prefix); may be null or zero-length
     */
    public void setName(String name) {
        this.entityName = name;
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
     * "<metaClassName>.<name>"
     */
    public String toString(){
        StringBuffer sb = new StringBuffer();
        if ( this.getMetaModelURI() != null ) {
            sb.append(this.getMetaModelURI());
            sb.append(":");     //$NON-NLS-1$
        }
        sb.append(this.getMetaClassName());
        sb.append("="); //$NON-NLS-1$
        sb.append(this.getName());
        return sb.toString();
    }

    /**
     * Get the value of the XMI ID.
     * @return the string ID for this entity
     */
    public String getID() {
        return entityID;
    }

    /**
     * Set the value of the XMI ID.
     * @param xmiID the string ID for this entity; may be null or zero-length
     */
    public void setID(String xmiID) {
        this.entityID = xmiID;
    }

    /**
     * Gets the uuid.
     * @return Returns a String
     */
    public String getUUID() {
        return entityUUID;
    }

    /**
     * Sets the uuid.
     * @param uuid The uuid to set; may be null or zero-length
     */
    public void setUUID(String uuid) {
        this.entityUUID = uuid;
    }

    /**
     * Gets the href.
     * @return Returns a String
     */
    public String getHref() {
        return entityHref;
    }

    /**
     * Sets the href.
     * @param href The href to set; may be null or zero-length
     */
    public void setHref(String href) {
        this.entityHref = href;
    }

    /**
     * Gets the label.
     * @return Returns a String
     */
    public String getLabel() {
        return entityLabel;
    }

    /**
     * Sets the label.
     * @param label The label to set; may be null or zero-length
     */
    public void setLabel(String label) {
        this.entityLabel = label;
    }

    /**
     * Gets the idRef.
     * @return Returns a String
     */
    public String getIDRef() {
        return entityIDRef;
    }

    /**
     * Sets the entityIDRef.
     * @param entityIDRef The entityIDRef to set; may be null or zero-length
     */
    public void setIDRef(String entityIDRef) {
        this.entityIDRef = entityIDRef;
    }

    /**
     * Gets the FeatureInfo for the feature on the entity that contains this entity
     * (e.g., this entity's parent entity).
     * @return Returns the feature info; if this entity represents a root (i.e.,
     * it has no parent), then this will be null.
     */
    public FeatureInfo getOwnerFeatureInfo() {
        return ownerFeature;
    }

    /**
     * @return
     */
    public Attributes getAttributes() {
        return attributes;
    }

    /**
     * @param attributes
     */
    public void setAttributes(Attributes attributes) {
        this.attributes = attributes;
    }

}
