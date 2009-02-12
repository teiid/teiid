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

import com.metamatrix.api.core.message.MessageList;

/**
 * Interface through which metadata defined via XMI is processed.  This interface
 * should be implemented by a component that is to create business objects
 * for XMI metadata content.
 * <p>
 * The XMI processor is designed to hide as much as possible the semantics of XMI by simply constructing
 * {@link FeatureInfo} and {@link EntityInfo} instances as an XMI stream is processed.  These info classes are
 * thus similar to SAX events of a SAX XML document parser.
 * </p>
 * <p>
 * This interface is designed such that an implementation return business objects
 * (which can be anyting, including null) in each of the methods.  Then, when
 * subsequent methods are invoked for XMI content related to that for previously
 * invoked methods, the caller will pass in method calls the appropriate business object.
 * Essentially, this hands back to the implementation a business object that
 * it created and therefore knows what that related metadata represents.
 * </p>
 */
public interface XMIReaderAdapter {
    
    /**
     * Returns the information in the XMI header
     * @return The object that contains the information in the XMI header; may be null
     */
    XMIHeader getHeader();
	
    /**
     * Define the information in the XMI header
     * @param xmiHeader The object that contains the information in the XMI header;
     * may not be null
     * @return the info object created for the header information; may be null
     */
    Object setHeader(XMIHeader xmiHeader);
    
    /**
     * Create a new Entity with the specified information.
     * @param entityInfo the EntityInfo that contains the information about the entity
     * to be created; may not be null
     * @param parentBusObj the object that was the object returned from this method when
     * the this entity's parent entity was created; may be null
     * @return the business object that represents the created entity; may be
     * null.  This object may be passed to subsequent method invocations on this interface when
     * the created entity is to be referenced
     */
    Object createEntity(EntityInfo entityInfo, Object parentBusObj);
    
    /**
     * Create a new Feature with the specified information.  Note that this method may
     * be called multiple times for the same <code>featureInfo</code> instance if 
     * that feature has multiple values.
     * @param featureInfo the FeatureInfo that contains the information about the feature
     * to be created; may not be null
     * @param parentBusObj the object that was the object returned from this method when
     * the this feature's parent entity was created; may be null
     */
    void createFeature(FeatureInfo featureInfo, Object parentBusObj);
    
    /**
     * Notify the implementation that the specified entity has been completed
     * and will have no additional features.
     * @param busObj the business object that is completed and that was returned from
     * the corresponding {@link #createEntity(EntityInfo, Object) createEntity}
     * method; may be null
     */
    void finishEntity(Object busObj);
    
    /**
     * Notify the implementation that the specified feature has been completed
     * and will have no additions or changes.
     * @param featureInfo the FeatureInfo that contains the information about the feature
     * that is now completed; may not be null
     * @param parentBusObj the business object returned from the 
     * {@link #createEntity(EntityInfo, Object) createEntity} method for the entity
     * to which the feature applies; may be null
     */
    void finishFeature(FeatureInfo featureInfo, Object parentBusObj);
    
    /**
     * Notify the implementation that the XMI content has been completely
     * processed.
     */
    void finishDocument();
    
    /**
     * Obtain any messages that have been generated during the use of this interface.
     * @return the collection of messages
     */
    MessageList getMessages();
}

