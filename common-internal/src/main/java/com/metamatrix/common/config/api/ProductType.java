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

package com.metamatrix.common.config.api;

import java.util.Collection;

/**
 * <p>A Product, in this context, is simply a named collection of
 * Service types.  For example, the MetaMatrix Server product has
 * these service types: Query service, Transaction service, and
 * RuntimeMetadata service.</p>
 *
 * <p>A Product is a ComponentType subclass; it is a ComponentType, but
 * has references to other ComponentTypes (specifically, ComponentTypes
 * representing types of services).  Since it is a ComponentType, it
 * can be thought of as representing a type of Product (either Platform,
 * MetaData Server, or MetaMatrix Server).</p>
 * 
 * <p><b>Note:</b> Product type names where moved to 
 * {@link com.metamatrix.core.util.MetaMatrixProductVersion} in
 * version 5.0 so that other classes that don't have a dependency on
 * com.metamatrix.common can reference them.</p>
 */
public interface ProductType extends ComponentType {
    public static final String COMPONENT_TYPE_NAME = "Product"; //$NON-NLS-1$
    
    public static final ComponentTypeID PRODUCT_TYPE_ID = new ComponentTypeID(COMPONENT_TYPE_NAME);
    public static final ComponentTypeID PRODUCT_SUPER_TYPE_ID = new ComponentTypeID(COMPONENT_TYPE_NAME);
    

    /**
     * Returns a Collection of ComponentTypeID objects, each representing
     * a type of service that this Product is comprised of
     * @return Collection of ComponentTypeID objects
     */
    Collection getComponentTypeIDs();


    /**
     * Returns true if thie component type is contained in
     * this instance of the product type.
     * @param componentTypeID is the id of the component type to check for.
     * @return boolean is true if the component type is contained in the product type
     */
    boolean contains(ComponentTypeID componentTypeID);
}

