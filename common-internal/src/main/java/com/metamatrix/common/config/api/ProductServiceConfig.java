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

import com.metamatrix.common.config.model.ConfigurationVisitor;

/**
 * <p>A ProductServiceConfig (or PSC) is a named collection of service
 * definitions.  A PSC can be deployed, which in effect deploys it's
 * contained service definitions.<p>
 *
 * <p>The <i>type</i> of a PSC will be an instance of ProductType.  The
 * inherited <code>getComponentTypeID</code> method will return a
 * ComponentTypeID object.</p>
 *
 * @see ServiceComponentDefn
 * @see ProductType  
 */
public interface ProductServiceConfig extends ComponentDefn {

    public static final ComponentTypeID PSC_COMPONENT_TYPE_ID = new ComponentTypeID(ProductServiceConfigComponentType.COMPONENT_TYPE_NAME);


    /**
     *@link dependency
     */

    /*#ServiceComponentDefnID lnkServiceComponentDefnID;*/
    /**
     * Returns a Collection of ServiceComponentDefnID objects, which
     * represent the service component definitions that are contained
     * by this product service configuration.
     */
    Collection getServiceComponentDefnIDs();
    
    /**
     * Indicates if the service belongs to this PSC.
     * @returns boolean true if the service is contained in this PSC.
     */
    boolean containsService(ServiceComponentDefnID serviceID);
    
        
    /**
     * Indicates if this Service Definition is enabled within the PSC
     * that contains it; "enabled" refers to it being enabled for
     * deployment, if the containing PSC is deployed.
     * @return boolean indicating if this service definition will be deployed
     * if the PSC which contains it is deployed
     */
    boolean isServiceEnabled(ServiceComponentDefnID serviceID);
    
    
    void accept(ConfigurationVisitor visitor);

}

