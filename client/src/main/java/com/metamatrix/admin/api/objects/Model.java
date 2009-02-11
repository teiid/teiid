/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.admin.api.objects;

import java.util.List;

/** 
 * Represents a metadata model in the MetaMatrix system.
 * 
 * @since 4.3
 */
public interface Model extends AdminObject {

    /**
     * Return the connector binding names for this Virtual Databse.
     * @return connector bindings bound to this model. 
     */
    List getConnectorBindingNames();

    /**
     * Determine if this model is a physical type.
     * 
     * @return <code>true</code> iff it contains physical group(s).
     */
    boolean isPhysical();

    /**
     * Determine whether this model is exposed for querying.
     * 
     * @return <code>true</code> iff the model is visible
     * for querying.
     */
    boolean isVisible();

    /**
     * Retrieve the model type.
     * TODO: one of ...
     * @return model type
     */
    String getModelType();

    /**
     * Retrive the model URI.
     * 
     * @return model URI
     */
    String getModelURI();

    /** 
     * Determine whether this model can support more than one connector binding.
     * 
     * @return <code>true</code> iff this model supports multi-source bindings
     */
    boolean supportsMultiSourceBindings();
    
    /** 
     * Determine whether this model is a Materialization Model
     *  
     * @return isMaterialization whether the model is a Materialization Model.
     * @since 4.3
     */
    
    boolean isMaterialization();

}