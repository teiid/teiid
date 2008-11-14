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

package com.metamatrix.toolbox.ui.widget.property;

/**
 * Defines interface for custom property dislpay JComponents to callback for validating
 * partial entries of property values.  This interface is implemented by PropertyChangeAdapter
 * and is broken out here to simplify the implementation requirements of custom components.
 */
public interface PropertyValidationListener {

    /**
     * Request that the specified property value be validated in the current context.  The
     * result of this method will be communicated via PropertyComponent.setValidity().
     * @param source the custom PropertyComponent requesting validation.
     * @param value the property value that should be validated.
     */
    void isValueValid(PropertyComponent source, Object value);

}


