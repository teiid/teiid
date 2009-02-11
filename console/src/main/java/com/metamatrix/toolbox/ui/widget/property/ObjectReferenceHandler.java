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

//
package com.metamatrix.toolbox.ui.widget.property;

import java.util.Collection;

import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinition;

/**
 * An interface for classes that can supply ObjectReference type PropertyDefinition values to
 * the PropertiedObjectPanel and it's ObjectReferencePropertyComponent.
 */
public interface ObjectReferenceHandler {

    /**
     * Defines a statically available "null" object that implementations should use to signal
     * the framework that the user wishes to set the ObjectReference to null.  The framework will
     * test the reference returned by getObjectReference and getObjectReferences against this
     * instance and, if they are ==, it will null out the property value on the object.
     */
    public static final Object NULL_OBJECT = new Object();

    /**
     * Return a value for the specified ObjectReference type PropertyDefinition on the specified
     * PropertiedObject.  This method is called when the PropertyDefinition has max multiplicity = 1.
     * @param object the PropertiedObject that is requesting a setValue
     * @param editor the PropertiedObjectEditor for this transaction
     * @param def the PropertyDefinition of type PropertyTypes.OBJECT_REFERENCE
     * @param currentValue the current reference for this PropertyDefinition's value.  May be null if
     * no value has been set.
     * @return the Object that should be set as the value of the specified PropertyDefinition, replacing
     * the current value. Returning null is interpreted by the framework as a "cancel" request and the
     * existing value will not be modified. Returning ObjectReferenceSupplier.NULL_OBJECT is interpreted
     * by the framework as a request to null out the existing value.
     */
    Object getObjectReference(PropertiedObject object,
                              PropertiedObjectEditor editor,
                              PropertyDefinition def,
                              Object currentValue);

    /**
     * Return a Collection of values for the specified ObjectReference type PropertyDefinition on
     * the specified PropertiedObject.  This method is called when the PropertyDefinition has
     * max multiplicity > 1.
     * @param object the PropertiedObject that is requesting a setValue
     * @param editor the PropertiedObjectEditor for this transaction
     * @param def the PropertyDefinition of type PropertyTypes.OBJECT_REFERENCE
     * @param currentValues an unmodifiable Collection of current references for this
     * PropertyDefinition's value. If no value has been set, the Collection will be empty.
     * Will never be null.
     * @return an Object value that will be SET to value of the specified PropertyDefinition.
     * Returning null is interpreted by the framework as a "cancel" request and the existing values
     * will not be modified. Returning an empty Collection is interpreted by the framework as a request
     * delete all the existing values.
     */
    Object[] getObjectReferences(PropertiedObject object,
                                PropertiedObjectEditor editor,
                                PropertyDefinition def,
                                Collection currentValues);

    /**
     * Determine if it is possible to navigate to the specified ObjectReference value.
     * @param objectReference a non-null reference value for a PropertyDefinition of type
     * PropertyTypes.OBJECT_REFERENCE.
     * @return true if this handler knows how to navigate to this object, false otherwise to
     * disable the ObjectReferencePropertyComponent's navigate action.
     */
    boolean canNavigateTo(Object objectReference);

    /**
     * Navigate to the specified ObjectReference value.  Called when the user activates the
     * ObjectReferencePropertyComponent's navigate action.
     * @param objectReference a non-null reference value for a PropertyDefinition of type
     * PropertyTypes.OBJECT_REFERENCE.
     */
    void navigateTo(Object objectReference);

}
