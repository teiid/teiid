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

package com.metamatrix.common.object;

import java.util.List;

public interface PropertiedObjectsEditor extends PropertiedObjectEditor {

    /**
     * The value object that is returned by this interface's <code>getValue(PropertyDefinition)</code>
     * method when there is no common value for that property definition on every object in the list
     */
    static Object DIFFERENT_VALUES = "<Different>"; //$NON-NLS-1$

    /**
     * Set the ordered list of PropertiedObject instances that this editor is to use.
     * @param objects the instances of PropertiedObject that are to be used
     * this this editor; may be null or a zero-length collection.
     */
    void setObjects( List objects );

    /**
     * Return the ordered list of PropertiedObject instances that this editor is set to use.
     * @return the instances of PropertiedObject that are used
     * this this editor; may be null or a zero-length collection.
     */
    List getObjects();

    /**
     * Return whether this editor currently is set to use any PropertiedObjects.
     * @return true if this editor has at least one PropertiedObject that it is
     * set to use, or false otherwise.
     */
    boolean hasObjects();

    /**
     * Obtain the list of PropertyDefinitions that are defined by <i>all</i> of the propertied
     * objects this editor is set to use.
     * @return an unmodifiable list of the PropertyDefinition objects that
     * are shared by all instances used by this editor; never null but possibly empty
     */
    List getSharedPropertyDefinitions();

    /**
     * Return the ordered list of property values for the specified property
     * definition, ordered by the list of PropertiedObjects that this editor
     * is currently set to use.  If this editor has no objects that it is using,
     * then this method returns an empty List.
     * <p>
     * The type of objects in the list and whether the list may contain null
     * references depend upon the type and cardinality defined by the PropertyDefinition.
     * @param def the reference to the PropertyDefinition describing the
     * property value is to be returned; may not be null
     * @return the list of values for the property, one for each PropertiedObject
     * this editor is set to use and ordered in the same order; the value
     * of the references in the returned List are dependent upon the PropertyDefinition
     * (i.e., they are a Collection if the property is multi-valued, or may be null
     * if the multiplicity includes "0")
     * @throws AssertionError if <code>def</code> is null
     */
    List getValues(PropertyDefinition def);

    /**
     * Set the value for the specified PropertyDefinition on each of the
     * PropertiedObject instances that this editor is set to use.  The values
     * are correlated to the PropertiedObjects by the order of each list; thus
     * the following must be true:
     * <p>
     * <code>(this.hasObjects() && ( values.size() == this.getObjects().size() )</code>
     * </p>
     * <p>
     * If a PropertiedObject instance in the list of objects this editor is using
     * that does not have the specified PropertyDefinition, the value is not
     * set on that object.
     * @param def the reference to the PropertyDefinition describing the
     * property whose values are to be changed; may not be null
     * @param values the new values for the property organized in the same
     * order as the list of PropertiedObjects that this editor is currently using;
     * may not be null although it may contain null values if the PropertyDefinition
     * has a multiplicity that includes "0")
     * @throws IllegalArgumentException if any of the values are considered invalid
     * for the PropertyDefinition.
     * @throws AssertionError if either of <code>def</code> or <code>values</code> is null
     */
    void setValues(PropertyDefinition def, List values );

    /**
     * Obtain from the PropertiedObject at the specified index the property value
     * that corresponds to the specified PropertyDefinition.  The return type and cardinality
     * (including whether the value may be null) depend upon the PropertyDefinition.
     * @param obj the propertied object whose property value is to be obtained;
     * may not be null
     * @param index the index of the PropertiedObject whose property value is to be returned;
     * must be within the range of the list of PropertiedObjects
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be returned; may not be null
     * @return the value for the property, which may be a collection if
     * the property is multi-valued, or may be null if the multiplicity
     * includes "0", or the NO_VALUE reference if the specified object
     * does not contain the specified PropertyDefinition
     * @throws IndexOutOfBoundsException if <code>index</code> is out of range (index < 0 || index >= this.getObjects().size()).
     * @throws AssertionError if <code>def</code> is null
     */
    Object getValue(int index,PropertyDefinition def);

    /**
     * Return whether the specified value is considered valid.
     * @param index the index of the PropertiedObject whose property value is to be returned;
     * must be within the range of the list of PropertiedObjects
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be validated; may not be null
     * @param value the proposed value for the property, which may be a collection if
     * the property is multi-valued, or may be null if the multiplicity
     * includes "0"
     * @return true if the value is considered valid, or false otherwise.
     * @throws IndexOutOfBoundsException if <code>index</code> is out of range (index < 0 || index >= this.getObjects().size()).
     * @throws AssertionError if <code>def</code> is null
     */
    boolean isValidValue(int index,PropertyDefinition def, Object value );

    /**
     * Set the value of the property defined by the PropertyDefinition on the
     * Propertied PropertiedObject at the specified index in this editor's list
     * of objects.
     * @param index the index of the PropertiedObject whose property value is to be returned;
     * must be within the range of the list of PropertiedObjects
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be changed; may not be null
     * @param value the new value for the property; the cardinality and type
     * must conform PropertyDefinition
     * @throws IllegalArgumentException if the value does not correspond
     * to the PropertyDefinition requirements.
     * @throws IndexOutOfBoundsException if <code>index</code> is out of range (index < 0 || index >= this.getObjects().size()).
     * @throws AssertionError if <code>def</code> is null, or if the object is read only
     */
    void setValue(int index,PropertyDefinition def, Object value);

    /**
     * Determine whether all of PropertiedObjects this editor is using
     * have the same value for the specified PropertyDefinition.  If they do,
     * then return that value; if not, then return the PropertiedObjectsEditor.DIFFERENT_VALUES
     * object.
     * @param def the reference to the PropertyDefinition describing the
     * property whose values are to be evaluated for equivalency; may not be null
     * @return the uniform value that all of the PropertiedObject instances this editor
     * is using have, or PropertiedObjectsEditor.DIFFERENT_VALUES if at least
     * two of the values are different; never null
     */
    Object getValue(PropertyDefinition def);

    /**
     * Set the specified value for the PropertyDefinition on all of PropertiedObjects
     * this editor is using.  The value must be considered valid for the specified
     * PropertyDefinition.
     * If a PropertiedObject instance in the list of objects this editor is using
     * that does not have the specified PropertyDefinition, the value is not
     * set on that object.
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be set on all referenced PropertiedObjects; may not be null
     * @param value the new value for the property; the cardinality and type
     * must conform PropertyDefinition
     * @throws IllegalArgumentException if the value is considered invalid
     * for the PropertyDefinition.
     */
    void setValue(PropertyDefinition def, Object value);

}

