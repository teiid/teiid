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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;

public class PropertiedObjectsEditorImpl extends PropertiedObjectEditorImpl implements PropertiedObjectsEditor {

    private List objects = new LinkedList();

    public PropertiedObjectsEditorImpl( PropertyAccessPolicy policy ) {
        super(policy);
    }

    public PropertiedObjectsEditorImpl() {
        super();
    }

    public List getObjects() {
        return objects;
    }

    public void setObjects(List objs) {
        if ( objs == null ) {
            objs = new LinkedList();    
        }
        this.objects = objs;
    }

    /**
     * Return whether this editor currently is set to use any PropertiedObjects.
     * @return true if this editor has at least one PropertiedObject that it is
     * set to use, or false otherwise.
     */
    public boolean hasObjects() {
        return ( this.objects.size() != 0 );
    }

    /**
     * Obtain the list of PropertyDefinitions that are defined by <i>all</i> of the propertied
     * objects this editor is set to use.
     * @return an unmodifiable list of the PropertyDefinition objects that
     * are shared by all instances used by this editor; never null but possibly empty
     */
    public List getSharedPropertyDefinitions() {
        int objSize = this.objects.size();
        if( objSize == 0) {
            return Collections.EMPTY_LIST;
        }
        
        //get a list of prop definitions for the first object
        List propDefns = getPropertyDefinitions((PropertiedObject)this.objects.get(0));
        if(objSize == 1)
            return propDefns;

        List sharedPropDefns = new ArrayList(7);

        //get prop definitions for the rest objects
        List propDefnsForObjects = new ArrayList(objSize - 1);
        for(int i = 1; i < objSize; i++){
            propDefnsForObjects.add(getPropertyDefinitions((PropertiedObject)this.objects.get(i)));
        }
        Iterator iter = propDefns.listIterator();
        boolean isShared;
        while(iter.hasNext()){
            isShared = true;
            Object propDefn = iter.next();
            for(int j = 0; j < propDefnsForObjects.size(); j++){
                if(!((List)propDefnsForObjects.get(j)).contains(propDefn)){
                    isShared = false;
                    break;
                }
            }
            if(isShared)
                sharedPropDefns.add(propDefn);
        }

        return Collections.unmodifiableList(sharedPropDefns);
    }

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
    public List getValues(PropertyDefinition def) {
		ArgCheck.isNotNull(def);
        if( this.objects.size() == 0) {
            return Collections.EMPTY_LIST;
        }
        List values = new ArrayList();
        Iterator iter = objects.listIterator();
        while(iter.hasNext()){
            PropertiedObject propObj = (PropertiedObject)iter.next();
            if(getPropertyDefinitions(propObj).contains(def))
                values.add(getValue(propObj, def));
            else
                values.add(PropertiedObjectEditor.NO_VALUE);
        }
        return Collections.unmodifiableList(values);
    }

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
    public void setValues(PropertyDefinition def, List values ) {
    	ArgCheck.isNotNull(def);
        Assertion.assertTrue(this.hasObjects() && (values.size() == this.getObjects().size()), "Must has objects and the size of objects must be the same as that of the values."); //$NON-NLS-1$

        int s = objects.size();
        for(int i = 0; i < s; i++){
            PropertiedObject propObj = (PropertiedObject)objects.get(i);
            Object value = values.get(i);
            if(getPropertyDefinitions(propObj).contains(def))
                setValue(propObj, def, value);
        }
    }

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
    public Object getValue(int index,PropertyDefinition def) {
    	ArgCheck.isNotNull(def);
        PropertiedObject propObj = (PropertiedObject)objects.get(index);
        if(getPropertyDefinitions(propObj).contains(def)) {
            return getValue(propObj, def);
        }
        return PropertiedObjectEditor.NO_VALUE;
    }

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
    public boolean isValidValue(int index,PropertyDefinition def, Object value ) {
    	ArgCheck.isNotNull(def);
        PropertiedObject propObj = (PropertiedObject)objects.get(index);
        if(getPropertyDefinitions(propObj).contains(def))
            return isValidValue(propObj, def, value);
        return false;   
    }

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
    public void setValue(int index,PropertyDefinition def, Object value) {
    	ArgCheck.isNotNull(def);
        PropertiedObject propObj = (PropertiedObject)objects.get(index);
        if(getPropertyDefinitions(propObj).contains(def))
            setValue(propObj, def, value);
    }

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
    public Object getValue(PropertyDefinition def) {
        List sharedPropDefns = getSharedPropertyDefinitions();
        if(hasObjects() && sharedPropDefns.contains(def)){
            Object value = getValue(0, def);
            Object nextValue;
            int s = objects.size();
            for(int i = 1; i < s; i++){
                nextValue = getValue(i, def);
                if(!value.equals(nextValue))
                    return PropertiedObjectsEditor.DIFFERENT_VALUES;
            }
            return value;
        }
        return PropertiedObjectsEditor.DIFFERENT_VALUES;
    }

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
    public void setValue(PropertyDefinition def, Object value) {
    	ArgCheck.isNotNull(def);

        int s = objects.size();
        for(int i = 0; i < s; i++){
            PropertiedObject propObj = (PropertiedObject)objects.get(i);
            if(getPropertyDefinitions(propObj).contains(def))
                setValue(propObj, def, value);
        }
    }
}

