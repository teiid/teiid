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

import com.metamatrix.core.util.ArgCheck;

public class PropertiedObjectEditorImpl implements PropertiedObjectEditor {

    private PropertyAccessPolicy policy;

    public PropertiedObjectEditorImpl( PropertyAccessPolicy policy ) {
    	ArgCheck.isNotNull(policy);
        this.policy = policy;
    }

    /**
     * Create an empty property definition object with all defaults.
     */
    public PropertiedObjectEditorImpl() {
        this( new DefaultPropertyAccessPolicy() );
    }

    protected PropertiedObjectImpl assertPropertiedObject( PropertiedObject obj ) {
    	ArgCheck.isInstanceOf(PropertiedObjectImpl.class, obj);
        return (PropertiedObjectImpl) obj;
    }

	// ########################## PropertiedObjectEditor Methods ###################################

    /**
     * Obtain the list of PropertyDefinitions that apply to the specified object's type.
     * @param obj the propertied object for which the PropertyDefinitions are
     * to be obtained; may not be null
     * @return an unmodifiable list of the PropertyDefinition objects that
     * define the properties for the object; never null but possibly empty
     * @throws AssertionError if <code>obj</code> is null
     */
    public List getPropertyDefinitions(PropertiedObject obj) {
        PropertiedObjectImpl descriptor = assertPropertiedObject(obj);
        return descriptor.getPropertyDefinitions();
    }

    /**
     * Get the allowed values for the property on the specified object.
     * By default, this implementation simply returns the allowed values in the
     * supplied PropertyDefinition instance.
     * @param obj the propertied object whose property value is to be obtained;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be returned; may not be null
     * @return the unmodifiable list of allowed values for this property, or an empty
     * set if the values do not have to conform to a fixed set.
     * @see #hasAllowedValues
     */
    public List getAllowedValues(PropertiedObject obj, PropertyDefinition def) {
    	ArgCheck.isNotNull(def);
        return def.getAllowedValues();    
    }

    /**
     * Obtain from the specified PropertiedObject the property value
     * that corresponds to the specified PropertyDefinition.  The return type and cardinality
     * (including whether the value may be null) depend upon the PropertyDefinition.
     * If the property definition allows multiple values (i.e., its maximum multiplicity
     * is greater than 1), the returned Object will be an Object[] containing
     * Objects of the type prescribed by the PropertyDefinition.
     * @param obj the propertied object whose property value is to be obtained;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be returned; may not be null
     * @return the value for the property, which will be an object array if the
     * property is multi-valued;  the result may be null if the multiplicity
     * includes "0", or the result may be the NO_VALUE reference if the specified
     * object does not contain the specified PropertyDefinition
     * @throws AssertionError if either of <code>obj</code> or <code>def</code> is null
     */
    public Object getValue(PropertiedObject obj, PropertyDefinition def) {
        PropertiedObjectImpl propObj = assertPropertiedObject(obj);
        ArgCheck.isNotNull(def);
        return propObj.getValue(def);
    }

    /**
     * Return whether the specified value is considered valid.  The value is not
     * valid if the propertied object does not have the specified property definition,
     * or if it does but the value is inconsistent with the requirements of the
     * property definition.  If the property is multi-valued, the value is expected
     * to be an instance of Object[].
     * @param obj the propertied object whose property value is to be validated;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be validated; may not be null
     * @param value the proposed value for the property, which must be an object array if
     * the property is multi-valued, and which may be null if the multiplicity
     * includes "0"
     * @return true if the value is considered valid, or false otherwise.
     * @throws AssertionError if either of <code>obj</code> or <code>def</code> is null,
     * or if the property is multi-valued and the <code>value</code> is not an instance
     * of Object[].
     */
    public boolean isValidValue(PropertiedObject obj, PropertyDefinition def, Object value ) {
        PropertiedObjectImpl propObj = assertPropertiedObject(obj);
        ArgCheck.isNotNull(def);
        return propObj.isValidValue(def,value);
    }

    /**
     * Set on the specified PropertiedObject the value defined by the specified PropertyDefinition.
     * @param obj the propertied object whose property value is to be set;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be changed; may not be null
     * @param value the proposed value for the property, which must be an object array if
     * the property is multi-valued, and which may be null if the multiplicity
     * includes "0"
     * @throws IllegalArgumentException if the value does not correspond
     * to the PropertyDefinition requirements.
     * @throws AssertionError if either of <code>obj</code> or <code>def</code> is null,
     * or if the property is multi-valued and the <code>value</code> is not an instance
     * of Object[].
     */
    public void setValue(PropertiedObject obj, PropertyDefinition def, Object value) {
        PropertiedObjectImpl propObj = assertPropertiedObject(obj);
        ArgCheck.isNotNull(def);
        propObj.setValue(def, value);
    }

    public PropertyAccessPolicy getPolicy() {
        return this.policy;
    }

    public void setPolicy(PropertyAccessPolicy policy) {
        if ( policy == null ) {
            this.policy = new DefaultPropertyAccessPolicy();
        } else {
            this.policy = policy;
        }
    }

	// ########################## PropertyAccessPolicy Methods ###################################

    public boolean isReadOnly(PropertiedObject obj) {
        return this.policy.isReadOnly(obj);
    }

    public boolean isReadOnly(PropertiedObject obj, PropertyDefinition def) {
    	ArgCheck.isNotNull(obj);
    	ArgCheck.isNotNull(def);
        return this.policy.isReadOnly(obj,def);
    }

    public void setReadOnly(PropertiedObject obj, PropertyDefinition def, boolean readOnly) {
    	ArgCheck.isNotNull(obj);
    	ArgCheck.isNotNull(def);
        this.policy.setReadOnly(obj,def,readOnly);
    }

    public void setReadOnly(PropertiedObject obj, boolean readOnly) {
    	ArgCheck.isNotNull(obj);
        this.policy.setReadOnly(obj,readOnly);
    }

    public void reset(PropertiedObject obj) {
    	ArgCheck.isNotNull(obj);
        assertPropertiedObject(obj);
        this.policy.reset(obj);
    }

}

