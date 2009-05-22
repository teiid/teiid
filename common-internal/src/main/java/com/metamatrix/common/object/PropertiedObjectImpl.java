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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.HashCodeUtil;

/**
 * This class provides an implementation of a PropertiedObject, including basic
 * management of properties and property definitions.  However, the <code>compareTo</code>
 * and <code>equals</code> assumes that the values either implement the Comparable
 * interface <i>or</i> they are comparable after <code>toString()</code> is called
 * on each value.
 */
public class PropertiedObjectImpl implements PropertiedObject, Serializable, Cloneable, Comparable {

//    private int hashcode;
    private Map properties;
    private List unmodifiablePropertyDefns;

    /**
     * Create an instance of the PropertiedObject, specifying the list of
     * PropertyDefinition instances to be used.
     * @param propertyDefns the list of PropertyDefinition instances; the list
     * is assumed to be immutable, and may never be null
     */
    public PropertiedObjectImpl( List propertyDefns ) {
    	ArgCheck.isNotNull(propertyDefns);
        this.properties = new HashMap();
        this.unmodifiablePropertyDefns = propertyDefns;
        updateHashCode();
    }

    protected PropertiedObjectImpl( PropertiedObjectImpl original ) {
        Assertion.isNotNull(original,"The original PropertiedObjectImpl reference may not be null"); //$NON-NLS-1$
        this.properties = new HashMap(original.properties);
        this.unmodifiablePropertyDefns = original.unmodifiablePropertyDefns;    // can reuse reference, since unmodifable
        updateHashCode();
    }

    /**
     * Update the currently cached hash code value with a newly computed one.
     * This method should be called in any subclass method that updates any field.
     * This includes constructors.
     * <p>
     * The implementation of this method invokes the <code>computeHashCode</code>
     * method, which is likely overridden in subclasses.
     */
    protected final void updateHashCode() {
//        this.hashcode = 
            computeHashCode();
    }
    /**
     * Return a new hash code value for this instance.  If subclasses provide additional
     * and non-transient fields, they should override this method using the following
     * template:
     * <pre>
     * protected int computeHashCode() {
     *      int result = super.computeHashCode();
     *      result = HashCodeUtil.hashCode(result, ... );
     *      result = HashCodeUtil.hashCode(result, ... );
     *      return result;
     * }
     * </pre>
     * Any specialized implementation must <i>not<i> rely upon the <code>hashCode</code>
     * method.
     * <p>
     * Note that this method does not and cannot actually update the hash code value.
     * Rather, this method is called by the <code>updateHashCode</code> method.
     * @return the new hash code for this instance.
     */
    protected int computeHashCode() {
        int result = 1;
        HashCodeUtil.hashCode(result,this.properties);
        HashCodeUtil.expHashCode(result,this.unmodifiablePropertyDefns);
        return result;
    }

    /**
     * <p>Compares this object to another. If the specified object is
     * an instance of the IODescriptor class, then this
     * method compares the instances; otherwise, it throws a
     * ClassCastException (as instances are comparable only to
     * instances of the same
     *  class).</p>
     * <p>Note:  this method <i>is</i> consistent with
     * <code>equals()</code>, meaning that
     * <code>(compare(x, y)==0) == (x.equals(y))</code>.</p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws IllegalArgumentException if the specified object reference is null
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {
        PropertiedObjectImpl that = (PropertiedObjectImpl)obj; // May throw ClassCastException
        if (obj == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0007, this.getClass().getName()));
        }

        // Compare the number of property values ...
        return this.compare(that);
    }

    protected int compare( PropertiedObjectImpl that ) {
        // Compare the number of property values ...
        int diff = this.properties.size() - that.properties.size();
        if ( diff != 0 ) {
            return diff;
        }

        // Iterate through the properties and compare values ...
        Map.Entry entry = null;
        Object thatValue = null;
        Object thisValue = null;
        Iterator iter = this.properties.entrySet().iterator();
        while ( iter.hasNext() ) {
            entry = (Map.Entry) iter.next();
            thisValue = entry.getValue();
            thatValue = that.properties.get(entry.getKey());
            if ( thisValue != null ) {
                if ( thatValue == null ) {
                    return 1;
                }
                if ( thisValue instanceof Comparable ) {
                    diff = ((Comparable)thisValue).compareTo(thatValue);
                } else {
                    diff = thisValue.toString().compareTo(thatValue.toString());
                }
                if ( diff != 0 ) {
                    return diff;
                }
            } else {
                if ( thatValue != null ) {
                    return -1;
                }
            }
        }
        return 0;
    }

    /**
     * Returns true if the specified object is semantically equal
     * to this instance.
     *   Note:  this method is consistent with
     * <code>compareTo()</code>.
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if (this.getClass().isInstance(obj)) {
            PropertiedObjectImpl that = (PropertiedObjectImpl)obj;
            return this.compare(that) == 0;
        }

        // Otherwise not comparable ...
        return false;
    }

    public Object clone() {
        return new PropertiedObjectImpl(this);
    }

    /**
     * Returns the hash code value for this object.
     * @return a hash code value for this object.
     */
//    public int hashCode() {
//        return this.properties.hashCode();
//    }

    /**
     * Obtain the list of PropertyDefinitions that apply to the specified object's type.
     * @return an unmodifiable list of the PropertyDefinition objects that
     * define the properties for the object; never null but possibly empty
     * @throws AssertionError if <code>obj</code> is null
     */
    public List getPropertyDefinitions() {
        return this.unmodifiablePropertyDefns;
    }

    /**
     * Set the list of PropertyDefinition instances that this object is to use.
     * @param propertyDefns the list of PropertyDefinition instances; the list
     * is assumed to be immutable, and may never be null
     */
    protected synchronized void setPropertyDefinitions( List propertyDefns ) {
    	ArgCheck.isNotNull(propertyDefns);
        this.unmodifiablePropertyDefns = propertyDefns;
        updateHashCode();
    }

    /**
     * Obtain from the specified PropertiedObject the property value
     * that corresponds to the specified PropertyDefinition.  The return type and cardinality
     * (including whether the value may be null) depend upon the PropertyDefinition.
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be returned; may not be null
     * @return the value for the property, which may be an empty collection if
     * the property is multi-valued, or may be null if the multiplicity
     * includes "0", or the NO_VALUE reference if the specified object
     * does not contain the specified PropertyDefinition
     * @throws AssertionError if <code>def</code> is null
     */
    public synchronized Object getValue(PropertyDefinition def) {
        Object result = this.properties.get(def);
        if ( result == null && def.hasDefaultValue() ) {
            result = def.getDefaultValue();
        }
        if ( result == null ) {
            return result;
        }

        // This is a check to verify that the types correspond to types and that we
        // are no longer using Collection to hold multi-valued properties
        if ( result instanceof Collection && def.getPropertyType() != PropertyType.LIST ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0021, def.getPropertyType().getDisplayName() ));
        }
        if ( result instanceof Set && def.getPropertyType() != PropertyType.SET ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0022, def.getPropertyType().getDisplayName() ));
        }

        return result;
    }

    /**
     * Return whether the specified value is considered valid.  The value is not
     * valid if the propertied object does not have the specified property definition,
     * or if it does but the value is inconsistent with the requirements of the
     * property definition.
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be validated; may not be null
     * @param value the proposed value for the property, which may be a collection if
     * the property is multi-valued, or may be null if the multiplicity
     * includes "0"
     * @return true if the value is considered valid, or false otherwise.
     * @throws AssertionError if either of <code>def</code> is null,
     * or if the property is multi-valued and the <code>value</code> is not an instance
     * of Object[].
     */
    public boolean isValidValue(PropertyDefinition def, Object value ) {
    	ArgCheck.isNotNull(def);

        // Check for a null value ...
        if ( value == null ) {
            return ( !def.isRequired() ); // only if minimum==0 is value allowed to be null
        }
        // This is a check to verify that the types correspond to types and that we
        // are no longer using Collection to hold multi-valued properties
        if ( value instanceof Collection && def.getPropertyType() != PropertyType.LIST ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0021, def.getPropertyType().getDisplayName() ));
        }
        if ( value instanceof Set && def.getPropertyType() != PropertyType.SET ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0022, def.getPropertyType().getDisplayName() ));
        }

        return def.getPropertyType().isValidValue(value);
    }

    /**
     * Set on the specified PropertiedObject the value defined by the specified PropertyDefinition.
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be changed; may not be null
     * @param value the new value for the property; the cardinality and type
     * must conform PropertyDefinition
     * @throws IllegalArgumentException if the value does not correspond
     * to the PropertyDefinition requirements.
     * @throws AssertionError if either of <code>def</code> is null
     * or if the property is multi-valued and the <code>value</code> is not an instance
     * of Object[].
     */
    public synchronized void setValue(PropertyDefinition def, Object value) {
        Assertion.contains(this.unmodifiablePropertyDefns,def,"The specified PropertyDefinition is not known or is not defined for this instance"); //$NON-NLS-1$

        // This is a check to verify that the types correspond to types and that we
        // are no longer using Collection to hold multi-valued properties
        if ( value instanceof Collection && def.getPropertyType() != PropertyType.LIST ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0021, def.getPropertyType().getDisplayName() ));
        }
        if ( value instanceof Set && def.getPropertyType() != PropertyType.SET ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0022, def.getPropertyType().getDisplayName() ));
        }

        this.properties.put(def,value);
        updateHashCode();
    }

    /**
     * Reads the properties and values from the input stream according to the Properties.load() method.
     * Only those properties that have corresponding property definitions in this object
     * are loaded.
     * <i>Note: this implementation is only able to load properties for PropertyDefinitions
     * that have a PropertyType that is represented as String.</i>
     * @param in the input stream, which is <i>not</i> closed by this method; may not be null
     * @throws AssertionError if the InputStream reference is null
     * @throws IOException if an error occurred when reading from the input stream
     */
    public synchronized void load(InputStream in) throws IOException {
    	ArgCheck.isNotNull(in);

        // Load the properties object ...
        Properties props = new Properties();
        props.load(in);    // may throw IOException

        // Set the values ...
        synchronized(this) {
            this.properties.clear();
            PropertyDefinition defn = null;
            Map propDefnsByName = new HashMap();
            Iterator iter = this.unmodifiablePropertyDefns.iterator();
            while ( iter.hasNext() ) {
                defn = (PropertyDefinition) iter.next();
                propDefnsByName.put(defn.getName(),defn);
            }
            Enumeration enumeration = props.propertyNames();
            while ( enumeration.hasMoreElements() ) {
                String name = enumeration.nextElement().toString();
                String value = props.getProperty(name);
                defn = (PropertyDefinition) propDefnsByName.get(name);
                if ( defn != null ) {
                    this.properties.put(defn,value);
                }
            }
        }
        updateHashCode();
    }

    /**
     * <p>Writes this property list (key and element pairs) in this Properties table
     * to the output stream in a format suitable for loading into a Properties
     * table using the load method.</p>
     * <p>If the header argument is not null, then an ASCII # character, the header
     * string, and a line separator are first written to the output stream.
     * Thus, the header can serve as an identifying comment.</p>
     * @param out an output stream; may not be null
     * @param header a description of the property list, which is <i>not</i> closed
     * by this method; may be null or zero-length
     * @throws AssertionError if the InputStream reference is null
     * @throws IOException if an error occurred when reading from the input stream
     */
    public void store(OutputStream out, String header) throws IOException {
    	ArgCheck.isNotNull(out);
        Properties props = this.getPropertiesObject();
        props.store(out,header);
    }

    protected synchronized Properties getPropertiesObject() {
        Properties result = new Properties();
        Map.Entry entry = null;
        Object value = null;
        PropertyDefinition defn = null;
        Iterator iter = this.properties.entrySet().iterator();
        while (iter.hasNext()) {
            entry = (Map.Entry) iter.next();
            defn = (PropertyDefinition) entry.getKey();
            value = entry.getValue();
            result.put(defn.getName(),value.toString());
        }
        return result;
    }

    public boolean containsAllRequiredValues() {
        Map.Entry entry = null;
        Object value = null;
        PropertyDefinition defn = null;
        Iterator iter = this.properties.entrySet().iterator();
        while (iter.hasNext()) {
            entry = (Map.Entry) iter.next();
            defn = (PropertyDefinition) entry.getKey();
            value = entry.getValue();
            if ( ! this.isValidValue(defn,value) ) {
                return false;
            }
        }
        return true;
    }
}

