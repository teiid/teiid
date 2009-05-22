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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public final class PropertyType implements Comparable, Serializable {
    private static final long serialVersionUID = -740884659902823711L;
    
	static final String STRING_NAME             = "String"; //$NON-NLS-1$
	static final String INTEGER_NAME            = "Integer"; //$NON-NLS-1$
	static final String LONG_NAME               = "Long"; //$NON-NLS-1$
	static final String FLOAT_NAME              = "Float"; //$NON-NLS-1$
	static final String DOUBLE_NAME             = "Double"; //$NON-NLS-1$
	static final String BYTE_NAME               = "Byte"; //$NON-NLS-1$
	static final String SHORT_NAME              = "Short"; //$NON-NLS-1$
	static final String BOOLEAN_NAME            = "Boolean"; //$NON-NLS-1$
	static final String TIME_NAME               = "Time"; //$NON-NLS-1$
	static final String DATE_NAME               = "Date"; //$NON-NLS-1$
	static final String TIMESTAMP_NAME          = "Timestamp"; //$NON-NLS-1$
	static final String LIST_NAME               = "List"; //$NON-NLS-1$
	static final String SET_NAME                = "Set"; //$NON-NLS-1$
    static final String URL_NAME                = "URL"; //$NON-NLS-1$
    static final String URI_NAME                = "URI"; //$NON-NLS-1$
	static final String HOSTNAME_NAME           = "Hostname"; //$NON-NLS-1$
	static final String FILE_NAME               = "File"; //$NON-NLS-1$
	static final String OBJECT_ID_NAME          = "ObjectID"; //$NON-NLS-1$
	static final String PROPERTIED_OBJECT_NAME  = "PropertiedObject"; //$NON-NLS-1$
	static final String MULTIPLICITY_NAME       = "Multiplicity"; //$NON-NLS-1$
	static final String PASSWORD_NAME           = "Password"; //$NON-NLS-1$
    static final String DESCRIPTOR_NAME	        = "ResourceDescriptor"; //$NON-NLS-1$
    static final String OBJECT_REFERENCE_NAME   = "ObjectReference"; //$NON-NLS-1$
    static final String DATA_TYPE_NAME          = "DataType"; //$NON-NLS-1$
    static final String UNBOUNDED_INTEGER_NAME  = "UnboundedInteger"; //$NON-NLS-1$
    static final String REG_EXPRESSION_NAME     = "RegularExpression"; //$NON-NLS-1$

    static final Class PASSWORD_CLASS = (new byte[1]).getClass();

	public static final PropertyType STRING             = new PropertyType(101, STRING_NAME,       String.class.getName() );
	public static final PropertyType INTEGER            = new PropertyType(102, INTEGER_NAME,      Integer.class.getName() );
	public static final PropertyType LONG               = new PropertyType(103, LONG_NAME,         Long.class.getName() );
	public static final PropertyType FLOAT              = new PropertyType(104, FLOAT_NAME,        Float.class.getName() );
	public static final PropertyType DOUBLE             = new PropertyType(105, DOUBLE_NAME,       Double.class.getName() );
	public static final PropertyType BYTE               = new PropertyType(106, BYTE_NAME,         Byte.class.getName() );
	public static final PropertyType SHORT              = new PropertyType(107, SHORT_NAME,        Short.class.getName() );
	public static final PropertyType BOOLEAN            = new PropertyType(108, BOOLEAN_NAME,      Boolean.class.getName() );
	public static final PropertyType TIME               = new PropertyType(109, TIME_NAME,         java.sql.Time.class.getName() );
	public static final PropertyType DATE               = new PropertyType(110, DATE_NAME,         java.sql.Date.class.getName() );
	public static final PropertyType TIMESTAMP          = new PropertyType(111, TIMESTAMP_NAME,    java.sql.Timestamp.class.getName() );
	public static final PropertyType LIST               = new PropertyType(112, LIST_NAME,         java.util.List.class.getName() );
	public static final PropertyType SET                = new PropertyType(113, SET_NAME,          java.util.Set.class.getName() );
    public static final PropertyType URL                = new PropertyType(114, URL_NAME,          java.net.URL.class.getName() );
	public static final PropertyType HOSTNAME           = new PropertyType(115, HOSTNAME_NAME,     java.net.InetAddress.class.getName() );
	public static final PropertyType FILE               = new PropertyType(116, FILE_NAME,         com.metamatrix.common.tree.directory.DirectoryEntry.class.getName() );
	public static final PropertyType OBJECT_ID          = new PropertyType(117, OBJECT_ID_NAME,    com.metamatrix.core.id.ObjectID.class.getName() );
	public static final PropertyType PASSWORD           = new PropertyType(119, PASSWORD_NAME,     PASSWORD_CLASS.getName() );
	public static final PropertyType PROPERTIED_OBJECT  = new PropertyType(120, PROPERTIED_OBJECT_NAME,    com.metamatrix.common.object.PropertiedObject.class.getName() );
    public static final PropertyType DESCRIPTOR         = new PropertyType(121, DESCRIPTOR_NAME,   com.metamatrix.common.object.PropertiedObject.class.getName() );
    public static final PropertyType DATA_TYPE          = new PropertyType(123, DATA_TYPE_NAME,   java.lang.Object.class.getName() );
    public static final PropertyType UNBOUNDED_INTEGER  = new PropertyType(124, UNBOUNDED_INTEGER_NAME,   String.class.getName() );
    public static final PropertyType REG_EXPRESSION     = new PropertyType(125, REG_EXPRESSION_NAME,   String.class.getName() );
    public static final PropertyType URI                = new PropertyType(126, URI_NAME,          String.class.getName() );

    public static final String UNBOUNDED_INTEGER_KEYWORD = "unbounded"; //$NON-NLS-1$

    private static Map BY_VALUE = new HashMap();
    private static Map BY_NAME = new HashMap();
    private static Map BY_CLASSNAME = new HashMap();

    static {
        add( STRING );
        add( INTEGER );
        add( LONG );
        add( FLOAT );
        add( DOUBLE );
        add( BYTE );
        add( SHORT );
        add( BOOLEAN );
        add( TIME );
        add( DATE );
        add( TIMESTAMP );
        add( LIST );
        add( SET );
        add( URL );
        add( URI );
        add( HOSTNAME );
        add( FILE );
        add( OBJECT_ID );
        add( PASSWORD );
        add( PROPERTIED_OBJECT );
        add( DESCRIPTOR );
        add( DATA_TYPE );
        add( UNBOUNDED_INTEGER );
        add( REG_EXPRESSION );
    }

    private static void add( PropertyType propType ) {
        BY_VALUE.put( new Integer(propType.hashCode()),propType );
        BY_NAME.put( propType.getDisplayName(),propType );
        BY_CLASSNAME.put( propType.getClassName(),propType );
    }

    public static PropertyType getInstance(int value) {
        return (PropertyType) BY_VALUE.get( new Integer(value) );
    }

    public static PropertyType getInstance(String displayName) {
        return (PropertyType) BY_NAME.get(displayName);
    }

    public static PropertyType getInstanceByClassName(String className) {
        return (PropertyType) BY_CLASSNAME.get(className);
    }

	private int value;
	private String displayName;
	private String className;
    private PropertyTypeValidator validator;

	private PropertyType( int value, String displayName, String className ) {
		this.value = value;
		this.displayName = displayName;
		this.className = className;
        this.validator = StandardPropertyTypeValidator.lookup(displayName);
	}

    /**
     * Return the display name for this type.
     * @return the display name
     */
    public String getDisplayName() {
        return this.displayName;
    }

    /**
     * Return the name of the Java class that best represents this type.
     * @return the Java class that best represents this type.
     */
    public String getClassName() {
        return this.className;
    }

    /**
     * Compares this object to another. If the specified object is
     * an instance of the MetaMatrixSessionID class, then this
     * method compares the contents; otherwise, it throws a
     * ClassCastException (as instances are comparable only to
     * instances of the same
     *  class).
     * <p>
     * Note:  this method <i>is</i> consistent with
     * <code>equals()</code>, meaning
     *  that
     * <code>(compare(x, y)==0) == (x.equals(y))</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws IllegalArgumentException if the specified object reference is null
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {
        PropertyType that = (PropertyType)obj; // May throw ClassCastException
        if ( obj == null ) {
            throw new IllegalArgumentException("Attempt to compare null"); //$NON-NLS-1$
        }

        return (this.value - that.value);
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
        //if ( this.getClass().isInstance(obj) ) {
        if (obj instanceof PropertyType) {
            PropertyType that = (PropertyType)obj;
        	return this.value == that.value;
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Returns the hash code value for this object.
     * @return a hash code value for this object.
     */
    public final int hashCode() {
        return this.value;
    }

    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public final String toString() {
        return Integer.toString( this.value );
    }

    public boolean isValidValue(Object value ) {
        if ( this.validator != null ) {
            return this.validator.isValidValue(value);
        }
        return true;
    }


}

