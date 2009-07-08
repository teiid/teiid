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

package com.metamatrix.metadata.runtime.api;



/**
 * <p>The DataType api represents the type of data a particular Element or Procedure is.  
 * The MetaMatrix standard types are called Runtime Types. Simple Types are called User Defined Types.  
 * Additionally, a complex data type can be defined to support a structure. This data type, called ResultSet, 
 * which can be defined by specifying the elements and their order.</p> 
 */
public interface DataType extends MetadataObject {
	/**
	 * Return the description.
	 *  @return String
	 */
    String getDescription();
    
	/**
	 * Returns a boolean indicating if this is a <b>MetaMatrix</b> standard type.
	 * @return boolean true if it is a standard type 
	 */
    boolean isStandard();

	/**
	 * if the data type is not a standard type, then this should return the MetaMatrix standard 
	 * data type that is considered the preferred internal type to which this data type corresponds.
	 */
    DataType getPreferredDataType();

    String getForm();

	/**
	 * Returns the scale, which is the number of significant digits to the right of the decimal point.
	 * The scale cannot exceed the length, and the scale defaults to 0 (meaning it is an integer number and the decimal point is dropped).
	 *  @return int
	 */
    int getScale();
    
	/**
	 * If the data type is numeric, the length is the total number of significant digits used to express the number.
	 * If it is a string, character array, or bit array it represents the maximum length of the value.
	 * For time and timestamp data types, the length is the number of positions that make up the fractional seconds.
	 *  @return int
	 */  
    int getLength();
    
	/**
	 * Returns a boolean indicating if the length is fixed.
	 * @return boolean
	 */
    boolean isLengthFixed();
    
	/**
	 * Returns a short indicating if the element can be set to null.
	 * @return short
	 * @see com.metamatrix.metadata.runtime.api.MetadataConstants.NULL_TYPES 
	 */
    short getNullType();
    
	/**
	 * Returns a boolean indicating if the element can be selected
	 * @return boolean
	 */
    boolean supportsSelect();
    
	/**
	 * Returns a boolean indicating if the element data is case sensitive.
	 * This value shall be false if the data type is not a character, character array or string type.
	 * @return boolean
	 */	
    boolean isCaseSensitive();
    
	/**
	 * Returns a boolean indicating if the element data is signed.
	 * @return boolean
	 */	
    boolean isSigned();
    
	/**
	 * Returns a boolean indicating if the element is auto incremented by the database.  Therefore, this element value should not be provided on an insert statement.
	 * @return boolean
	 */	
    boolean isAutoIncrement();
    
	/**
	 * Returns the minimum value that the element can represent.
	 * @return String
	 */
	
    String getMinimumRange();
	/**
	 * eturns the maximum value that the element can represent.
	 * @return String
	 */	
    String getMaximumRange();
    
	/**
	 * Return short indicating the type.
	 * @return short
	 *
	 * @see com.metamatrix.metadata.runtime.api.MetadataConstants.DATATYPE_TYPES
	 */	
    short getType();
    
	/**
	 * @return String is the name of the Java class that represents this datatype 
	 */	
    String getJavaClassName();
    
	/**
	 * @return String is the native type this DataType will map to. 
	 */
	
    String getNativeType();
	/**
	 * Returns a boolean indicating if this a physical data type.
	 * @return boolean
	 */	
    boolean isPhysical();
    
	/**
	 * Returns an int indicating the precision length. Default to MetadataConstants.NOT_DEFINED_INT if not set.
	 * @return int
	 */	
    int getPrecisionLength();
    
	/**
	 * Returns an int indicating the radix. Default to MetadataConstants.NOT_DEFINED_INT if not set.
	 * @return int
	 */	
    int getRadix();
    
	/**
	 * Returns a boolean indicating whether precision length is fixed. Default to false;
	 * @return boolean
	 */	
    boolean getFixedPrecisionLength();
    
	/**
	 * Returns a short indicating the serach typr.
	 * @return short
	 */	
    short getSearchType();
    
	/**
	 * Returns a string indicating the path.
	 * @return String
	 */	
    String getPath();
    
	/**
	 * Returns a list of type <code>DataTypeElement</code>.
	 * @return List
	 */
    java.util.List getElements();
    
    /**
     * Returns Runtime type that this datatype is mapped to.
     * If this datatype is Runtime type, return null.
     * @return DataType reference
     */
    DataType getRuntimeType();
    
    /**
	 * Returns true if this datatype is Runtime Type, false otherwise. 
	 * @return boolean
	 */
    public boolean isRuntimeType();
    
}

