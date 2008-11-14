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

package com.metamatrix.metadata.runtime.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.metamatrix.common.types.DataTypeManager;

import com.metamatrix.metadata.runtime.api.DataType;
import com.metamatrix.metadata.runtime.api.DataTypeElement;

public class BasicDataType extends BasicMetadataObject implements DataType {

    private String description;
    private DataType prefDataType;
    private String form;
    private String defaultJavaClassName = "java.lang.Object";
    private String javaClassName;
    private int length;
    private String minRange;
    private String maxRange;
    private String nativeType = "object";
    private int scale;
    private short type = 1;
    private boolean isAutoIncrement;
    private boolean isCaseSensitive;
    private boolean isLengthFixed;
    private short nullType = 1;
    private boolean isSigned;
    private boolean isStandard;
    private boolean supportsSelect;
    private boolean isPhysical;
    private int prcsnLength;
    private int radix;
    private boolean fixedPrcsnLength;
    private short searchType = 1;
    private String path;
    private List elements;
    private String baseType;
    private String runtimeTypeUUID;
    private DataType runtimeType;
//    private long pUID = MetadataConstants.NOT_DEFINED_LONG;
    private boolean javaClassNameIsSet = false;
    
/**
 * Call constructor to instantiate a runtime object by passing the RuntimeID that identifies the entity and the VIrtualDatabaseID that identifes the Virtual Database the object will be contained.
 */
    public BasicDataType(BasicDataTypeID dataTypeID, BasicVirtualDatabaseID virtualDBID) {
        super(dataTypeID, virtualDBID);
    }
/**
 * return the description.
 *  @return String
 */
    public  String getDescription() {
        return this.description;
    }
/**
 * if the data type is not a standard type, then this should return the MetaMatrix standard
 * data type that is considered the preferred internal type to which this data type corresponds.
 */
    public  DataType getPreferredDataType() {
        return this.prefDataType;

    }
/**
 * returns a boolean indicating if this is a <b>MetaMatrix</b> standard type.
 * @return boolean true if it is a standard type
 */
    public  boolean isStandard() {
        return isStandard;
    }
    
    public  String getForm() {
        return this.form;
    }
/**
 * returns the scale, which is the number of significant digits to the right of the decimal point.
 * The scale cannot exceed the length, and the scale defaults to 0 (meaning it is an integer number and the decimal point is dropped).
 *  @return int
 */
    public  int getScale() {
        return scale;
    }
/**
 * If the data type is numeric, the length is the total number of significant digits used to express the number.
 * If it is a string, character array, or bit array it represents the maximum length of the value.
 * For time and timestamp data types, the length is the number of positions that make up the fractional seconds.
 *  @return int
 */

    public  int getLength() {
        return length;
    }
/**
 * returns a boolean indicating if the length is fixed.
 * @return boolean
 */
    public  boolean isLengthFixed() {
        return isLengthFixed;

    }
/**
 * returns a short indicating if the element can be set to null.
 * @return short
 * @see com.metamatrix.metadata.runtime.api.MetadataConstants.NULL_TYPES
 */
    public  short getNullType() {
        return nullType;
    }
/**
 * returns a boolean indicating if the element can be selected
 * @return boolean
 */
    public  boolean supportsSelect() {
        return supportsSelect;
    }
/*
 * returns a boolean indicating if the element data is case sensitive.
 * This value shall be false if the data type is not a character, character array or string type.
 * @return boolean
 */
    public  boolean isCaseSensitive() {
        return isCaseSensitive;
    }
/**
 * returns a boolean indicating if the element data is signed.
 * @return boolean
 */
    public  boolean isSigned() {
        return isSigned;
    }
/**
 * returns a boolean indicating if the element is auto incremented by the database.  Therefore, this element value should not be provided on an insert statement.
 * @return boolean
 */
    public  boolean isAutoIncrement() {
        return isAutoIncrement;
    }
/**
 * returns the minimum value that the element can represent.
 * @return String
 */
    public  String getMinimumRange(){
        return minRange;
    }
/**
 * eturns the maximum value that the element can represent.
 * @return String
 */
    public  String getMaximumRange() {
        return maxRange;
    }
/**
 * return short indicating the type.
 * @return short
 *
 * @see com.metamatrix.metadata.runtime.api.MetadataConstants.DATATYPE_TYPES
 */
    public  short getType() {
        return type;
    }
/**
 * @return String is the name of the Java class that represents this datatype
 */
    public  String getJavaClassName() {
        if (javaClassName == null) {
            javaClassName = defaultJavaClassName;
        }
        return javaClassName;
    }
    public boolean isPhysical() {
        return this.isPhysical;
    }
/**
 * @return String is the native type this DataType will map to.
 */
    public  String getNativeType() {
        return nativeType;
    }
    public  int getPrecisionLength() {
        return prcsnLength;
    }
    public  int getRadix() {
        return radix;
    }
    public  short getSearchType() {
        return searchType;
    }
    public boolean getFixedPrecisionLength() {
        return fixedPrcsnLength;
    }
    public String getPath() {
	      return this.path;
    }
    public List getElements(){
        return (this.elements == null ? Collections.EMPTY_LIST : this.elements);
    }
    public void addElement(DataTypeElement dtElement){
        if(elements == null)
            elements = new ArrayList();
        elements.add(dtElement);
    }
    public void setDescription(String desc) {
        this.description = desc;
    }
    public void setForm(String newForm) {
        this.form = newForm;
    }
    public void setDefaultJavaClassName(String className) {
        this.defaultJavaClassName = className;
    }
    public void setJavaClassName(String className) {
        this.javaClassName = className;
        this.javaClassNameIsSet = true;
    }
    public void setLength(int length) {
        this.length = length;
    }
    public void setMaximumRange(String maxRange) {
        this.maxRange = maxRange;
    }
    public void setMinimumRange(String minRange) {
        this.minRange = minRange;
    }
    public void setNativeType(String nativeType) {
        this.nativeType = nativeType;
    }
    public void setPreferredDataType(DataType type) {
        this.prefDataType = type;
    }
    public void setScale(int scale) {
        this.scale = scale;
    }
    public void setType(short type) {
        this.type = type;
    }
    public void setIsAutoIncrement(boolean autoIncrement) {
        this.isAutoIncrement = autoIncrement;
    }
    public void setIsCaseSensitive(boolean caseSensitive) {
        this.isCaseSensitive = caseSensitive;
    }
    public void setIsLengthFixed(boolean lengthFixed) {
        this.isLengthFixed = lengthFixed;
    }
    public void setNullType(short nullable) {
        this.nullType = nullable;
    }
    public void setIsSigned(boolean signed) {
        this.isSigned = signed;
    }
    public void setIsStandard(boolean standard) {
        this.isStandard = standard;
    }
    public void setIsPhysical(boolean isPhysical) {
        this.isPhysical = isPhysical;
    }
    public void setPrecisionLength(int prcsnLength) {
        this.prcsnLength = prcsnLength;
    }
    public void setRadix(int radix) {
        this.radix = radix;
    }
    public void setSearchType(short searchType) {
        this.searchType = searchType;
    }
    public void setFixedPrecisionLength(boolean fixedPrcsnLength) {
        this.fixedPrcsnLength = fixedPrcsnLength;
    }
    public void setSupportsSelect(boolean supportSelect){
        this.supportsSelect = supportSelect;
    }
    public void setPath(String path){
        this.path = path;
    }
    public void setElements(List elements){
        this.elements = elements;
    }
    
    /**
     * Returns Runtime type that this datatype is mapped to.
     * If this datatype is Runtime type, return null.
     * @return DataType reference
     */
    public DataType getRuntimeType(){
    	return runtimeType;	
    }
    
    /**
	 * Returns true if this datatype is MetaMatrix standard Type, false otherwise. 
	 * @return boolean
	 */
    public boolean isRuntimeType(){	
    	return runtimeType == null;
    }
    
    public String getBaseTypeUUID(){
    	return baseType;
    }
    
    public void setBaseTypeUUID(String baseTypeUUID){
    	this.baseType = baseTypeUUID;
    } 
    
    public void setRuntimeType(DataType runtimeType){
    	this.runtimeType = runtimeType;
        if ( !javaClassNameIsSet && runtimeType != null) {
            String runtimeTypeName = runtimeType.getFullName();
            Class javaClass = DataTypeManager.getDataTypeClass(runtimeTypeName);
            if (javaClass != null) {
                setJavaClassName(javaClass.getName());
            }
        }	
    }
    
    public String getRTUUID(){
    	return runtimeTypeUUID;
    }
    
    public void setRTUUID(String runtimeTypeUUID){
    	this.runtimeTypeUUID = runtimeTypeUUID;
    }   
}

