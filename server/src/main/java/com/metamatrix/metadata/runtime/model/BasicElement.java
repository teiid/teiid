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

package com.metamatrix.metadata.runtime.model;

import org.teiid.connector.metadata.runtime.MetadataConstants;

import com.metamatrix.metadata.runtime.api.DataType;
import com.metamatrix.metadata.runtime.api.Element;
import com.metamatrix.metadata.runtime.api.ElementID;

public class BasicElement extends BasicMetadataObject implements Element {
        private String description;
        private String alias;
        private String label;
        private DataType dataType;
        private int scale;
        private int length;
        private int prcsnLength = MetadataConstants.NOT_DEFINED_INT;
        private int radix = MetadataConstants.NOT_DEFINED_INT;
        private int charOctetLength = MetadataConstants.NOT_DEFINED_INT;
        private boolean isPhysical;
        private boolean isLengthFixed;
        private short nullType = 1;
        private boolean sup_SELECT;
        private boolean sup_SET;
        private boolean sup_SUB;
        private boolean sup_UPDATE;
        private boolean isCaseSensitive;
        private boolean isSigned;
        private boolean isCurrency;
        private boolean isAutoIncrement;
        private String minRange;
        private String maxRange;
        private short searchType = 1;
        private String format;
        private String defaultValue;
//        private long dataTypeUID;
        private int position;
	

/**
 * Call constructor to instantiate a runtime object by passing the RuntimeID that identifies the entity and the VIrtualDatabaseID that identifes the Virtual Database the object will be contained.
 */
    public BasicElement(ElementID element, BasicVirtualDatabaseID virtualDBID) {
        super(element, virtualDBID);
    }

    public String getDescription() {
        return this.description;
    }

    /**
    * Override the super method so that when the name
    * is returned, it is the name and not the full path for
    * an element
    */
    public String getNameInSource() {
        String alias = getAlias();
        if(alias != null)
	        return alias;
        return getName();
    }

    public String getAlias(){
        return alias;
    }
    public void setAlias(String alias){
        this.alias = alias;
    }
    public boolean isPhysical() {
        return this.isPhysical;
    }
    public String getLabel(){
	      return this.label;
    }
    public DataType getDataType(){
    	if(dataType.getRuntimeType() != null){
	      return dataType.getRuntimeType();
    	}
    	return dataType;
    }
    public DataType getActualDataType(){
    	return dataType;
    }
    public int getScale(){
	      return scale;
    }
    public int getLength(){
	      return this.length;
    }
    public boolean isLengthFixed() {
	      return this.isLengthFixed;
    }
    public short getNullType(){
	    return this.nullType;
    }
    public boolean supportsSelect() {
      return sup_SELECT;
    }
    public boolean supportsSet() {
      return sup_SET;
    }
    public boolean supportsSubscription() {
      return sup_SUB;
    }
    public boolean supportsUpdate() {
      return sup_UPDATE;
    }
    public boolean isCaseSensitive() {
      return isCaseSensitive;
    }
    public boolean isSigned() {
      return isSigned;
    }
    public boolean isCurrency() {
      return isCurrency;
    }
    public boolean isAutoIncrement() {
      return isAutoIncrement;
    }
    public String getMinimumRange() {
      return minRange;
    }

    public String getMaximumRange() {
      return maxRange;
    }
    public short getSearchType() {
      return searchType;
    }
    public String getFormat() {
      return format;
    }
    public Object getDefaultValue() {
      return defaultValue;
    }
    public int getPrecisionLength(){
        return prcsnLength;
    }
    public int getRadix(){
        return radix;
    }
    public int getCharOctetLength(){
        return charOctetLength;
    }
    public int getPosition(){
	    return this.position;
    }
    public void setDescription(String description) {
        this.description = description;
    }
    public void setPhysical(boolean isPhysical) {
        this.isPhysical = isPhysical;
    }
    public void setLabel(String label){
	      this.label = label;
    }
    public void setDataType(DataType type) {
        this.dataType = type;
    }
    public void setScale(int scale) {
        this.scale = scale;
    }
    public void setLength(int length) {
        this.length = length;
    }
    public void setIsLengthFixed(boolean isLengthFixed) {
        this.isLengthFixed = isLengthFixed;
    }
    public void setNullType(short nullType) {
        this.nullType = nullType;
    }
    public void setSupportsSelect(boolean sup_SELECT) {
        this.sup_SELECT = sup_SELECT;
    }
    public void setSupportsSet(boolean sup_SET) {
        this.sup_SET = sup_SET;
    }
    public void setSupportsSubscription(boolean sup_SUB) {
        this.sup_SUB = sup_SUB;
    }
    public void setSupportsUpdate(boolean sup_UPDATE) {
        this.sup_UPDATE = sup_UPDATE;
    }
    public void setIsCaseSensitive(boolean isCaseSensitive) {
        this.isCaseSensitive = isCaseSensitive;
    }
    public void setIsSigned(boolean isSigned) {
        this.isSigned = isSigned;
    }
    public void setIsCurrency(boolean isCurrency) {
        this.isCurrency = isCurrency;
    }
    public void setIsAutoIncrement(boolean isAutoIncremented) {
        this.isAutoIncrement = isAutoIncremented;
    }
    public void setMinimumRange(String minRange) {
        this.minRange = minRange;
    }
    public void setMaximumRange(String maxRange) {
        this.maxRange = maxRange;
    }
    public void setSearchType(short searchType) {
        this.searchType = searchType;
    }
    public void setFormat(String format) {
        this.format = format;
    }
    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }
    
    public void setPrecisionLength(int prcsnLength){
        this.prcsnLength = prcsnLength;
    }
    public void setRadix(int radix){
        this.radix = radix;
    }
    public void setCharOctetLength(int charOctetLength){
        this.charOctetLength = charOctetLength;
    }
    public void setPosition(int position){
	    this.position = position;
    }
}

