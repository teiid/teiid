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

package com.metamatrix.common.jdbc.metadata;



public class Column extends JDBCObject {

    private int dataType;
    private String dataTypeName;
    private int size;
    private int decimalDigits;
    private int radix;
    private String remarks;
    private String defaultValue;
    private int charOctetLength;
    private int position;
    private Nullability nullability;
    private String classMapping;
    private String label;
    private ColumnType columnType;
    private boolean autoIncremented;


    public Column() {
        super();
        setColumnType( ColumnType.COLUMN_NOT_PSEUDO );
        setAutoIncremented(false);
    }

    public Column(String name) {
        super(name);
        setColumnType( ColumnType.COLUMN_NOT_PSEUDO );
        setAutoIncremented(false);
    }
    
    public Column(String name, int sqlType) {
        super(name);
        setDataType(sqlType);
        setColumnType( ColumnType.COLUMN_NOT_PSEUDO );
        setAutoIncremented(false);
    }

    /**
    * Returns the JDBC sql data type
    * @see java.sql.Types
	  * @return Returns an int
    */
    public int getDataType() {
        return dataType;
    }

    /**
     * Sets the sqlType
     * @see java.sql.Types
     * @param sqlType The sqlType to set
     */
    public void setDataType(int dataType) {
        this.dataType = dataType;
    }

    public String getDataTypeName() {
        return dataTypeName;
    }

    public void setDataTypeName(String dataTypeName) {
        this.dataTypeName = dataTypeName;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getDecimalDigits() {
        return decimalDigits;
    }

    public void setDecimalDigits(int decimalDigits) {
        this.decimalDigits = decimalDigits;
    }

    public int getRadix() {
        return radix;
    }

    public void setRadix(int radix) {
        this.radix = radix;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean hasDefaultValue(String defaultValue) {
        return this.defaultValue != null && this.defaultValue.length() != 0;
    }

    public int getCharOctetLength() {
        return charOctetLength;
    }

    public void setCharOctetLength(int charOctetLength) {
        this.charOctetLength = charOctetLength;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public Nullability getNullability() {
        return nullability;
    }

    public void setNullability(Nullability nullability) {
        this.nullability = nullability;
    }

    public void setClassMapping(String className){
        this.classMapping = className;
    }

    public String getClassMapping() {
        return this.classMapping;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }

    /**
     * Gets the columnType.
     * @return Returns a ColumnType
     */
    public ColumnType getColumnType() {
        return columnType;
    }

    /**
     * Sets the columnType.
     * @param columnType The columnType to set
     */
    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }

    /**
     * Gets the autoIncremented.
     * @return Returns a boolean
     */
    public boolean getAutoIncremented() {
        return autoIncremented;
    }

    /**
     * Sets the autoIncremented.
     * @param autoIncremented The autoIncremented to set
     */
    public void setAutoIncremented(boolean autoIncremented) {
        this.autoIncremented = autoIncremented;
    }

}
