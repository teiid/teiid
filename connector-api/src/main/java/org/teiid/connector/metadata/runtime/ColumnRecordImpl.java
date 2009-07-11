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

package org.teiid.connector.metadata.runtime;

/**
 * ColumnRecordImpl
 */
public class ColumnRecordImpl extends BaseColumn implements Comparable<ColumnRecordImpl> {

    private boolean selectable = true;
    private boolean updatable;
    private boolean autoIncrementable;
    private boolean caseSensitive;
    private boolean signed;
    private boolean currency;
    private boolean fixedLength;
    private boolean tranformationInputParameter;
    private int searchType;
    private Object minValue;
    private Object maxValue;
    private String nativeType;
    private String format;
    private int charOctetLength;
    private int distinctValues = -1;
    private int nullValues = -1;

    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    @Override
    public int compareTo(ColumnRecordImpl record) {
    	return this.getPosition() - record.getPosition();
    }
    
    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#getCharOctetLength()
     */
    public int getCharOctetLength() {
        return charOctetLength;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#getMaxValue()
     */
    public Object getMaxValue() {
        return maxValue;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#getMinValue()
     */
    public Object getMinValue() {
        return minValue;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#getSearchTye()
     */
    public int getSearchType() {
        return searchType;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#getFormat()
     */
    public String getFormat() {
        return format;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#isAutoIncrementable()
     */
    public boolean isAutoIncrementable() {
        return autoIncrementable;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#isCaseSensitive()
     */
    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#isCurrency()
     */
    public boolean isCurrency() {
        return currency;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#isFixedLength()
     */
    public boolean isFixedLength() {
        return fixedLength;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#isTranformationInputParameter()
     * @since 4.2
     */
    public boolean isTranformationInputParameter() {
        return tranformationInputParameter;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#isSelectable()
     */
    public boolean isSelectable() {
        return selectable;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#isSigned()
     */
    public boolean isSigned() {
        return signed;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#isUpdatable()
     */
    public boolean isUpdatable() {
        return updatable;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#getNativeType()
     * @since 4.2
     */
    public String getNativeType() {
        return nativeType;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#getDistinctValues()
     * @since 4.3
     */
    public int getDistinctValues() {
        return this.distinctValues;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnRecord#getNullValues()
     * @since 4.3
     */
    public int getNullValues() {
        return this.nullValues;
    }

    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================

    /**
     * @param b
     */
    public void setAutoIncrementable(boolean b) {
        autoIncrementable = b;
    }

    /**
     * @param b
     */
    public void setCaseSensitive(boolean b) {
        caseSensitive = b;
    }

    /**
     * @param i
     */
    public void setCharOctetLength(int i) {
        charOctetLength = i;
    }

    /**
     * @param b
     */
    public void setCurrency(boolean b) {
        currency = b;
    }

    /**
     * @param b
     */
    public void setFixedLength(boolean b) {
        fixedLength = b;
    }

    /**
     * @param object
     */
    public void setMaxValue(Object object) {
        maxValue = object;
    }

    /**
     * @param object
     */
    public void setMinValue(Object object) {
        minValue = object;
    }

    /**
     * @param s
     */
    public void setSearchType(int s) {
        searchType = s;
    }

    /**
     * @param b
     */
    public void setSelectable(boolean b) {
        selectable = b;
    }

    /**
     * @param b
     */
    public void setSigned(boolean b) {
        signed = b;
    }

    /**
     * @param b
     */
    public void setUpdatable(boolean b) {
        updatable = b;
    }

    /**
     * @param string
     */
    public void setFormat(String string) {
        format = string;
    }

    /**
     * @param distinctValues The distinctValues to set.
     * @since 4.3
     */
    public void setDistinctValues(int distinctValues) {
        this.distinctValues = distinctValues;
    }

    /**
     * @param nullValues The nullValues to set.
     * @since 4.3
     */
    public void setNullValues(int nullValues) {
        this.nullValues = nullValues;
    }

    /**
     * @param nativeType The nativeType to set.
     * @since 4.2
     */
    public void setNativeType(String nativeType) {
        this.nativeType = nativeType;
    }

    /**
     * @param b
     */
    public void setTransformationInputParameter(boolean b) {
        this.tranformationInputParameter = b;
    }
    
    public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append(getClass().getSimpleName());
        sb.append(" name="); //$NON-NLS-1$
        sb.append(getName());
        sb.append(", nameInSource="); //$NON-NLS-1$
        sb.append(getNameInSource());
        sb.append(", uuid="); //$NON-NLS-1$
        sb.append(getUUID());
        sb.append(", pathInModel="); //$NON-NLS-1$
        sb.append(getPath());
        return sb.toString();
    }

}