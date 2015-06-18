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

package org.teiid.metadata;

import org.teiid.core.types.DataTypeManager;

/**
 * ColumnRecordImpl
 */
public class Column extends BaseColumn implements Comparable<Column> {

	private static final long serialVersionUID = -1310120788764453726L;

	public enum SearchType {
		Unsearchable,
		Like_Only {
			@Override
			public String toString() {
				return "Like Only"; //$NON-NLS-1$ 
			}
		},
		All_Except_Like {
			@Override
			public String toString() {
				return "All Except Like"; //$NON-NLS-1$
			}
		},
		Searchable,
		Equality_Only {
			@Override
			public String toString() {
				return "Equality Only"; //$NON-NLS-1$
			}
		}
	}
	
    private boolean selectable = true;
    private boolean updatable;
    private boolean autoIncremented;
    private boolean caseSensitive;
    private boolean signed;
    private boolean currency;
    private boolean fixedLength;
    private SearchType searchType;
    private volatile String minimumValue;
    private volatile String maximumValue;
    //TODO: nativeType is now on the base class, but left here for serialization compatibility
    private String nativeType;
    private String format;
    private int charOctetLength;
    private volatile int distinctValues = -1;
    private volatile int nullValues = -1;
    private ColumnSet<?> parent;
    
    @Override
    public void setDatatype(Datatype datatype, boolean copyAttributes, int arrayDimensions) {
    	super.setDatatype(datatype, copyAttributes, arrayDimensions);
    	if (datatype != null && copyAttributes) {
    		//if (DefaultDataTypes.STRING.equals(datatype.getRuntimeTypeName())) { 
    		    //TODO - this is not quite valid since we are dealing with length representing chars in UTF-16, then there should be twice the bytes
    			//this.charOctetLength = datatype.getLength();
    		//}
    		this.signed = datatype.isSigned();
    		this.autoIncremented = datatype.isAutoIncrement();
    		this.caseSensitive = datatype.isCaseSensitive();
    		this.signed = datatype.isSigned();
    	}
    }
    
    public void setParent(ColumnSet<?> parent) {
		this.parent = parent;
	}
    
    @Override
    public ColumnSet<?> getParent() {
    	return parent;
    }

    @Override
    public int compareTo(Column record) {
    	return this.getPosition() - record.getPosition();
    }
    
    public int getCharOctetLength() {
        return charOctetLength;
    }

    public String getMaximumValue() {
        return maximumValue;
    }

    public String getMinimumValue() {
        return minimumValue;
    }

    public SearchType getSearchType() {
    	if (searchType == null) {
    		return this.getDatatype().getSearchType();
    	}
        return searchType;
    }
    
    public boolean isSearchTypeSet() {
    	return searchType != null;
    }

    public String getFormat() {
        return format;
    }

    public boolean isAutoIncremented() {
        return autoIncremented;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public boolean isCurrency() {
        return currency;
    }

    public boolean isFixedLength() {
        return fixedLength;
    }

    public boolean isSelectable() {
        return selectable;
    }

    public boolean isSigned() {
        return signed;
    }

    public boolean isUpdatable() {
        return updatable;
    }

    public String getNativeType() {
        return nativeType;
    }

    public int getNullValues() {
    	if (nullValues >= -1) {
    		return nullValues;
    	}
    	return Integer.MAX_VALUE;
    }
    
    public float getNullValuesAsFloat() {
    	return Table.asFloat(nullValues);
    }
    
    public int getDistinctValues() {
    	if (distinctValues >= -1) {
    		return distinctValues;
    	}
    	return Integer.MAX_VALUE;
    }
    
    public float getDistinctValuesAsFloat() {
    	return Table.asFloat(distinctValues);
    }

    /**
     * @param b
     */
    public void setAutoIncremented(boolean b) {
    	autoIncremented = b;
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
    public void setMaximumValue(String object) {
        maximumValue = DataTypeManager.getCanonicalString(object);
    }

    /**
     * @param object
     */
    public void setMinimumValue(String object) {
        minimumValue = DataTypeManager.getCanonicalString(object);
    }

    /**
     * @param s
     */
    public void setSearchType(SearchType s) {
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
        format = DataTypeManager.getCanonicalString(string);
    }

    /**
     * @param distinctValues The distinctValues to set.
     * @since 4.3
     */
    public void setDistinctValues(int distinctValues) {
        this.distinctValues = Table.asInt(distinctValues);
    }
    
    public void setDistinctValues(long distinctValues) {
    	this.distinctValues = Table.asInt(distinctValues);
    }

    /**
     * @param nullValues The nullValues to set.
     * @since 4.3
     */
    public void setNullValues(int nullValues) {
        this.nullValues = Table.asInt(nullValues);
    }
    
    public void setNullValues(long nullValues) {
    	this.nullValues = Table.asInt(nullValues);
    }

    /**
     * @param nativeType The nativeType to set.
     * @since 4.2
     */
    public void setNativeType(String nativeType) {
        this.nativeType = DataTypeManager.getCanonicalString(nativeType);
    }
    
    public void setColumnStats(ColumnStats stats) {
    	if (stats.getDistinctValues() != null) {
			setDistinctValues(stats.getDistinctValues().longValue());
		}
		if (stats.getNullValues() != null) {
			setNullValues(stats.getNullValues().longValue());
		}
		if (stats.getMaximumValue() != null) {
			setMaximumValue(stats.getMaximumValue());
		}
		if (stats.getMinimumValue() != null) {
			setMinimumValue(stats.getMinimumValue());
		}
    }

}