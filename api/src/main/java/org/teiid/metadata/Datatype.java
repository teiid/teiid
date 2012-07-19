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

import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column.SearchType;

public class Datatype extends AbstractMetadataRecord {
	
	private static final long serialVersionUID = -7839335802224393230L;
	
	public enum Type {
		Basic,
		UserDefined,
		ResultSet
	}
	
	public enum Variety {
		Atomic,
		List,
		Union,
		Complex
	}

    private static final String DEFAULT_JAVA_CLASS_NAME = "java.lang.Object";  //$NON-NLS-1$

    private int length;
    private int precisionLength;
    private int scale;
    private int radix;
    private boolean isSigned;
    private boolean isAutoIncrement;
    private boolean isCaseSensitive;
    private Type type;
    private SearchType searchType;
    private NullType nullType = NullType.Nullable;
    private String javaClassName = DEFAULT_JAVA_CLASS_NAME;
    private String runtimeTypeName;
    private String basetypeName;
    private Variety varietyType;

    /**
     * Get the length of the type.
     * 
     * For string (binary or character) types, it is the number of characters.
     * For all other types it is the byte storage size.
     * @return
     */
    public int getLength() {
        return this.length;
    }
    
    /**
     * @deprecated
     * @see #getPrecision()
     */
    public int getPrecisionLength() {
        return this.precisionLength;
    }

    public int getPrecision() {
        return this.precisionLength;
    }

    public int getScale() {
        return this.scale;
    }

    public int getRadix() {
        return this.radix;
    }

    public boolean isSigned() {
        return this.isSigned;
    }

    public boolean isAutoIncrement() {
        return this.isAutoIncrement;
    }

    public boolean isCaseSensitive() {
        return this.isCaseSensitive;
    }

    public Type getType() {
        return this.type;
    }

    public boolean isBuiltin() {
        return getType() == Type.Basic;
    }


    public SearchType getSearchType() {
        return this.searchType;
    }

    public NullType getNullType() {
    	if (this.nullType == null) {
    		return NullType.Unknown;
    	}
        return this.nullType;
    }

    public String getJavaClassName() {
        return this.javaClassName;
    }

    public String getRuntimeTypeName() {
        return this.runtimeTypeName;
    }

    public String getBasetypeName() {
        return this.basetypeName;
    }
    
    public void setBasetypeName(String name) {
    	this.basetypeName = name;	
    }

    public Variety getVarietyType() {
        return this.varietyType;
    }

    /**
     * @param b
     */
    public void setAutoIncrement(boolean b) {
        isAutoIncrement = b;
    }

    /**
     * @param b
     */
    public void setCaseSensitive(boolean b) {
        isCaseSensitive = b;
    }

    /**
     * @param b
     */
    public void setSigned(boolean b) {
        isSigned = b;
    }

    /**
     * @param string
     */
    public void setJavaClassName(String string) {
        javaClassName = string;
    }

    /**
     * @param i
     */
    public void setLength(int i) {
        length = i;
    }

    /**
     * @param s
     */
    public void setNullType(NullType s) {
        nullType = s;
    }

    /**
     * @param i
     */
    public void setPrecision(int i) {
        precisionLength = i;
    }
    
    /**
     * @deprecated
     * @see #setPrecision(int)
     */
    public void setPrecisionLength(int i) {
        precisionLength = i;
    }

    /**
     * @param i
     */
    public void setRadix(int i) {
        radix = i;
    }

    /**
     * @param string
     */
    public void setRuntimeTypeName(String string) {
        runtimeTypeName = string;
    }

    /**
     * @param i
     */
    public void setScale(int i) {
        scale = i;
    }

    /**
     * @param s
     */
    public void setSearchType(SearchType s) {
        searchType = s;
    }

    /**
     * @param s
     */
    public void setType(Type s) {
        type = s;
    }

    /**
     * @param s
     */
    public void setVarietyType(Variety s) {
        varietyType = s;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append(getClass().getSimpleName());
        sb.append(" name="); //$NON-NLS-1$
        sb.append(getName());
        sb.append(", basetype name="); //$NON-NLS-1$
        sb.append(getBasetypeName());
        sb.append(", runtimeType="); //$NON-NLS-1$
        sb.append(getRuntimeTypeName());
        sb.append(", javaClassName="); //$NON-NLS-1$
        sb.append(getJavaClassName());
        sb.append(", ObjectID="); //$NON-NLS-1$
        sb.append(getUUID());
        return sb.toString();
    }

}