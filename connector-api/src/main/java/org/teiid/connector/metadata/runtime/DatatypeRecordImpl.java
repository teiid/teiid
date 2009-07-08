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

import java.util.List;


/**
 * ColumnRecordImpl
 */
public class DatatypeRecordImpl extends AbstractMetadataRecord {

    /**
	 * Constants for names of accessor methods that map to fields stored  on the DatatypeRecords.
	 * Note the names do not have "get" on them, this is also the nameInsource
	 * of the attributes on SystemPhysicalModel.
	 * @since 4.3
	 */
	public static interface MetadataFieldNames {
	
	    String DATA_TYPE_UUID = "DatatypeID";  //$NON-NLS-1$
	    String BASE_TYPE_UUID = "BasetypeID";  //$NON-NLS-1$
	    String RUN_TYPE_NAME = "RuntimeTypeName";  //$NON-NLS-1$
	}

	/** Delimiter used to separate the URI string from the URI fragment */
    public static final String URI_REFERENCE_DELIMITER = "#"; //$NON-NLS-1$
	
    private static final String DEFAULT_JAVA_CLASS_NAME = "java.lang.Object";  //$NON-NLS-1$

    private int length;
    private int precisionLength;
    private int scale;
    private int radix;
    private boolean isSigned;
    private boolean isAutoIncrement;
    private boolean isCaseSensitive;
    private short type;
    private short searchType;
    private short nullType;
    private String javaClassName = DEFAULT_JAVA_CLASS_NAME;
    private String runtimeTypeName;
    private String datatypeID;
    private String basetypeID;
    private String primitiveTypeID;
    private short varietyType;
    private List varietyProps;

    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getName()
     */
    public String getName() {
        final String fullName = super.getFullName();
        int indx = fullName.lastIndexOf(URI_REFERENCE_DELIMITER);
        if (indx > -1) {
            return fullName.substring(indx+1);
        }
        indx = fullName.lastIndexOf(AbstractMetadataRecord.NAME_DELIM_CHAR);
        if (indx > -1) {
            return fullName.substring(indx+1);
        }
        return fullName;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getModelName()
     */
    public String getModelName() {
        final String fullName = super.getFullName();
        int indx = fullName.lastIndexOf(URI_REFERENCE_DELIMITER);
        if (indx > -1) {
            return fullName.substring(0, indx);
        }
        indx = fullName.lastIndexOf(AbstractMetadataRecord.NAME_DELIM_CHAR);
        if (indx > -1) {
            return fullName.substring(0, indx);
        }
        return fullName;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getLength()
     */
    public int getLength() {
        return this.length;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getPrecisionLength()
     */
    public int getPrecisionLength() {
        return this.precisionLength;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getScale()
     */
    public int getScale() {
        return this.scale;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getRadix()
     */
    public int getRadix() {
        return this.radix;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#isSigned()
     */
    public boolean isSigned() {
        return this.isSigned;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#isAutoIncrement()
     */
    public boolean isAutoIncrement() {
        return this.isAutoIncrement;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#isCaseSensitive()
     */
    public boolean isCaseSensitive() {
        return this.isCaseSensitive;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getType()
     */
    public short getType() {
        return this.type;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#isBuiltin()
     */
    public boolean isBuiltin() {
        if ( getType() == MetadataConstants.DATATYPE_TYPES.BASIC ) {
            return true;
        }
        return false;
    }


    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getSearchType()
     */
    public short getSearchType() {
        return this.searchType;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getNullType()
     */
    public short getNullType() {
        return this.nullType;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getJavaClassName()
     */
    public String getJavaClassName() {
        return this.javaClassName;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getRuntimeTypeName()
     */
    public String getRuntimeTypeName() {
        return this.runtimeTypeName;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getDatatypeID()
     */
    public String getDatatypeID() {
        return this.datatypeID;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getBasetypeID()
     */
    public String getBasetypeID() {
        return this.basetypeID;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getBasetypeName()
     */
    public String getBasetypeName() {
        if ( this.basetypeID != null ) {
            final int i = getBasetypeID().lastIndexOf(URI_REFERENCE_DELIMITER);
            if ( i != -1 && getBasetypeID().length() > (i+1)) {
                return getBasetypeID().substring(i+1);
            }
        }
        return null;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getPrimitiveTypeID()
     * @since 4.3
     */
    public String getPrimitiveTypeID() {
        return this.primitiveTypeID;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getVarietyType()
     */
    public short getVarietyType() {
        return this.varietyType;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.DatatypeRecord#getVarietyProps()
     */
    public List getVarietyProps() {
        return this.varietyProps;
    }

    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================

    /**
     * @param string
     */
    public void setBasetypeID(String string) {
        basetypeID = string;
    }

    /**
     * @param string
     */
    public void setPrimitiveTypeID(String string) {
        primitiveTypeID = string;
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
    public void setNullType(short s) {
        nullType = s;
    }

    /**
     * @param i
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
    public void setSearchType(short s) {
        searchType = s;
    }

    /**
     * @param s
     */
    public void setType(short s) {
        type = s;
    }

    /**
     * @param string
     */
    public void setDatatypeID(String string) {
        datatypeID = string;
    }

    /**
     * @param list
     */
    public void setVarietyProps(List list) {
        varietyProps = list;
    }

    /**
     * @param s
     */
    public void setVarietyType(short s) {
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
        sb.append(", datatypeID="); //$NON-NLS-1$
        sb.append(getDatatypeID());

        return sb.toString();
    }

}