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
 * ProcedureParameterRecordImpl
 */
public class ProcedureParameterRecordImpl extends AbstractMetadataRecord {

    private String datatypeUUID;
    private String runtimeType;
    private Object defaultValue;
    private int type;
    private int length;
    private int scale;
    private int radix;
    private int precision;
    private int nullType;
    private int position;
    private boolean optional;
    private DatatypeRecordImpl datatype;

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.ProcedureParameterRecord#getDefaultValue()
     */
    public Object getDefaultValue() {
        return defaultValue;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.ProcedureParameterRecord#getType()
     */
    public short getType() {
        return (short)type;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.ProcedureParameterRecord#getDatatypeUUID()
     */
    public String getDatatypeUUID() {
        return datatypeUUID;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.ProcedureParameterRecord#getRuntimeType()
     */
    public String getRuntimeType() {
        return runtimeType;
    }

    /**
     * @return
     */
    public int getLength() {
        return length;
    }

    /**
     * @return
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * @return
     */
    public int getScale() {
        return scale;
    }

    /**
     * @return
     */
    public int getRadix() {
        return radix;
    }

    /**
     * @return
     */
    public int getPosition() {
        return position;
    }

    /**
     * @return
     */
    public int getNullType() {
        return nullType;
    }

	/*
	 * @see com.metamatrix.modeler.core.metadata.runtime.ProcedureParameterRecord#isOptional()
	 */
	public boolean isOptional() {
		return optional;
	}

	/**
	 * @param i
	 */
	public void setLength(int i) {
		length = i;
	}

	/**
	 * @param i
	 */
	public void setPrecision(int i) {
		precision = i;
	}

	/**
	 * @param i
	 */
	public void setScale(int i) {
		scale = i;
	}

	/**
	 * @param i
	 */
	public void setRadix(int i) {
		radix = i;
	}

    /**
     * @param i
     */
    public void setNullType(int i) {
        nullType = i;
    }

	/**
	 * @param i
	 */
	public void setPosition(int i) {
		position = i;
	}

	/**
	 * @param string
	 */
	public void setRuntimeType(String string) {
		runtimeType = string;
	}
	/**
	 * @param string
	 */
	public void setDatatypeUUID(String string) {
		datatypeUUID = string;
	}

	/**
	 * @param object
	 */
	public void setDefaultValue(Object object) {
		defaultValue = object;
	}

	/**
	 * @param i
	 */
	public void setType(int i) {
		type = i;
	}

    /**
     * @param b
     */
    public void setOptional(boolean b) {
        optional = b;
    }
    
    public DatatypeRecordImpl getDatatype() {
		return datatype;
	}
    
    public void setDatatype(DatatypeRecordImpl datatype) {
		this.datatype = datatype;
	}

}