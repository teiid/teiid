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

public class BaseColumn extends AbstractMetadataRecord {
	
	private String datatypeUUID;
    private String runtimeType;
    private Object defaultValue;
    private int length;
    private int scale;
    private int radix;
    private int precision;
    private int nullType;
    private int position;
    private DatatypeRecordImpl datatype;

    public Object getDefaultValue() {
        return defaultValue;
    }

    public String getDatatypeUUID() {
        return datatypeUUID;
    }

    public String getRuntimeType() {
        return runtimeType;
    }

    public int getLength() {
        return length;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public int getRadix() {
        return radix;
    }

    public int getPosition() {
        return position;
    }

    public int getNullType() {
        return nullType;
    }

	public void setLength(int i) {
		length = i;
	}

	public void setPrecision(int i) {
		precision = i;
	}

	public void setScale(int i) {
		scale = i;
	}

	public void setRadix(int i) {
		radix = i;
	}

    public void setNullType(int i) {
        nullType = i;
    }

	public void setPosition(int i) {
		position = i;
	}

	public void setRuntimeType(String string) {
		runtimeType = string;
	}

	public void setDatatypeUUID(String string) {
		datatypeUUID = string;
	}

	public void setDefaultValue(Object object) {
		defaultValue = object;
	}

    public DatatypeRecordImpl getDatatype() {
		return datatype;
	}
    
    public void setDatatype(DatatypeRecordImpl datatype) {
		this.datatype = datatype;
	}

}
