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
import org.teiid.translator.TypeFacility;

public abstract class BaseColumn extends AbstractMetadataRecord {
	
	private static final long serialVersionUID = 6382258617714856616L;

	public enum NullType {
		No_Nulls {
			@Override
			public String toString() {
				return "No Nulls"; //$NON-NLS-1$
			}
		},
		Nullable,
		Unknown		
	}
	
	private String datatypeUUID;
    private String runtimeType;
    private String defaultValue;
    private int length;
    private int scale;
    private int radix;
    private int precision;
    private NullType nullType;
    private int position;
    private Datatype datatype;

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getDatatypeUUID() {
        return datatypeUUID;
    }

    public String getRuntimeType() {
        return runtimeType;
    }
    
    public Class<?> getJavaType() {
    	return TypeFacility.getDataTypeClass(runtimeType);
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

    /**
     * 1 based ordinal position
     * @return
     */
    public int getPosition() {
        return position;
    }

    public NullType getNullType() {
    	if (nullType == null) {
    		return NullType.Unknown;
    	}
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

    public void setNullType(NullType i) {
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

	public void setDefaultValue(String object) {
		defaultValue = DataTypeManager.getCanonicalString(object);
	}

    public Datatype getDatatype() {
		return datatype;
	}
    
    public void setDatatype(Datatype datatype) {
		this.datatype = datatype;
		if (datatype != null) {
			this.datatypeUUID = this.datatype.getUUID();
			this.runtimeType = this.datatype.getRuntimeTypeName();
		}
	}
    
	public String getDatatypeID() {
		if (this.datatype != null) {
			return this.datatype.getDatatypeID();
		}
		return null;
	}
	
	public String getBaseTypeID() {
		if (this.datatype != null) {
			return this.datatype.getBasetypeID();
		}
		return null;
	}

	public String getPrimitiveTypeID() {
		if (this.datatype != null) {
			return this.datatype.getPrimitiveTypeID();
		}
		return null;
	}
}
