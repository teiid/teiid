/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.metadata;

import java.util.Collections;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.translator.TypeFacility;

public abstract class BaseColumn extends AbstractMetadataRecord {

    public static final String DEFAULT_HANDLING = AbstractMetadataRecord.RELATIONAL_PREFIX + "default_handling"; //$NON-NLS-1$
    public static final String EXPRESSION_DEFAULT = "expression"; //$NON-NLS-1$
    public static final String OMIT_DEFAULT = "omit"; //$NON-NLS-1$

    public static final String SPATIAL_SRID = MetadataFactory.SPATIAL_PREFIX + "srid"; //$NON-NLS-1$
    public static final String SPATIAL_TYPE = MetadataFactory.SPATIAL_PREFIX + "type"; //$NON-NLS-1$
    public static final String SPATIAL_COORD_DIMENSION = MetadataFactory.SPATIAL_PREFIX + "coord_dimension"; //$NON-NLS-1$

    //the defaults are safe for odbc/jdbc metadata
    public static final int DEFAULT_PRECISION = Short.MAX_VALUE;
    //similar to postgresql behavior, we default to a non-zero
    public static final int DEFAULT_SCALE = Short.MAX_VALUE/2;

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
    private int arrayDimensions;
    private String nativeType;

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
        if (precision == 0 && this.getDatatype() != null && getDatatype().getName().equals(DataTypeManager.DefaultDataTypes.BIG_DECIMAL)) {
            return DEFAULT_PRECISION;
        }
        return precision;
    }

    public int getScale() {
        if (this.getDatatype() != null && getDatatype().getName().equals(DataTypeManager.DefaultDataTypes.BIG_DECIMAL)) {
            if (precision == 0 && scale == 0) {
                return DEFAULT_SCALE;
            }
            int effectivePrecision = precision == 0?DEFAULT_PRECISION:precision;
            if (Math.abs(scale) > effectivePrecision) {
                return Integer.signum(scale)*effectivePrecision;
            }
        }
        return scale;
    }

    public boolean isDefaultPrecisionScale() {
        return precision == 0 && scale == 0;
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
        runtimeType = DataTypeManager.getCanonicalString(string);
    }

    public void setDatatypeUUID(String string) {
        datatypeUUID = DataTypeManager.getCanonicalString(string);
    }

    public void setDefaultValue(String object) {
        defaultValue = DataTypeManager.getCanonicalString(object);
    }

    /**
     * Get the type.  Represents the component type if {@link #getArrayDimensions()} &gt; 0
     * @return
     */
    public Datatype getDatatype() {
        return datatype;
    }

    /**
     * Set the datatype without copying any of the datatype attributes and assumes a non-array type
     * @see #setDatatype(Datatype, boolean, int) for more control over the type info
     * @param datatype
     */
    public void setDatatype(Datatype datatype) {
        setDatatype(datatype, false, 0);
    }

    /**
     * Set the datatype and assumes a non-array type
     * @see #setDatatype(Datatype, boolean, int) for more control over the type info
     * @param datatype
     * @param copyAttributes
     */
    public void setDatatype(Datatype datatype, boolean copyAttributes) {
        setDatatype(datatype, copyAttributes, 0);
    }

    public void setDatatype(Datatype datatype, boolean copyAttributes, int arrayDimensions) {
        this.datatype = datatype;
        this.arrayDimensions = arrayDimensions;
        if (datatype != null) {
            this.datatypeUUID = this.datatype.getUUID();
            this.runtimeType = this.datatype.getRuntimeTypeName();
            if (arrayDimensions > 0) {
                this.runtimeType += StringUtil.join(Collections.nCopies(arrayDimensions, "[]"), ""); //$NON-NLS-1$ //$NON-NLS-2$
            }
            if (copyAttributes) {
                this.radix = this.datatype.getRadix();
                this.length = this.datatype.getLength();
                if (!datatype.getName().equals(DataTypeManager.DefaultDataTypes.BIG_DECIMAL)) {
                    this.precision = this.datatype.getPrecision();
                    this.scale = this.datatype.getScale();
                }
                this.nullType = this.datatype.getNullType();
            }
        }
    }

    /**
     * Get the array dimensions.
     * @return
     */
    public int getArrayDimensions() {
        return arrayDimensions;
    }

    public String getNativeType() {
        return nativeType;
    }

    /**
     * @param nativeType The nativeType to set.
     * @since 4.2
     */
    public void setNativeType(String nativeType) {
        this.nativeType = DataTypeManager.getCanonicalString(nativeType);
    }

}
