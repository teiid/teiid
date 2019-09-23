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
package org.teiid.olingo.common;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmParameter;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.geo.Geospatial;
import org.apache.olingo.commons.api.edm.geo.Geospatial.Dimension;
import org.apache.olingo.commons.api.edm.geo.SRID;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBinary;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDecimal;
import org.apache.olingo.commons.core.edm.primitivetype.EdmPrimitiveTypeFactory;
import org.apache.olingo.commons.core.edm.primitivetype.EdmStream;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.commons.core.edm.primitivetype.EdmTimeOfDay;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.locationtech.jts.geom.Geometry;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.*;
import org.teiid.core.util.StringUtil;
import org.teiid.metadata.BaseColumn;
import org.teiid.query.function.GeometryUtils;
import org.teiid.translator.TranslatorException;

public class ODataTypeManager {

    private static HashMap<String, String> odataTypes = new HashMap<String, String>();
    private static HashMap<String, String> teiidTypes = new HashMap<String, String>();

    static {
        odataTypes.put("Edm.String", DataTypeManager.DefaultDataTypes.STRING);
        odataTypes.put("Edm.Boolean", DataTypeManager.DefaultDataTypes.BOOLEAN);
        odataTypes.put("Edm.Byte", DataTypeManager.DefaultDataTypes.SHORT);
        odataTypes.put("Edm.SByte", DataTypeManager.DefaultDataTypes.BYTE);
        odataTypes.put("Edm.Int16", DataTypeManager.DefaultDataTypes.SHORT);
        odataTypes.put("Edm.Int32", DataTypeManager.DefaultDataTypes.INTEGER);
        odataTypes.put("Edm.Int64", DataTypeManager.DefaultDataTypes.LONG);
        odataTypes.put("Edm.Single", DataTypeManager.DefaultDataTypes.FLOAT);
        odataTypes.put("Edm.Double", DataTypeManager.DefaultDataTypes.DOUBLE);
        odataTypes.put("Edm.Decimal", DataTypeManager.DefaultDataTypes.BIG_DECIMAL);
        odataTypes.put("Edm.Date", DataTypeManager.DefaultDataTypes.DATE);
        odataTypes.put("Edm.TimeOfDay", DataTypeManager.DefaultDataTypes.TIME);
        odataTypes.put("Edm.DateTimeOffset", DataTypeManager.DefaultDataTypes.TIMESTAMP);
        odataTypes.put("Edm.Stream", DataTypeManager.DefaultDataTypes.BLOB);
        odataTypes.put("Edm.Guid", DataTypeManager.DefaultDataTypes.STRING);
        odataTypes.put("Edm.Binary", DataTypeManager.DefaultDataTypes.VARBINARY); //$NON-NLS-1$
        odataTypes.put("Edm.Geometry", DataTypeManager.DefaultDataTypes.GEOMETRY); //$NON-NLS-1$
        odataTypes.put("Edm.GeometryPoint", DataTypeManager.DefaultDataTypes.GEOMETRY); //$NON-NLS-1$
        odataTypes.put("Edm.GeometryLineString", DataTypeManager.DefaultDataTypes.GEOMETRY); //$NON-NLS-1$
        odataTypes.put("Edm.GeometryPolygon", DataTypeManager.DefaultDataTypes.GEOMETRY); //$NON-NLS-1$
        odataTypes.put("Edm.GeometryMultiPolygon", DataTypeManager.DefaultDataTypes.GEOMETRY); //$NON-NLS-1$
        odataTypes.put("Edm.GeometryMultiPoint", DataTypeManager.DefaultDataTypes.GEOMETRY); //$NON-NLS-1$
        odataTypes.put("Edm.GeometryMultiLineString", DataTypeManager.DefaultDataTypes.GEOMETRY); //$NON-NLS-1$
        odataTypes.put("Edm.GeometryCollection", DataTypeManager.DefaultDataTypes.GEOMETRY); //$NON-NLS-1$
        odataTypes.put("Edm.Geography", DataTypeManager.DefaultDataTypes.GEOMETRY); //$NON-NLS-1$
        odataTypes.put("Edm.GeographyPoint", DataTypeManager.DefaultDataTypes.GEOGRAPHY); //$NON-NLS-1$
        odataTypes.put("Edm.GeographyLineString", DataTypeManager.DefaultDataTypes.GEOGRAPHY); //$NON-NLS-1$
        odataTypes.put("Edm.GeographyPolygon", DataTypeManager.DefaultDataTypes.GEOGRAPHY); //$NON-NLS-1$
        odataTypes.put("Edm.GeographyMultiPolygon", DataTypeManager.DefaultDataTypes.GEOGRAPHY); //$NON-NLS-1$
        odataTypes.put("Edm.GeographyMultiPoint", DataTypeManager.DefaultDataTypes.GEOGRAPHY); //$NON-NLS-1$
        odataTypes.put("Edm.GeographyMultiLineString", DataTypeManager.DefaultDataTypes.GEOGRAPHY); //$NON-NLS-1$
        odataTypes.put("Edm.GeographyCollection", DataTypeManager.DefaultDataTypes.GEOGRAPHY); //$NON-NLS-1$
        teiidTypes.put(DataTypeManager.DefaultDataTypes.STRING, "Edm.String");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BOOLEAN, "Edm.Boolean");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BYTE, "Edm.SByte");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.SHORT, "Edm.Int16");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.INTEGER, "Edm.Int32");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.LONG, "Edm.Int64");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.FLOAT, "Edm.Single");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.DOUBLE, "Edm.Double");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BIG_INTEGER, "Edm.Decimal");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL, "Edm.Decimal");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.DATE, "Edm.Date");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.TIME, "Edm.TimeOfDay");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.TIMESTAMP, "Edm.DateTimeOffset");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BLOB, "Edm.Stream");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.CLOB, "Edm.Stream");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.JSON, "Edm.Stream");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.XML, "Edm.Stream");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.VARBINARY, "Edm.Binary"); //$NON-NLS-1$
        //will fail for most values
        teiidTypes.put(DataTypeManager.DefaultDataTypes.OBJECT, "Edm.Binary"); //$NON-NLS-1$
        teiidTypes.put(DataTypeManager.DefaultDataTypes.GEOMETRY, "Edm.Stream"); //$NON-NLS-1$
        teiidTypes.put(DataTypeManager.DefaultDataTypes.GEOGRAPHY, "Edm.Stream"); //$NON-NLS-1$
    }

    public static String teiidType(SingletonPrimitiveType odataType, boolean array) {
        String type =  odataType.getFullQualifiedName().getFullQualifiedNameAsString();
        return teiidType(type, array);
    }

    public static String teiidType(String odataType, boolean array) {
        String type =  odataTypes.get(odataType);
        if (type == null) {
            type = DataTypeManager.DefaultDataTypes.STRING; // special case for enum type
        }
        if (array) {
           type +="[]";
        }
        return type;
    }

    public static EdmPrimitiveTypeKind odataType(Class<?> teiidRuntimeTypeClass) {
        String dataType = DataTypeManager.getDataTypeName(teiidRuntimeTypeClass);
        return odataType(dataType);
    }

    public static EdmPrimitiveTypeKind odataType(BaseColumn c) {
        String runtimeType = c.getRuntimeType();
        //try to map to the specific type
        if (c.getDatatype() != null
                && (c.getDatatype().getName().equalsIgnoreCase(DataTypeManager.DefaultDataTypes.GEOMETRY)
                    || c.getDatatype().getName().equalsIgnoreCase(DataTypeManager.DefaultDataTypes.GEOGRAPHY))) {
            boolean geometry = c.getDatatype().getName().equalsIgnoreCase(DataTypeManager.DefaultDataTypes.GEOMETRY);
            String type = c.getProperty(BaseColumn.SPATIAL_TYPE, false);
            if (type != null) {
                try {
                    return StringUtil.caseInsensitiveValueOf(EdmPrimitiveTypeKind.class,
                            (geometry?"Geometry":"Geography")+type); //$NON-NLS-1$ //$NON-NLS-2$
                } catch (IllegalArgumentException e) {

                }
            }
            //unknown or null case
            if (geometry) {
                return EdmPrimitiveTypeKind.Geometry;
            }
            return EdmPrimitiveTypeKind.Geography;
        }
        return odataType(runtimeType);
    }

    public static EdmPrimitiveTypeKind odataType(String teiidRuntimeType) {
        if (teiidRuntimeType.endsWith("[]")) {
            teiidRuntimeType = teiidRuntimeType.substring(0, teiidRuntimeType.length()-2);
            //multi-dimensional is not supported - will be returned as string
        }
        String type =  teiidTypes.get(teiidRuntimeType);
        if (type == null) {
            type = "Edm.String";
        }
        return EdmPrimitiveTypeKind.valueOfFQN(type);
    }

    /**
     *
     * @param type
     * @param value
     * @param odataType type hint if the value could be a string containing a literal value of another type
     * @return
     * @throws TeiidException
     */
    public static Object convertToTeiidRuntimeType(Class<?> type, Object value, String odataType, String srid) throws TeiidException {
        if (value == null) {
            return null;
        }
        if (type.isAssignableFrom(value.getClass())) {
            return value;
        }
        if (value instanceof UUID) {
            return value.toString();
        }
        if (type.isArray() && value instanceof List<?>) {
            List<?> list = (List<?>)value;
            Class<?> expectedArrayComponentType = type.getComponentType();
            Object array = Array.newInstance(type.getComponentType(), list.size());
            for (int i = 0; i < list.size(); i++) {
                Object arrayItem = convertToTeiidRuntimeType(expectedArrayComponentType, list.get(i), null, srid);
                Array.set(array, i, arrayItem);
            }
            return array;
        }

        if (odataType != null && value instanceof String) {
            try {
                value = ODataTypeManager.parseLiteral(odataType, (String)value);
            } catch (TeiidException e) {
                throw new TranslatorException(e);
            }
        }

        if (value instanceof Geospatial) {
            final Geospatial val = (Geospatial)value;

            //Due to https://issues.apache.org/jira/browse/OLINGO-1299
            //we cannot rely on the dimension of the value, so we
            //pass in the type
            return Olingo2Teiid.convert(val, type, srid);
        }
        if (value instanceof Calendar) {
            Calendar calender = (Calendar)value;
            if (type.isAssignableFrom(java.sql.Time.class)) {
                calender.set(Calendar.YEAR, 1970);
                calender.set(Calendar.MONTH, Calendar.JANUARY);
                calender.set(Calendar.DAY_OF_MONTH, 1);
                calender.set(Calendar.MILLISECOND, 0);
                return new Time(calender.getTimeInMillis());
            } else if (type.isAssignableFrom(java.sql.Date.class)) {
                calender.set(Calendar.HOUR_OF_DAY, 0);
                calender.set(Calendar.MINUTE, 0);
                calender.set(Calendar.SECOND, 0);
                calender.set(Calendar.MILLISECOND, 0);
                return new java.sql.Date(calender.getTimeInMillis());
            } else if (type.isAssignableFrom(java.sql.Timestamp.class)) {
                return new Timestamp(calender.getTimeInMillis());
            }
        }

        Transform transform = DataTypeManager.getTransform(value.getClass(), type);
        if (transform != null) {
            try {
                value = transform.transform(value, type);
            } catch (TransformationException e) {
                throw new TeiidException(e);
            }
        }
        return value;
    }

    public static Object convertByteArrayToTeiidRuntimeType(final Class<?> type, final byte[] contents,
            final String odataType, String srid) throws TeiidException {
        if (contents == null) {
            return null;
        }
        Object value = null;
        if (DataTypeManager.isLOB(type)) {
            InputStreamFactory isf = new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return new ByteArrayInputStream(contents);
                }
            };
            if (type.isAssignableFrom(SQLXML.class)) {
                value = new SQLXMLImpl(isf);
            } else if (type.isAssignableFrom(ClobType.class)) {
                value = new ClobImpl(isf, -1);
            } else if (type.isAssignableFrom(BlobType.class)) {
                value = new BlobImpl(isf);
            }
        } else if (DataTypeManager.DefaultDataClasses.VARBINARY.equals(type)) {
            value = contents;
        } else {
            value = convertToTeiidRuntimeType(type, new String(contents), odataType, srid);
        }
        return value;
    }

    public static Object parseLiteral(EdmParameter edmParameter, Class<?> runtimeType, String value)
            throws TeiidProcessingException {
        EdmPrimitiveType primitiveType = EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind
                .valueOf(edmParameter.getType()
                        .getFullQualifiedName()
                        .getFullQualifiedNameAsString().substring(4)));

        try {
            if (EdmString.getInstance().equals(edmParameter.getType())) {
                value = EdmString.getInstance().fromUriLiteral(value);
            }
            Object converted =  primitiveType.valueOfString(value,
                    edmParameter.isNullable(),
                    edmParameter.getMaxLength(),
                    edmParameter.getPrecision(),
                    edmParameter.getScale(),
                    true,
                    runtimeType);
            return converted;
        } catch (EdmPrimitiveTypeException e) {
            throw new TeiidProcessingException(e);
        }
    }

    public static Object parseLiteral(EdmProperty edmProperty, Class<?> runtimeType, String value)
            throws TeiidException {
        EdmPrimitiveType primitiveType = EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind
                .valueOf(edmProperty.getType()
                        .getFullQualifiedName()
                        .getFullQualifiedNameAsString().substring(4)));

        try {
            if (EdmString.getInstance().equals(edmProperty.getType())) {
                value = EdmString.getInstance().fromUriLiteral(value);
            }
            Object converted =  primitiveType.valueOfString(value,
                    edmProperty.isNullable(),
                    edmProperty.getMaxLength(),
                    edmProperty.getPrecision(),
                    edmProperty.getScale(),
                    true,
                    runtimeType);
            return converted;
        } catch (EdmPrimitiveTypeException e) {
            throw new TeiidException(e);
        }
    }

    public static Object parseLiteral(String odataType, String value)
            throws TeiidException {
        EdmPrimitiveType primitiveType = EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind
                .valueOf(odataType.substring(4)));

        int maxLength = DataTypeManager.MAX_STRING_LENGTH;
        if (primitiveType instanceof EdmBinary ||primitiveType instanceof EdmStream) {
            maxLength = DataTypeManager.MAX_VARBINARY_BYTES;
        }

        int precision = 4;
        int scale = 3;
        if (primitiveType instanceof EdmDecimal) {
            precision = 38;
            scale = 9;
        }

        Class<?> expectedClass = primitiveType.getDefaultType();

        try {
            if (EdmString.getInstance().equals(primitiveType)) {
                value = EdmString.getInstance().fromUriLiteral(value);
            }
            Object converted =  primitiveType.valueOfString(value,
                    false,
                    maxLength,
                    precision,
                    scale,
                    true,
                    expectedClass);

            if (primitiveType instanceof EdmTimeOfDay) {
                Calendar ts =  (Calendar)converted;
                return new Time(ts.getTimeInMillis());
            } else if (primitiveType instanceof EdmDate) {
                Calendar ts =  (Calendar)converted;
                return new Date(ts.getTimeInMillis());
            }
            return converted;
        } catch (EdmPrimitiveTypeException e) {
            throw new TeiidException(e);
        }
    }

    public static Object rationalizePrecision(Integer precision, Integer scale, Object value) {
        if (precision == null) {
            return value;
        }
        if (value instanceof BigDecimal) {
            BigDecimal bigDecimalValue = (BigDecimal)value;
            //if precision is set, then try to set an appropriate scale to pass the facet check
            final int digits = bigDecimalValue.scale() >= 0
                      ? Math.max(bigDecimalValue.precision(), bigDecimalValue.scale())
                          : bigDecimalValue.precision() - bigDecimalValue.scale();

            if (bigDecimalValue.scale() > (scale == null ? 0 : scale) || (digits > precision)) {
                BigDecimal newBigDecimal = bigDecimalValue.setScale(Math.min(digits > precision ? bigDecimalValue.scale() - digits + precision : bigDecimalValue.scale(), scale == null ? 0 : scale), RoundingMode.HALF_UP);
                //only allow for trimming trailing zeros
                if (newBigDecimal.compareTo(bigDecimalValue) == 0) {
                    bigDecimalValue = newBigDecimal;
                }
            }
            return bigDecimalValue;
        } else if (value instanceof Timestamp) {
            if (precision < 9) {
                Timestamp timestamp = (Timestamp)value;
                int nanos = timestamp.getNanos();
                long mask = (long)Math.pow(10, 9-precision);
                long adjusted = (nanos / mask) * mask;
                if (adjusted != nanos) {
                    Timestamp result = new Timestamp(timestamp.getTime());
                    result.setNanos((int)adjusted);
                    return result;
                }

            }
        }
        return value;
    }

    public static Geospatial convertToODataValue(InputStream wkb, boolean includesSrid)
            throws FunctionExecutionException {
        Geometry g = GeometryUtils.getGeometry(wkb, null, includesSrid);
        JTS2OlingoBridge bridge = new JTS2OlingoBridge(Dimension.GEOMETRY, includesSrid?SRID.valueOf(String.valueOf(g.getSRID())):null);
        return bridge.convert(g);
    }

    public static String convertToODataURIValue(Object val, String odataType) throws EdmPrimitiveTypeException {
        if (val == null) {
            return "null"; // is this correct? //$NON-NLS-1$
        }
        if(odataType.startsWith("Edm.")) { //$NON-NLS-1$
            odataType = odataType.substring(4);
        }
        if (val instanceof AbstractGeospatialType) {
            Geometry g;
            try {
                g = GeometryUtils.getGeometry((AbstractGeospatialType)val);
            } catch (FunctionExecutionException e1) {
                throw new EdmPrimitiveTypeException(e1.getMessage(), e1);
            }
            return geometryToODataValueString(g, val instanceof GeometryType);
        }
        EdmPrimitiveTypeKind kind = EdmPrimitiveTypeKind.valueOf(odataType);
        String value =  EdmPrimitiveTypeFactory.getInstance(kind).valueToString(
                val, true, null, null, Integer.MAX_VALUE, true);
        if (kind == EdmPrimitiveTypeKind.String) {
            return EdmString.getInstance().toUriLiteral(value);
        }
        return value;
    }

    static String geometryToODataValueString(Geometry g, boolean geometry) {
        StringWriter sw = new StringWriter();
        sw.write((geometry?"geometry":"geography")+"'SRID="); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        sw.write(String.valueOf(g.getSRID()));
        sw.write(";"); //$NON-NLS-1$
        ODataWKTWriter writer = new ODataWKTWriter();
        try {
            writer.write(g, sw);
        } catch (IOException e) {
            throw new TeiidRuntimeException(e);
        }
        sw.write("'"); //$NON-NLS-1$
        return sw.toString();
    }

}
