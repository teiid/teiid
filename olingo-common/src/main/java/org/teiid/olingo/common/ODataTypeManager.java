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
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.apache.olingo.commons.api.edm.EdmParameter;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.geo.Geospatial;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBinary;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDecimal;
import org.apache.olingo.commons.core.edm.primitivetype.EdmPrimitiveTypeFactory;
import org.apache.olingo.commons.core.edm.primitivetype.EdmStream;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.commons.core.edm.primitivetype.EdmTimeOfDay;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.teiid.GeometryInputSource;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.*;
import org.teiid.query.function.GeometryUtils;
import org.teiid.translator.TranslatorException;

import com.vividsolutions.jts.geom.Geometry;

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
        odataTypes.put("Edm.GeometryMultiPoint", DataTypeManager.DefaultDataTypes.GEOMETRY); //$NON-NLS-1$
        odataTypes.put("Edm.GeometryMultiLineString", DataTypeManager.DefaultDataTypes.GEOMETRY); //$NON-NLS-1$
        odataTypes.put("Edm.GeometryCollection", DataTypeManager.DefaultDataTypes.GEOMETRY); //$NON-NLS-1$
        
        teiidTypes.put(DataTypeManager.DefaultDataTypes.STRING, "Edm.String");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BOOLEAN, "Edm.Boolean");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.SHORT, "Edm.Byte");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BYTE, "Edm.SByte");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.SHORT, "Edm.Int16");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.INTEGER, "Edm.Int32");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.LONG, "Edm.Int64");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.FLOAT, "Edm.Single");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.DOUBLE, "Edm.Double");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL, "Edm.Decimal");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.DATE, "Edm.Date");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.TIME, "Edm.TimeOfDay");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.TIMESTAMP, "Edm.DateTimeOffset");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BLOB, "Edm.Stream");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.CLOB, "Edm.Stream");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.XML, "Edm.Stream");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.VARBINARY, "Edm.Binary"); //$NON-NLS-1$
        //will fail for most values
        teiidTypes.put(DataTypeManager.DefaultDataTypes.OBJECT, "Edm.Binary"); //$NON-NLS-1$ 
        teiidTypes.put(DataTypeManager.DefaultDataTypes.GEOMETRY, "Edm.Stream"); //$NON-NLS-1$
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
	public static Object convertToTeiidRuntimeType(Class<?> type, Object value, String odataType) throws TeiidException {
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
                Object arrayItem = convertToTeiidRuntimeType(expectedArrayComponentType, list.get(i), null);
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
        
        if (value instanceof Geospatial && type == DataTypeManager.DefaultDataClasses.GEOMETRY) {
        	final Geospatial val = (Geospatial)value;
        	
        	//the strategy of converting to GML will only work for reading from
        	//the odata translator - we need to actually convert to the GeometryType
        	//for the odata service.  When we undertake that, then the forked Atom serializer can be removed.
        	
        	//the parser will guess geography by default.  we'll simply rely on the import logic to have chosen geometry only for edm geometry type values
        	//if (val.getDimension() == Dimension.GEOMETRY) {
				return new GeometryInputSource() {
					@Override
					public Reader getGml() throws Exception {
						AtomGeoValueSerializer serializer = new AtomGeoValueSerializer();
						XMLOutputFactory factory = XMLOutputFactory.newInstance();
						StringWriter sw = new StringWriter();
						final XMLStreamWriter writer = factory.createXMLStreamWriter(sw);
						serializer.serialize(writer, val);
						writer.close();
						return new StringReader(sw.toString());
					}
					
					@Override
					public Integer getSrid() {
						String srid = val.getSrid().toString();
						try {
							return Integer.parseInt(srid);
						} catch (NumberFormatException e) {
							return null;
						}
					}
				};
        	//}
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
            final String odataType) throws TeiidException {
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
            value = convertToTeiidRuntimeType(type, new String(contents), odataType);
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
    
    public static String convertToODataURIValue(Object val, String odataType) throws EdmPrimitiveTypeException {
        if (val == null) {
            return "null"; // is this correct? //$NON-NLS-1$
        }
        if(odataType.startsWith("Edm.")) { //$NON-NLS-1$
            odataType = odataType.substring(4);
        }
        if (odataType.startsWith("Geometry") && val instanceof GeometryType) { //$NON-NLS-1$
        	Geometry g;
			try {
				g = GeometryUtils.getGeometry((GeometryType)val);
			} catch (FunctionExecutionException e1) {
				throw new EdmPrimitiveTypeException(e1.getMessage(), e1);
			}
        	StringWriter sw = new StringWriter();
        	sw.write("geometry'SRID="); //$NON-NLS-1$
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
        EdmPrimitiveTypeKind kind = EdmPrimitiveTypeKind.valueOf(odataType);
        String value =  EdmPrimitiveTypeFactory.getInstance(kind).valueToString(
                val, true, DataTypeManager.MAX_STRING_LENGTH, 0, 0, true);
        if (kind == EdmPrimitiveTypeKind.String) {
            return EdmString.getInstance().toUriLiteral(value);
        }
        return value;
    }   

}
