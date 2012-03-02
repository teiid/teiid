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

package org.teiid.query.processor.xml;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import javax.xml.transform.TransformerException;

import net.sf.saxon.value.DateTimeValue;
import net.sf.saxon.value.GYearMonthValue;

import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.util.CommandContext;



/**
 * This class will make a minimal effort to output xsd formatted values for a given
 * built-in type.  It will not attempt to narrow or otherwise fit most values into
 * their output space (months can be greater than 12, nonNegative numbers can be 
 * negative, etc.)
 */
public final class XMLValueTranslator {

    private static String NEGATIVE_INFINITY = "-INF"; //$NON-NLS-1$
    private static String POSITIVE_INFINITY = "INF"; //$NON-NLS-1$
    
    private static String GMONTHDAY_FORMAT = "--MM-dd"; //$NON-NLS-1$
    private static String GYEAR_FORMAT = "0000"; //$NON-NLS-1$
    
    public static final String DATETIME    = "dateTime"; //$NON-NLS-1$
    public static final String DOUBLE      = "double"; //$NON-NLS-1$
    public static final String FLOAT       = "float"; //$NON-NLS-1$
    public static final String GDAY        = "gDay"; //$NON-NLS-1$
    public static final String GMONTH      = "gMonth"; //$NON-NLS-1$
    public static final String GMONTHDAY   = "gMonthDay"; //$NON-NLS-1$
    public static final String GYEAR       = "gYear"; //$NON-NLS-1$
    public static final String GYEARMONTH  = "gYearMonth"; //$NON-NLS-1$
    
    public static final String STRING = "string"; //$NON-NLS-1$
    
    private static final Map<String, Integer> TYPE_CODE_MAP;
    
    private static final int DATETIME_CODE = 0;
    private static final int DOUBLE_CODE = 1;
    private static final int FLOAT_CODE = 2;
    private static final int GDAY_CODE = 3;
    private static final int GMONTH_CODE = 4;
    private static final int GMONTHDAY_CODE = 5;
    private static final int GYEAR_CODE = 6;
    private static final int GYEARMONTH_CODE = 7;
        
    static {
        TYPE_CODE_MAP = new HashMap<String, Integer>(20);
        TYPE_CODE_MAP.put(DATETIME, new Integer(DATETIME_CODE));
        TYPE_CODE_MAP.put(DOUBLE, new Integer(DOUBLE_CODE));
        TYPE_CODE_MAP.put(FLOAT, new Integer(FLOAT_CODE));
        TYPE_CODE_MAP.put(GDAY, new Integer(GDAY_CODE));
        TYPE_CODE_MAP.put(GMONTH, new Integer(GMONTH_CODE));
        TYPE_CODE_MAP.put(GMONTHDAY, new Integer(GMONTHDAY_CODE));
        TYPE_CODE_MAP.put(GYEAR, new Integer(GYEAR_CODE));
        TYPE_CODE_MAP.put(GYEARMONTH, new Integer(GYEARMONTH_CODE));   
    }
       
    /**
     * Translate the value object coming from the mapping class into the string that will be 
     * placed in the XML document for a tag.  Usually, the value.toString() method is just called
     * to translate to a string.  In some special cases, the runtimeType and builtInType
     * will be used to determine a custom translation to string where the Java object toString()
     * does not comply with the XML Schema spec.
     * 
     * NOTE: date objects are not checked for years less than 1
     *    
     * @param value The value
     * @param runtimeType The runtime type
     * @param builtInType The design-time atomic built-in type (or null if none)
     * @return String representing the value
     * @throws FunctionExecutionException 
     * @throws TransformationException 
     * @since 5.0
     */
    public static String translateToXMLValue(Object value, Class<?> runtimeType, String builtInType, CommandContext context) throws FunctionExecutionException, TransformationException {
        if (value == null) {
            return null;
        }
        
        Integer typeCode = TYPE_CODE_MAP.get(builtInType);
        
        String valueStr = null;
        
        if (builtInType == null || typeCode == null || runtimeType == DataTypeManager.DefaultDataClasses.STRING || STRING.equals(builtInType)) {
            valueStr = defaultTranslation(value);
        } else {
        
            int type = typeCode.intValue();
            
            switch (type) {
                case DATETIME_CODE:
				try {
					valueStr = XMLSystemFunctions.convertToAtomicValue(value).getStringValue();
				} catch (TransformerException e) {
					throw new TransformationException(e, e.getMessage());
				}
                    break;
                case DOUBLE_CODE:
                    valueStr = doubleToDouble((Double)value);
                    break;
                case FLOAT_CODE:
                    valueStr = floatToFloat((Float)value);
                    break;
                case GDAY_CODE:
                    valueStr = bigIntegerTogDay((BigInteger)value);
                    break;
                case GMONTH_CODE:
                    valueStr = bigIntegerTogMonth((BigInteger)value);
                    break;
                case GMONTHDAY_CODE:
                    valueStr = FunctionMethods.format(context, (Timestamp)value, GMONTHDAY_FORMAT);
                    break;
                case GYEAR_CODE:
                    valueStr = FunctionMethods.format(context, (BigInteger)value, GYEAR_FORMAT);
                    break;
                case GYEARMONTH_CODE:
				DateTimeValue dtv;
				try {
					dtv = ((DateTimeValue)XMLSystemFunctions.convertToAtomicValue(value));
				} catch (TransformerException e) {
					throw new TransformationException(e, e.getMessage());
				}
                	valueStr = new GYearMonthValue(dtv.getYear(), dtv.getMonth(), dtv.getTimezoneInMinutes()).getStringValue();
                    break;
                default:
                    valueStr = defaultTranslation(value);
                    break;
            }
            
        }        

        //Per defects 11789, 14905, 15117
        if (valueStr != null && valueStr.length()==0){
            valueStr = null;
        }
    
        return valueStr;
    }

    /**
     * Translate any non-null value to a string by using the Object toString() method.
     * This works in any case where the Java string representation of an object is the 
     * same as the expected XML Schema output form.
     *   
     * @param value Value returned from a mapping class
     * @return String content to put in XML output
     * @throws TransformationException 
     * @since 5.0
     */
    static String defaultTranslation(Object value) throws TransformationException {
        return DataTypeManager.transformValue(value, DataTypeManager.DefaultDataClasses.STRING);
    }
    
    /**
     * Translate runtime float to xs:float string value.  The normal Java representation 
     * for floats is fine except the strings used for positive and negative infinity 
     * are different.
     *  
     * @param f Runtime float
     * @return String representing xs:float
     * @throws TransformationException 
     * @since 5.0
     */
    static String floatToFloat(Float f) throws TransformationException {
        if(f.floatValue() == Float.NEGATIVE_INFINITY) {
            return NEGATIVE_INFINITY;
        } else if(f.floatValue() == Float.POSITIVE_INFINITY) {
            return POSITIVE_INFINITY;
        }
        return defaultTranslation(f);
    }

    /**
     * Translate runtime double to xs:double string value.  The normal Java representation 
     * for doubles is fine except the strings used for positive and negative infinity 
     * are different.
     *  
     * @param d Runtime double
     * @return String representing xs:double
     * @throws TransformationException 
     * @since 5.0
     */
    static String doubleToDouble(Double d) throws TransformationException {
        if(d.doubleValue() == Double.NEGATIVE_INFINITY) {
            return NEGATIVE_INFINITY;
        } else if(d.doubleValue() == Double.POSITIVE_INFINITY) {
            return POSITIVE_INFINITY;
        }                    
        return defaultTranslation(d);
    }

    /**
     * gMonths out of the valid range are returned in tact. 
     * @param value
     * @return
     * @since 5.0
     */
    static String bigIntegerTogMonth(BigInteger value) {
        long month = value.longValue();
        
        if(month < 10) {
            // Add leading 0
            return "--0" + month; //$NON-NLS-1$
        } 

        // Don't need leading 0
        return "--" + month; //$NON-NLS-1$        
    }
    
    /**
     * gDays out of the valid range are returned in tact. 
     * @param value
     * @return
     * @since 5.0
     */
    static String bigIntegerTogDay(BigInteger value) {
        long day = value.longValue();
        
        if(day < 10) {
            // Add leading 0
            return "---0" + day; //$NON-NLS-1$
        } 

        // Don't need leading 0
        return "---" + day; //$NON-NLS-1$        
    }
    
}
