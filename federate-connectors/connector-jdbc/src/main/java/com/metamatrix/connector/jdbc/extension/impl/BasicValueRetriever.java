/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.jdbc.extension.impl;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.jdbc.extension.ValueRetriever;

/**
 * Retrieve objects  
 */
public class BasicValueRetriever implements ValueRetriever {
    // Because the retrieveValue() method will be hit for every value of 
    // every JDBC result set returned, we do lots of weird special stuff here 
    // to improve the performance (most importantly to remove big if/else checks
    // of every possible type.  
    
    private static final Map TYPE_CODE_MAP; // Map of primitive Class -> Integer code
    
    private static final int INTEGER_CODE = 0;
    private static final int LONG_CODE = 1;
    private static final int DOUBLE_CODE = 2;
    private static final int BIGDECIMAL_CODE = 3;
    private static final int SHORT_CODE = 4;
    private static final int FLOAT_CODE = 5;
    private static final int TIME_CODE = 6;
    private static final int DATE_CODE = 7;
    private static final int TIMESTAMP_CODE = 8;
    private static final int BLOB_CODE = 9;
    private static final int CLOB_CODE = 10;
    // WARNING: it might be likely to introduce bugs if you specify non-numeric types here.  Currently
    // we will use conversions from DataTypeManager to do those conversions and calling the specific
    // getter method will potentially use a different effective conversion (for boolean/etc)
    // which may conflict with previous expected results.  
    
    static {
        TYPE_CODE_MAP = new HashMap(20);
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.INTEGER, new Integer(INTEGER_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.LONG, new Integer(LONG_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.DOUBLE, new Integer(DOUBLE_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL, new Integer(BIGDECIMAL_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.SHORT, new Integer(SHORT_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.FLOAT, new Integer(FLOAT_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.TIME, new Integer(TIME_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.DATE, new Integer(DATE_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.TIMESTAMP, new Integer(TIMESTAMP_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.BLOB, new Integer(BLOB_CODE));
        TYPE_CODE_MAP.put(TypeFacility.RUNTIME_TYPES.CLOB, new Integer(CLOB_CODE));
    }
    
    /* 
     * @see com.metamatrix.connector.jdbc.extension.ValueRetriever#retrieveValue(java.sql.ResultSet, int, java.lang.Class, java.util.Calendar)
     */
    public Object retrieveValue(ResultSet results, int columnIndex, Class expectedType, int nativeSQLType, Calendar cal, TypeFacility typeFacility) throws SQLException {
        Integer code = (Integer) TYPE_CODE_MAP.get(expectedType);
        if(code != null) {
            // Calling the specific methods here is more likely to get uniform (and fast) results from different
            // data sources as the driver likely knows the best and fastest way to convert from the underlying
            // raw form of the data to the expected type.  We use a switch with codes in order without gaps
            // as there is a special bytecode instruction that treats this case as a map such that not every value 
            // needs to be tested, which means it is very fast.
            switch(code.intValue()) {
                case INTEGER_CODE:  {
                    int value = results.getInt(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }
                    return Integer.valueOf(value);
                }
                case LONG_CODE:  {
                    long value = results.getLong(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return Long.valueOf(value);
                }                
                case DOUBLE_CODE:  {
                    double value = results.getDouble(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return new Double(value);
                }                
                case BIGDECIMAL_CODE:  {
                    return results.getBigDecimal(columnIndex); 
                }
                case SHORT_CODE:  {
                    short value = results.getShort(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }                    
                    return Short.valueOf(value);
                }
                case FLOAT_CODE:  {
                    float value = results.getFloat(columnIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return new Float(value);
                }
                case TIME_CODE: {
                	return results.getTime(columnIndex, cal);
                }
                case DATE_CODE: {
                    return results.getDate(columnIndex, cal);
                }
                case TIMESTAMP_CODE: {
                    return results.getTimestamp(columnIndex, cal);
                }
    			case BLOB_CODE: {
    				try {
    					return typeFacility.convertToRuntimeType(results.getBlob(columnIndex));
    				} catch (SQLException e) {
    					// ignore
    				}
    				break;
    			}
    			case CLOB_CODE: {
    				try {
    					return typeFacility.convertToRuntimeType(results.getClob(columnIndex));
    				} catch (SQLException e) {
    					// ignore
    				}
    				break;
    			}                
            }
        }

        // otherwise fall through and call getObject() and rely on the normal translation routines
        switch(nativeSQLType) {
	        case Types.BLOB: 
                return typeFacility.convertToRuntimeType(results.getBlob(columnIndex));
	        case Types.CLOB: 
	        	return typeFacility.convertToRuntimeType(results.getClob(columnIndex));
	        case Types.BINARY:
	        case Types.VARBINARY:
	        case Types.LONGVARBINARY:
	            return typeFacility.convertToRuntimeType(results.getBytes(columnIndex));
        }
        
        return typeFacility.convertToRuntimeType(results.getObject(columnIndex));
                        
    }

    public Object retrieveValue(CallableStatement results, int parameterIndex, Class expectedType, Calendar cal, TypeFacility typeFacility) throws SQLException{
        Integer code = (Integer) TYPE_CODE_MAP.get(expectedType);
        if(code != null) {
            switch(code.intValue()) {
                case INTEGER_CODE:  {
                    int value = results.getInt(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }
                    return Integer.valueOf(value);
                }
                case LONG_CODE:  {
                    long value = results.getLong(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return Long.valueOf(value);
                }                
                case DOUBLE_CODE:  {
                    double value = results.getDouble(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return new Double(value);
                }                
                case BIGDECIMAL_CODE:  {
                    return results.getBigDecimal(parameterIndex); 
                }
                case SHORT_CODE:  {
                    short value = results.getShort(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    }                    
                    return Short.valueOf(value);
                }
                case FLOAT_CODE:  {
                    float value = results.getFloat(parameterIndex);                    
                    if(results.wasNull()) {
                        return null;
                    } 
                    return new Float(value);
                }
                case TIME_CODE: {
                    try {
                        return results.getTime(parameterIndex, cal);
                    } catch (SQLException e) {
                        //ignore
                    }
                }
                case DATE_CODE: {
                    try {
                        return results.getDate(parameterIndex, cal);
                    } catch (SQLException e) {
                        //ignore
                    }
                }
                case TIMESTAMP_CODE: {
                    try {
                        return results.getTimestamp(parameterIndex, cal);
                    } catch (SQLException e) {
                        //ignore
                    }
                }
    			case BLOB_CODE: {
    				try {
    					return typeFacility.convertToRuntimeType(results.getBlob(parameterIndex));
    				} catch (SQLException e) {
    					// ignore
    				}
    			}
    			case CLOB_CODE: {
    				try {
    					return typeFacility.convertToRuntimeType(results.getClob(parameterIndex));
    				} catch (SQLException e) {
    					// ignore
    				}
    			}
            }
        }

        // otherwise fall through and call getObject() and rely on the normal
		// translation routines
		return typeFacility.convertToRuntimeType(results.getObject(parameterIndex));
    }
}
