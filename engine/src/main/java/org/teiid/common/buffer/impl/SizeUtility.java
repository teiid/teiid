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

package org.teiid.common.buffer.impl;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.types.DataTypeManager;


/**
 * Utility methods to determine the size of Java objects, particularly with 
 * respect to the Teiid runtime types.
 * 
 * The sizes are loosely based on expected heap size and are generally optimistic.
 * Actual object allocation efficiency can be quite poor.  
 */
public final class SizeUtility {
	public static final int REFERENCE_SIZE = 8;
	
	private long bigIntegerEstimate;
	private long bigDecimalEstimate;
	
	public SizeUtility() {
		boolean isValueCacheEnabled = DataTypeManager.isValueCacheEnabled();
		bigIntegerEstimate = getSize(isValueCacheEnabled, DataTypeManager.DefaultDataClasses.BIG_INTEGER);
		bigDecimalEstimate = getSize(isValueCacheEnabled, DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
	}
	
    public long getBatchSize(TupleBatch data) {
    	return getBatchSize(DataTypeManager.isValueCacheEnabled(), data);
    }
	
    private long getBatchSize(boolean accountForValueCache, TupleBatch data) {
        int colLength = data.getDataTypes().length;
        int rowLength = data.getRowCount();
    
        // Array overhead for row array
        long size = 16 + alignMemory(rowLength * REFERENCE_SIZE); 
        // array overhead for all the columns ( 8 object overhead + 4 ref + 4 int)
        size += (rowLength * (48 + alignMemory(colLength * REFERENCE_SIZE))); 
        for (int col = 0; col < colLength; col++) {
            Class<?> type = DataTypeManager.getDataTypeClass(data.getDataTypes()[col]);
                        
            if (type == DataTypeManager.DefaultDataClasses.STRING 
            		|| type == DataTypeManager.DefaultDataClasses.OBJECT
            		|| type == DataTypeManager.DefaultDataClasses.BIG_INTEGER
            		|| type == DataTypeManager.DefaultDataClasses.BIG_DECIMAL) {
            	int estRow = 0;
                for (int row = 0; row < rowLength; row++) {
                	boolean updateEst = row == estRow;
                    size += getSize(data.getTuples().get(row).get(col), updateEst, accountForValueCache);
                    if (updateEst) {
                    	estRow = estRow * 2 + 1;
                    }
                }
            } else {
            	size += getSize(accountForValueCache, type) * rowLength;
            }
        }
        return size;
    }
    
    static int getSize(boolean isValueCacheEnabled,
			Class<?> type) {
		if (type == DataTypeManager.DefaultDataClasses.STRING) {
			return isValueCacheEnabled?100:256; //assumes an "average" string length of approximately 100 chars
		} else if (type == DataTypeManager.DefaultDataClasses.DATE 
				|| type == DataTypeManager.DefaultDataClasses.TIME 
				|| type == DataTypeManager.DefaultDataClasses.TIMESTAMP) {
			return isValueCacheEnabled?20:28;
		} else if (type == DataTypeManager.DefaultDataClasses.LONG 
				|| type	 == DataTypeManager.DefaultDataClasses.DOUBLE) {
			return isValueCacheEnabled?12:16;
		} else if (type == DataTypeManager.DefaultDataClasses.INTEGER 
				|| type == DataTypeManager.DefaultDataClasses.FLOAT) {
			return isValueCacheEnabled?6:12;
		} else if (type == DataTypeManager.DefaultDataClasses.CHAR 
				|| type == DataTypeManager.DefaultDataClasses.SHORT) {
			return isValueCacheEnabled?4:10;
		} else if (type == DataTypeManager.DefaultDataClasses.OBJECT) {
			return 1024;
		} else if (type == DataTypeManager.DefaultDataClasses.NULL) {
			return 0; //it's free
		} else if (type == DataTypeManager.DefaultDataClasses.BYTE
				|| type == DataTypeManager.DefaultDataClasses.BOOLEAN) {
			return 1; //should always be value cached, but there's a small chance it's not
		} else if (type == DataTypeManager.DefaultDataClasses.BIG_INTEGER){
			return isValueCacheEnabled?75:100;
		} else if (type == DataTypeManager.DefaultDataClasses.BIG_DECIMAL) {
		 	return isValueCacheEnabled?150:200;
		}
		return 512; //this is is misleading for lobs
		//most references are not actually removed from memory
	}
    
    /**
     * Get size of object
     * @return Size in bytes
     */
    protected long getSize(Object obj, boolean updateEstimate, boolean accountForValueCache) {
        if(obj == null) {
            return 0;
        }

        Class<?> type = DataTypeManager.determineDataTypeClass(obj);
        if(type == DataTypeManager.DefaultDataClasses.STRING) {
            int length = ((String)obj).length();
            if (length > 0) {
                return alignMemory(40 + (2 * length));
            }
            return 40;
        } else if(obj instanceof Iterable<?>) {
        	Iterable<?> i = (Iterable<?>)obj;
        	long total = 16;
        	for (Object object : i) {
				total += getSize(object, true, false) + REFERENCE_SIZE;
			}
        	return total;
        } else if(type == DataTypeManager.DefaultDataClasses.BIG_DECIMAL) {
        	if (!updateEstimate) {
        		return bigDecimalEstimate;
        	}
            int bitLength = ((BigDecimal)obj).unscaledValue().bitLength();
            //TODO: this does not account for the possibility of a cached string
            long result = 88 + alignMemory(4 + (bitLength >> 3));
            if (updateEstimate) {
            	bigDecimalEstimate = (bigDecimalEstimate + result)/2;
            }
            return result;
        } else if(type == DataTypeManager.DefaultDataClasses.BIG_INTEGER) {
        	if (!updateEstimate) {
        		return bigIntegerEstimate;
        	}
            int bitLength = ((BigInteger)obj).bitLength();
            long result = 40 + alignMemory(4 + (bitLength >> 3));
            if (updateEstimate) {
            	bigIntegerEstimate = (bigIntegerEstimate + result)/2;
            }
            return result;
        } else if(obj.getClass().isArray()) {
        	Class<?> componentType = obj.getClass().getComponentType(); 
        	if (!componentType.isPrimitive()) {
	            Object[] rows = (Object[]) obj;
	            long total = 16 + alignMemory(rows.length * REFERENCE_SIZE); // Array overhead
	            for(int i=0; i<rows.length; i++) {
	                total += getSize(rows[i], true, false);
	            }
	            return total;
        	}
        	int length = Array.getLength(obj);
        	int primitiveSize = 8;
        	if (componentType == boolean.class) {
        		primitiveSize = 4;
        	} else if (componentType == byte.class) {
        		primitiveSize = 1;
        	} else if (componentType == short.class) {
        		primitiveSize = 2;
        	} else if (componentType == int.class || componentType == float.class) {
        		primitiveSize = 4;
        	}
        	return alignMemory(length * primitiveSize) + 16;
        }        	
		return getSize(accountForValueCache, type);
    }
    
    /**
     * Most current VMs have memory alignment that places objects into heap space that is a multiple of 8 Bytes.
     * This utility method helps with calculating the aligned size of an object.
     * @param numBytes
     * @return
     * @since 4.2
     */
    private static long alignMemory(long numBytes) {
        long remainder = numBytes % 8;
        if (remainder != 0) {
            numBytes += (8 - remainder);
        }
        return numBytes;
    }
    
}