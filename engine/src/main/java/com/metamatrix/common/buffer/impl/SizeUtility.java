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

package com.metamatrix.common.buffer.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.Assertion;


/**
 * Utility methods to determine the size of Java objects, particularly with 
 * respect to the MetaMatrix runtime types.
 */
public final class SizeUtility {
    private static final int DEFAULT_BUFFER_SIZE = 4096;
	public static final boolean IS_64BIT = System.getProperty("sun.arch.data.model", "32").indexOf("64") != -1; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
//	public static final boolean IS_64BIT = true;
	public static final int REFERENCE_SIZE = IS_64BIT?8:4;
    
    
    /**
     * Constructor for SizeUtility - utility class, can't construct
     */
    private SizeUtility() {
    }
    
    /**
     * This method calculates the sizes of the tuples based on their list sizes
     * Since we know the type and length, it jsut calculates size from that 
     * information, instead of walking whole object tree. 
     * @param types - Data type for the each column
     * @param data - data 
     * @return size in total
     */
    public static long getBatchSize(String[] types, List[] data) {
        
        // If the type information is not available then, use the long route
        // and calculate each and every size.
        if (types == null) {
            return getSize(data);
        }
        
        int colLength = types.length;
        int rowLength = data.length;
    
        // Array overhead for row array
        long size = 16 + alignMemory(rowLength * REFERENCE_SIZE); 
        // array overhead for all the columns ( 8 object overhead + 4 ref + 4 int)
        size += (rowLength * (48 + alignMemory(colLength * REFERENCE_SIZE)));         
        for (int col = 0; col < colLength; col++) {
            Class type = DataTypeManager.getDataTypeClass(types[col]);
                        
            if (type == DataTypeManager.DefaultDataClasses.CHAR
                    || type == DataTypeManager.DefaultDataClasses.BOOLEAN
                    || type == DataTypeManager.DefaultDataClasses.BYTE
                    || type == DataTypeManager.DefaultDataClasses.SHORT
                    || type == DataTypeManager.DefaultDataClasses.INTEGER
                    || type == DataTypeManager.DefaultDataClasses.LONG
                    || type == DataTypeManager.DefaultDataClasses.FLOAT
                    || type == DataTypeManager.DefaultDataClasses.DOUBLE) {
            	size += (20*rowLength); 
            } else if (type == DataTypeManager.DefaultDataClasses.DATE
                    || type == DataTypeManager.DefaultDataClasses.TIME
                    || type == DataTypeManager.DefaultDataClasses.TIMESTAMP) { 
                	// Even though Timestamp contains an extra int, these are 
                	// the same size because of rounding                                                                               // though
                size += (32*rowLength);            
            } else if (type == DataTypeManager.DefaultDataClasses.NULL) {
            	//do nothing
            }
            else {
                for (int row = 0; row < rowLength; row++) {
                    size += getSize(data[row].get(col));
                }
            }
        }
        return size;
    }
    
    /**
     * Get size of object
     * @return Size in bytes
     */
    public static long getSize(Object obj) {
        if(obj == null) {
            return 0;
        }

        Class type = obj.getClass();
        // Defect 14530 - this code represents the size of the object in the heap,
        // and not necessarily the number of bytes serialized.
        if (type == DataTypeManager.DefaultDataClasses.CHAR ||
            type == DataTypeManager.DefaultDataClasses.BOOLEAN ||
            type == DataTypeManager.DefaultDataClasses.BYTE ||
            type == DataTypeManager.DefaultDataClasses.SHORT ||
            type == DataTypeManager.DefaultDataClasses.INTEGER ||
            type == DataTypeManager.DefaultDataClasses.LONG ||
            type == DataTypeManager.DefaultDataClasses.FLOAT ||
            type == DataTypeManager.DefaultDataClasses.DOUBLE) {
                return 20;
        } else if (type == DataTypeManager.DefaultDataClasses.DATE ||
                   type == DataTypeManager.DefaultDataClasses.TIME ||
                   type == DataTypeManager.DefaultDataClasses.TIMESTAMP) { // Even though Timestamp contains an extra int, these are the same size because of rounding
            return 32;
        } else if(type == DataTypeManager.DefaultDataClasses.STRING) {
            int length = ((String)obj).length();
            if (length > 0) {
                return alignMemory(40 + (2 * length));
            }
            return 40;
        } else if(obj instanceof List) {
            int total = 16; // Object overhead + 4(int modcount), rounded
            if (obj instanceof LinkedList) {
                total += 8; // 4 (Entry ref) + 4 (int size)
                for (Iterator i = ((List)obj).iterator(); i.hasNext();) {
                    total+= 16 + SizeUtility.getSize(i.next()); // rounded(4 (Object ref + 4 (next Entry ref) + 4 (previous Entry ref)) + actual object
                }
            } else if (obj instanceof ArrayList) {
                List arrayList = (ArrayList)obj;
                int arraySize = arrayList.size();
                int maxEnsuredSize = (arraySize < 10) ? 10 : arraySize * 3 / 2; // assuming default size
                total += 24 /*4(char[] ref) + 4(int) + 16 (array overhead)*/
                         +alignMemory(maxEnsuredSize * REFERENCE_SIZE); /*number of references held in the array*/                if (arraySize > 0) {
                    for (int i = 0; i < arraySize; i++) {
                        total += SizeUtility.getSize(arrayList.get(i));
                    }
                }
            } else if (obj instanceof Vector) {
                List arrayList = (List)obj;
                int arraySize = arrayList.size();
                int maxEnsuredSize = (arraySize < 10) ? 10 : arraySize * 2; // assuming default size, default capacity growth
                total += 16 // 4(Object[] ref) + 4 (int capacity) + 4 (int capIncrement), rounded
                         + alignMemory(maxEnsuredSize * REFERENCE_SIZE); // Array overhead
            } else { // Assume java.util.Arrays.ArrayList
                List list = (List)obj;
                int arraySize = list.size();
                total += 16 + alignMemory(arraySize * REFERENCE_SIZE); // For the Object[]                
                if (arraySize > 0) {
                    for (int i = 0; i < arraySize; i++) {
                        total += SizeUtility.getSize(list.get(i));
                    }
                }
            }             
            return total;
        } else if(type == DataTypeManager.DefaultDataClasses.BIG_DECIMAL) {
            int bitLength = ((BigDecimal)obj).unscaledValue().bitLength();
            return // 8     // Object overhead
                   // + 4   // BigInteger reference
                   // + 4   // int scale
                   // Not aligning the above since they are already a multiple of 8
                   // The remaining should be the same as BigInteger
                   // + 8   // object overhead
                   // + 24  // 6 int fields
                   // + 4   // int[] ref
                   // + 16 + alignMemory(4 * ((bitLength/32) + 1))   // int[] overhead
                72 + alignMemory(4 + (bitLength >> 3)); // Same as above calculation
        } else if(type == DataTypeManager.DefaultDataClasses.BIG_INTEGER) {
            int bitLength = ((BigInteger)obj).bitLength();
            return // + 8           // object overhead
                   // + 24          // 6 int fields
                   // + 4           // int[] ref
                   // + 16 + alignMemory(4 * ((bitLength/32) + 1))   // int[] overhead. BigInteger represents all values in all bases as a concatenation of chunks of 32-bits.
                56 + alignMemory(4 + (bitLength >> 3)); // Same as above calculation
        } else if(type.isArray() && !type.getComponentType().isPrimitive()) {
            Object[] rows = (Object[]) obj;
            long total = 16 + alignMemory(rows.length * REFERENCE_SIZE); // Array overhead
            for(int i=0; i<rows.length; i++) {
                total += SizeUtility.getSize(rows[i]);
            }
            return total;
        } else {
            // Unknown object
            // 8 - for object overhead
            return 8+alignMemory(4+getSerializedSize(obj)); // Assume some max value for unknown objects.
        }
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

    private static long getSerializedSize(Object anObj) {
		Assertion.assertTrue(anObj instanceof Serializable);
		try {
			ByteArrayOutputStream bout = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
			ObjectOutputStream out = new ObjectOutputStream(bout);
			out.writeObject(anObj);
			out.flush();
			long size = bout.size();
			out.close();
			bout.close();
			return size;
		} catch (IOException e) {
			// just return exception, this should not happen
			throw new MetaMatrixRuntimeException(e);
		}
	}
}