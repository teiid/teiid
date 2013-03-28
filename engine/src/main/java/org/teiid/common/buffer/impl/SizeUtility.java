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

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.core.types.BaseLob;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.InputStreamFactory.StorageMode;
import org.teiid.core.types.Streamable;


/**
 * Utility methods to determine the size of Java objects, particularly with 
 * respect to the Teiid runtime types.
 * 
 * The sizes are loosely based on expected heap size and are generally optimistic.
 * Actual object allocation efficiency can be quite poor.  
 */
public final class SizeUtility {
	private static final int UNKNOWN_SIZE_BYTES = 512;

	private static final class DummyOutputStream extends OutputStream {
		int bytes;

		@Override
		public void write(int arg0) throws IOException {
			bytes++;
		}
		
		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			bytes+=len;
		}
		
		public int getBytes() {
			return bytes;
		}
	}

	public static final int REFERENCE_SIZE = 8;
	
	private static Map<Class<?>, int[]> SIZE_ESTIMATES = new HashMap<Class<?>, int[]>(128);
	private static Set<Class<?>> VARIABLE_SIZE_TYPES = new HashSet<Class<?>>();
	static {
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.STRING, new int[] {100, Math.max(100, DataTypeManager.nextPowOf2(DataTypeManager.MAX_STRING_LENGTH/16))});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.VARBINARY, new int[] {100, Math.max(100, DataTypeManager.MAX_LOB_MEMORY_BYTES/32)});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.DATE, new int[] {20, 28});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.TIME, new int[] {20, 28});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.TIMESTAMP, new int[] {20, 28});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.LONG, new int[] {12, 16});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.DOUBLE, new int[] {12, 16});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.INTEGER, new int[] {6, 12});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.FLOAT, new int[] {6, 12});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.CHAR, new int[] {4, 10});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.SHORT, new int[] {4, 10});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.OBJECT, new int[] {1024, 1024});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.NULL, new int[] {0, 0});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.BYTE, new int[] {1, 1});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.BOOLEAN, new int[] {1, 1});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.BIG_INTEGER, new int[] {75, 100});
		SIZE_ESTIMATES.put(DataTypeManager.DefaultDataClasses.BIG_DECIMAL, new int[] {150, 200});
		VARIABLE_SIZE_TYPES.add(DataTypeManager.DefaultDataClasses.STRING);
		VARIABLE_SIZE_TYPES.add(DataTypeManager.DefaultDataClasses.VARBINARY);
		VARIABLE_SIZE_TYPES.add(DataTypeManager.DefaultDataClasses.OBJECT);
		VARIABLE_SIZE_TYPES.add(DataTypeManager.DefaultDataClasses.BIG_INTEGER);
		VARIABLE_SIZE_TYPES.add(DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
	}
	
	private long bigIntegerEstimate;
	private long bigDecimalEstimate;
	private Class<?>[] types;
	
	private static class ClassStats {
		AtomicInteger samples = new AtomicInteger();
		volatile int averageSize = UNKNOWN_SIZE_BYTES;
	}
	
	private static ConcurrentHashMap<String, ClassStats> objectEstimates = new ConcurrentHashMap<String, ClassStats>();
	
	public SizeUtility(Class<?>[] types) {
		boolean isValueCacheEnabled = DataTypeManager.isValueCacheEnabled();
		bigIntegerEstimate = getSize(isValueCacheEnabled, DataTypeManager.DefaultDataClasses.BIG_INTEGER);
		bigDecimalEstimate = getSize(isValueCacheEnabled, DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
		this.types = types;
	}
	
    public long getBatchSize(boolean accountForValueCache, List<? extends List<?>> data) {
        int colLength = types.length;
        int rowLength = data.size();
    
        // Array overhead for row array
        long size = 16 + alignMemory(rowLength * REFERENCE_SIZE); 
        // array overhead for all the columns ( 8 object overhead + 4 ref + 4 int)
        size += (rowLength * (48 + alignMemory(colLength * REFERENCE_SIZE))); 
        for (int col = 0; col < colLength; col++) {
            Class<?> type = types[col];
            int rowsSampled = 0;
            int estimatedSize = 0;
			if (VARIABLE_SIZE_TYPES.contains(type)) {
                for (int row = 0; row < rowLength; row=(row*2)+1) {
                	rowsSampled++;
                    estimatedSize += getSize(data.get(row).get(col), types[col], true, accountForValueCache);
                }
                size += estimatedSize/(float)rowsSampled * rowLength;
            } else {
            	size += getSize(accountForValueCache, type) * rowLength;
            }
        }
        return size;
    }
    
    static int getSize(boolean isValueCacheEnabled,
			Class<?> type) {
    	int[] vals = SIZE_ESTIMATES.get(type);
    	if (vals == null) {
			return UNKNOWN_SIZE_BYTES; //this is is misleading for lobs
			//most references are not actually removed from memory
    	}
    	return vals[isValueCacheEnabled?0:1];
	}
    
    /**
     * Get size of object
     * @return Size in bytes
     */
    protected long getSize(Object obj, Class<?> type, boolean updateEstimate, boolean accountForValueCache) {
        if(obj == null) {
            return 0;
        }

        if(obj.getClass() == DataTypeManager.DefaultDataClasses.STRING) {
            int length = ((String)obj).length();
            if (length > 0) {
                return alignMemory(40 + (2 * length));
            }
            return 40;
        } else if(obj.getClass() == DataTypeManager.DefaultDataClasses.VARBINARY) {
            int length = ((BinaryType)obj).getLength();
            if (length > 0) {
                return alignMemory(16 + length);
            }
            return 16;
        } else if(type == DataTypeManager.DefaultDataClasses.BIG_DECIMAL) {
        	if (!updateEstimate) {
        		return bigDecimalEstimate;
        	}
            int bitLength = ((BigDecimal)obj).unscaledValue().bitLength();
            //TODO: this does not account for the possibility of a cached string
            long result = 88 + alignMemory(4 + (bitLength >> 3));
        	bigDecimalEstimate = (bigDecimalEstimate + result)/2;
            return result;
        } else if(type == DataTypeManager.DefaultDataClasses.BIG_INTEGER) {
        	if (!updateEstimate) {
        		return bigIntegerEstimate;
        	}
            int bitLength = ((BigInteger)obj).bitLength();
            long result = 40 + alignMemory(4 + (bitLength >> 3));
        	bigIntegerEstimate = (bigIntegerEstimate + result)/2;
            return result;
        } else if(obj instanceof Iterable<?>) {
        	Iterable<?> i = (Iterable<?>)obj;
        	long total = 16;
        	for (Object object : i) {
				total += getSize(object, DataTypeManager.determineDataTypeClass(object), updateEstimate, false) + REFERENCE_SIZE;
			}
        	return total;
        } else if(obj.getClass().isArray()) {
        	Class<?> componentType = obj.getClass().getComponentType(); 
        	if (!componentType.isPrimitive()) {
	            Object[] rows = (Object[]) obj;
	            long total = 16 + alignMemory(rows.length * REFERENCE_SIZE); // Array overhead
	            for(int i=0; i<rows.length; i++) {
	                total += getSize(rows[i], DataTypeManager.determineDataTypeClass(rows[i]), updateEstimate, false);
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
        } else if (obj instanceof Streamable<?>) {
			try {
				Streamable<?> s = (Streamable)obj;
				Object o = s.getReference();
				if (o instanceof BaseLob) {
					InputStreamFactory isf = ((BaseLob)o).getStreamFactory();
					if (isf.getStorageMode() == StorageMode.MEMORY) {
		    			long length = isf.getLength();
		    			if (length >= 0) {
		    				return 40 + alignMemory(length);
		    			}
					} else if (isf.getStorageMode() == StorageMode.PERSISTENT) {
						long length = isf.getLength();
	    				return 40 + alignMemory(Math.min(DataTypeManager.MAX_LOB_MEMORY_BYTES, length));
					}
	    		}
			} catch (Exception e) {
			}
        } else if (type == DataTypeManager.DefaultDataClasses.OBJECT) {
        	if (obj.getClass() != DataTypeManager.DefaultDataClasses.OBJECT && (SIZE_ESTIMATES.containsKey(obj.getClass()) || VARIABLE_SIZE_TYPES.contains(obj.getClass()))) {
        		return getSize(obj, obj.getClass(), updateEstimate, accountForValueCache);
        	}
        	//assume we can get a plausable estimate from the serialized size
        	if (obj instanceof Serializable) {
            	ClassStats stats = objectEstimates.get(obj.getClass().getName()); //we're ignoring classloader differences here
            	if (stats == null) {
            		stats = new ClassStats();
            		objectEstimates.put(obj.getClass().getName(), stats);
            	}
            	if (updateEstimate) {
	            	int samples = stats.samples.getAndIncrement();
	            	if (samples < 1000 || (samples&1023) == 1023) {
	    	        	try {
	    		        	DummyOutputStream os = new DummyOutputStream();
	    		        	ObjectOutputStream oos = new ObjectOutputStream(os) {
	    		        		@Override
	    		        		protected void writeClassDescriptor(
	    		        				ObjectStreamClass desc) throws IOException {
	    		        		}
	    		        		@Override
	    		        		protected void writeStreamHeader()
	    		        				throws IOException {
	    		        		}
	    		        	};
	    		        	oos.writeObject(obj);
	    		        	oos.close();
	    		        	int result = (int)alignMemory(os.getBytes() * 3);
	    		        	if (result > stats.averageSize) {
	    		        		stats.averageSize = (stats.averageSize + result*2)/3;
	    		        	} else {
	    		        		stats.averageSize = (stats.averageSize + result)/2;
	    		        	}
	    		        	return result;
	    	        	} catch (Exception e) {
	    	        		
	    	        	}            		
	            	}
            	}
            	return stats.averageSize;
        	}
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