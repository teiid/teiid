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

package org.teiid.core.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities used by Externalizable classes to read/write objects from
 * ObjectInput/ObjectOutput instances.
 */

public class ExternalizeUtil {
    
    private ExternalizeUtil() {
    }
    
    /**
     * Writes an array to the output.
     * @param out the output instance
     * @param array reference to an array. Can be null.
     * @throws IOException
     */
    public static void writeArray(ObjectOutput out, Object[] array) throws IOException {
        if (array == null) {
            out.writeInt(0);
        } else {
            final int length = array.length;
            out.writeInt(length);
            for (int i = 0; i < length; i++) {
                out.writeObject(array[i]);
            }
        }
    }
    
    /**
     * Writes a Collection to the output using its Iterator.
     * @param out the output instance
     * @param coll reference to a Collection. Can be null.
     * @throws IOException
     */
    public static void writeCollection(ObjectOutput out, Collection<?> coll) throws IOException {
        if (coll == null) {
            out.writeInt(0);
        } else {
            final int size = coll.size();
            out.writeInt(coll.size());
            if (size > 0) {
                for (Object object : coll) {
                    out.writeObject(object);
                }
            }
        }
    }
    
    public static void writeList(ObjectOutput out, List<?> coll) throws IOException {
    	writeCollection(out, coll);
    }
    
    /**
     * Writes the key-value pairs of the given map to the output.
     * @param out the output instance
     * @param list reference to a Map. Can be null.
     * @throws IOException
     */
    public static void writeMap(ObjectOutput out, Map<?, ?> map) throws IOException {
        if (map == null) {
            out.writeInt(0);
        } else {
            out.writeInt(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                out.writeObject(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }
    }
    
    /**
     * Reads an array of String that was written to the output by this utility class
     * @param in
     * @return a non-null String[]
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
	public static <T> T[] readArray(ObjectInput in, Class<T> type) throws IOException, ClassNotFoundException {
        final int length = in.readInt();
        T[] result = (T[])Array.newInstance(type, length);
        for (int i = 0; i < length; i++) {
        	result[i] = type.cast(in.readObject());
        }
        return result;
    }
    
    public static String[] readStringArray(ObjectInput in) throws IOException, ClassNotFoundException {
    	return readArray(in, String.class);
    }
    
    /**
     * Reads a List that was written by this utility class.
     * @param in
     * @return a non-null List
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static <T> List<T> readList(ObjectInput in, Class<T> type) throws IOException, ClassNotFoundException {
        return Arrays.asList(readArray(in, type));
    }
    
    public static List<?> readList(ObjectInput in) throws IOException, ClassNotFoundException {
    	return readList(in, Object.class);
    }
    
    /**
     * Reads a Map that was written by this utility class
     * @param in
     * @return a non-null Map
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
	public static <K, V> Map<K, V> readMap(ObjectInput in) throws IOException, ClassNotFoundException {
        final int size = in.readInt();
        HashMap<K, V> map = new HashMap<K, V>(size);
        for (int i = 0; i < size; i++) {
            map.put((K)in.readObject(), (V)in.readObject());
        }
        return map;
    }
    
}
