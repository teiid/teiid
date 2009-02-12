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

package com.metamatrix.core.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
    public static void writeCollection(ObjectOutput out, Collection coll) throws IOException {
        if (coll == null) {
            out.writeInt(0);
        } else {
            final int size = coll.size();
            if (size > 0) {
                out.writeInt(coll.size());
                for (Iterator i = coll.iterator(); i.hasNext();) {
                    out.writeObject(i.next());
                }
            }
        }
    }
    
    /**
     * Writes a List to the output using its indexes.
     * @param out the output instance
     * @param list reference to a List. Can be null.
     * @throws IOException
     */
    public static void writeList(ObjectOutput out, List list) throws IOException {
        if (list == null) {
            out.writeInt(0);
        } else {
            final int size = list.size();
            out.writeInt(size);
            for (int i = 0; i < size; i++) {
                out.writeObject(list.get(i));
            }
        }
    }
    
    /**
     * Writes the key-value pairs of the given map to the output.
     * @param out the output instance
     * @param list reference to a Map. Can be null.
     * @throws IOException
     */
    public static void writeMap(ObjectOutput out, Map map) throws IOException {
        if (map == null) {
            out.writeInt(0);
        } else {
            out.writeInt(map.size());
            Map.Entry entry = null;
            for (Iterator i = map.entrySet().iterator(); i.hasNext();) {
                entry = (Map.Entry)i.next();
                out.writeObject(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }
    }
    
    /**
     * Reads an array of String that was written to the ouput by this utility class
     * @param in
     * @return a non-null String[]
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static String[] readStringArray(ObjectInput in) throws IOException, ClassNotFoundException {
        final int length = in.readInt();
        String[] strings = new String[length];
        for (int i = 0; i < length; i++) {
            strings[i] = (String)in.readObject();
        }
        return strings;
    }
    
    /**
     * Reads a List that was written by this utility class.
     * @param in
     * @return a non-null List
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static List readList(ObjectInput in) throws IOException, ClassNotFoundException {
        final int size = in.readInt();
        Object [] array = new Object[size];
        for (int i = 0; i < size; i++) {
            array[i] = in.readObject();
        }
        return Arrays.asList(array);
    }
    
    /**
     * Reads a Map that was written by this utility class
     * @param in
     * @return a non-null Map
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static Map readMap(ObjectInput in) throws IOException, ClassNotFoundException {
        final int size = in.readInt();
        HashMap map = new HashMap(size + 1, 1);
        for (int i = 0; i < size; i++) {
            map.put(in.readObject(), in.readObject());
        }
        return map;
    }
    
    /*
     * Serializing CoreException and subclasses.
     */
    public static void writeThrowable(ObjectOutput out, Throwable t) throws IOException {
        out.writeObject(t);
    }
    
    public static Throwable readThrowable(ObjectInput in) throws IOException, ClassNotFoundException {
        return (Throwable)in.readObject();
    }
    
}
