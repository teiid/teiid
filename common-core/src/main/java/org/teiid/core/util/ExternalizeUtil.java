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
            out.writeInt(size);
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
     * @param map reference to a Map. Can be null.
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

    public static void writeEnum(ObjectOutput out, Enum<?> value) throws IOException {
        if (value == null) {
            out.writeObject(null);
        } else {
            out.writeUTF(value.name());
        }
    }

    public static <T extends Enum<T>> T readEnum(ObjectInput in, Class<T> clazz, T defaultVal) throws IOException {
        String name = in.readUTF();
        if (name == null) {
            return null;
        }
        try {
            return Enum.valueOf(clazz, name);
        } catch (IllegalArgumentException e) {
            return defaultVal;
        }
    }

}
