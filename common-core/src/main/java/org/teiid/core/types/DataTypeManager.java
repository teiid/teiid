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

package org.teiid.core.types;

import java.lang.ref.WeakReference;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

import org.teiid.core.CorePlugin;
import org.teiid.core.types.basic.*;
import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.core.util.PropertiesUtils;


/**
 * <p>
 * This class manages data type, conversions between data types, and comparators
 * for data types. In the future other data type information may be managed
 * here.
 *
 *
 * <p>
 * In general, methods are provided to refer to types either by Class, or by
 * Class name. The benefit of the Class name option is that the user does not
 * need to load the Class object, which may not be in the classpath. The
 * advantage of the Class option is speed.
 *
 *
 * TODO: refactor the string/class/code into an enum
 */
public class DataTypeManager {

    public static final String ARRAY_SUFFIX = "[]"; //$NON-NLS-1$
    public static final boolean USE_VALUE_CACHE = PropertiesUtils.getHierarchicalProperty("org.teiid.useValueCache", false, Boolean.class); //$NON-NLS-1$
    private static final boolean COMPARABLE_LOBS = PropertiesUtils.getHierarchicalProperty("org.teiid.comparableLobs", false, Boolean.class); //$NON-NLS-1$
    private static final boolean COMPARABLE_OBJECT = PropertiesUtils.getHierarchicalProperty("org.teiid.comparableObject", false, Boolean.class); //$NON-NLS-1$
    public static final boolean PAD_SPACE = PropertiesUtils.getHierarchicalProperty("org.teiid.padSpace", false, Boolean.class); //$NON-NLS-1$
    public static final String DEFAULT_COLLATION = "UCS-2"; //$NON-NLS-1$
    public static final String COLLATION_LOCALE = PropertiesUtils.getHierarchicalProperty("org.teiid.collationLocale", null); //$NON-NLS-1$

    private static boolean valueCacheEnabled = USE_VALUE_CACHE;

    private interface ValueCache<T> {
        T getValue(T value);
    }

    private static class HashedValueCache<T> implements ValueCache<T> {

        final Object[] cache;

        HashedValueCache(int size) {
            cache = new Object[1 << size];
        }

        @SuppressWarnings("unchecked")
        public T getValue(T value) {
            int index = hash(primaryHash(value)) & (cache.length - 1);
            Object canonicalValue = get(index);
            if (value.equals(canonicalValue)) {
                return (T)canonicalValue;
            }
            set(index, value);
            return value;
        }

        protected Object get(int index) {
            return cache[index];
        }

        protected void set(int index, T value) {
            cache[index] = value;
        }

        protected int primaryHash(T value) {
            return value.hashCode();
        }

        /*
         * The same power of 2 hash bucketing from the Java HashMap
         */
        final static int hash(int h) {
            h ^= (h >>> 20) ^ (h >>> 12);
            return h ^= (h >>> 7) ^ (h >>> 4);
        }
    }

    public static class WeakReferenceHashedValueCache<T> extends HashedValueCache<T> {

        public WeakReferenceHashedValueCache(int size) {
            super(size);
        }

        public T getByHash(Object obj) {
            int index = hash(obj.hashCode()) & (cache.length - 1);
            return get(index);
        }

        @Override
        protected T get(int index) {
            WeakReference<T> ref = (WeakReference<T>) cache[index];
            if (ref != null) {
                T result = ref.get();
                if (result == null) {
                    cache[index] = null;
                }
                return result;
            }
            return null;
        }

        @Override
        protected void set(int index, T value) {
            cache[index] = new WeakReference<T>(value);
        }

    }

    private static Map<Class<?>, ValueCache<?>> valueMaps = new HashMap<Class<?>, ValueCache<?>>(128);
    private static HashedValueCache<String> stringCache = new WeakReferenceHashedValueCache<String>(17) {

        @Override
        protected int primaryHash(String value) {
            if (value.length() < 14) {
                return value.hashCode();
            }
            return HashCodeUtil.expHashCode(value);
        }
    };

    public static final int MAX_STRING_LENGTH = PropertiesUtils.getHierarchicalProperty("org.teiid.maxStringLength", 4000, Integer.class); //$NON-NLS-1$
    public static final int MAX_VARBINARY_BYTES = Math.max(nextPowOf2(2*MAX_STRING_LENGTH), 1<<13);
    public static final int MAX_LOB_MEMORY_BYTES = Math.max(nextPowOf2(8*MAX_STRING_LENGTH), 1<<15);

    public static int nextPowOf2(int val) {
        int result = 1;
        while (result < val) {
            result <<= 1;
        }
        return result;
    }

    public static final class DataTypeAliases {
        public static final String VARCHAR = "varchar"; //$NON-NLS-1$
        public static final String TINYINT = "tinyint"; //$NON-NLS-1$
        public static final String SMALLINT = "smallint"; //$NON-NLS-1$
        public static final String BIGINT = "bigint"; //$NON-NLS-1$
        public static final String REAL = "real"; //$NON-NLS-1$
        public static final String DECIMAL = "decimal"; //$NON-NLS-1$
    }

    public static final class DefaultDataTypes {
        public static final String STRING = "string"; //$NON-NLS-1$
        public static final String BOOLEAN = "boolean"; //$NON-NLS-1$
        public static final String BYTE = "byte"; //$NON-NLS-1$
        public static final String SHORT = "short"; //$NON-NLS-1$
        public static final String CHAR = "char"; //$NON-NLS-1$
        public static final String INTEGER = "integer"; //$NON-NLS-1$
        public static final String LONG = "long"; //$NON-NLS-1$
        public static final String BIG_INTEGER = "biginteger"; //$NON-NLS-1$
        public static final String FLOAT = "float"; //$NON-NLS-1$
        public static final String DOUBLE = "double"; //$NON-NLS-1$
        public static final String BIG_DECIMAL = "bigdecimal"; //$NON-NLS-1$
        public static final String DATE = "date"; //$NON-NLS-1$
        public static final String TIME = "time"; //$NON-NLS-1$
        public static final String TIMESTAMP = "timestamp"; //$NON-NLS-1$
        public static final String OBJECT = "object"; //$NON-NLS-1$
        public static final String NULL = "null"; //$NON-NLS-1$
        public static final String BLOB = "blob"; //$NON-NLS-1$
        public static final String CLOB = "clob"; //$NON-NLS-1$
        public static final String XML = "xml"; //$NON-NLS-1$
        public static final String VARBINARY = "varbinary"; //$NON-NLS-1$
        public static final String GEOMETRY = "geometry"; //$NON-NLS-1$
        public static final String GEOGRAPHY = "geography"; //$NON-NLS-1$
        public static final String JSON = "json"; //$NON-NLS-1$
    }

    public static final class DefaultDataClasses {
        public static final Class<String> STRING = String.class;
        public static final Class<Boolean> BOOLEAN = Boolean.class;
        public static final Class<Byte> BYTE = Byte.class;
        public static final Class<Short> SHORT = Short.class;
        public static final Class<Character> CHAR = Character.class;
        public static final Class<Integer> INTEGER = Integer.class;
        public static final Class<Long> LONG = Long.class;
        public static final Class<BigInteger> BIG_INTEGER = BigInteger.class;
        public static final Class<Float> FLOAT = Float.class;
        public static final Class<Double> DOUBLE = Double.class;
        public static final Class<BigDecimal> BIG_DECIMAL = BigDecimal.class;
        public static final Class<java.sql.Date> DATE = java.sql.Date.class;
        public static final Class<Time> TIME = Time.class;
        public static final Class<Timestamp> TIMESTAMP = Timestamp.class;
        public static final Class<Object> OBJECT = Object.class;
        public static final Class<NullType> NULL = NullType.class;
        public static final Class<BlobType> BLOB = BlobType.class;
        public static final Class<ClobType> CLOB = ClobType.class;
        public static final Class<XMLType> XML = XMLType.class;
        public static final Class<BinaryType> VARBINARY = BinaryType.class;
        public static final Class<GeometryType> GEOMETRY = GeometryType.class;
        public static final Class<GeographyType> GEOGRAPHY = GeographyType.class;
        public static final Class<JsonType> JSON = JsonType.class;
    }

    public static final class DefaultTypeCodes {
        public static final int STRING = 0;
        public static final int CHAR = 1;
        public static final int BOOLEAN = 2;
        public static final int BYTE = 3;
        public static final int SHORT = 4;
        public static final int INTEGER = 5;
        public static final int LONG = 6;
        public static final int BIGINTEGER = 7;
        public static final int FLOAT = 8;
        public static final int DOUBLE = 9;
        public static final int BIGDECIMAL = 10;
        public static final int DATE = 11;
        public static final int TIME = 12;
        public static final int TIMESTAMP = 13;
        public static final int OBJECT = 14;
        public static final int BLOB = 15;
        public static final int CLOB = 16;
        public static final int XML = 17;
        public static final int NULL = 18;
        public static final int VARBINARY = 19;
        public static final int GEOMETRY = 20;
        public static final int GEOGRAPHY = 21;
        public static final int JSON = 22;
    }

    public static final int MAX_TYPE_CODE = DefaultTypeCodes.JSON;

    private static final Map<Class<?>, Integer> typeMap = new LinkedHashMap<Class<?>, Integer>(64);
    private static final List<Class<?>> typeList;

    static {
        typeMap.put(DataTypeManager.DefaultDataClasses.STRING, DefaultTypeCodes.STRING);
        typeMap.put(DataTypeManager.DefaultDataClasses.CHAR, DefaultTypeCodes.CHAR);
        typeMap.put(DataTypeManager.DefaultDataClasses.BOOLEAN, DefaultTypeCodes.BOOLEAN);
        typeMap.put(DataTypeManager.DefaultDataClasses.BYTE, DefaultTypeCodes.BYTE);
        typeMap.put(DataTypeManager.DefaultDataClasses.SHORT, DefaultTypeCodes.SHORT);
        typeMap.put(DataTypeManager.DefaultDataClasses.INTEGER, DefaultTypeCodes.INTEGER);
        typeMap.put(DataTypeManager.DefaultDataClasses.LONG, DefaultTypeCodes.LONG);
        typeMap.put(DataTypeManager.DefaultDataClasses.BIG_INTEGER, DefaultTypeCodes.BIGINTEGER);
        typeMap.put(DataTypeManager.DefaultDataClasses.FLOAT, DefaultTypeCodes.FLOAT);
        typeMap.put(DataTypeManager.DefaultDataClasses.DOUBLE, DefaultTypeCodes.DOUBLE);
        typeMap.put(DataTypeManager.DefaultDataClasses.BIG_DECIMAL, DefaultTypeCodes.BIGDECIMAL);
        typeMap.put(DataTypeManager.DefaultDataClasses.DATE, DefaultTypeCodes.DATE);
        typeMap.put(DataTypeManager.DefaultDataClasses.TIME, DefaultTypeCodes.TIME);
        typeMap.put(DataTypeManager.DefaultDataClasses.TIMESTAMP, DefaultTypeCodes.TIMESTAMP);
        typeMap.put(DataTypeManager.DefaultDataClasses.OBJECT, DefaultTypeCodes.OBJECT);
        typeMap.put(DataTypeManager.DefaultDataClasses.BLOB, DefaultTypeCodes.BLOB);
        typeMap.put(DataTypeManager.DefaultDataClasses.CLOB, DefaultTypeCodes.CLOB);
        typeMap.put(DataTypeManager.DefaultDataClasses.XML, DefaultTypeCodes.XML);
        typeMap.put(DataTypeManager.DefaultDataClasses.NULL, DefaultTypeCodes.NULL);
        typeMap.put(DataTypeManager.DefaultDataClasses.VARBINARY, DefaultTypeCodes.VARBINARY);
        typeMap.put(DataTypeManager.DefaultDataClasses.GEOMETRY, DefaultTypeCodes.GEOMETRY);
        typeMap.put(DataTypeManager.DefaultDataClasses.GEOGRAPHY, DefaultTypeCodes.GEOGRAPHY);
        typeMap.put(DataTypeManager.DefaultDataClasses.JSON, DefaultTypeCodes.JSON);
        typeList = new ArrayList<Class<?>>(typeMap.keySet());
    }

    public static int getTypeCode(Class<?> source) {
        Integer result = typeMap.get(source);
        if (result == null) {
            return DefaultTypeCodes.OBJECT;
        }
        return result;
    }

    public static Class<?> getClass(int code) {
        Class<?> result = typeList.get(code);
        if (result == null) {
            return DefaultDataClasses.OBJECT;
        }
        return result;
    }

    /**
     * Doubly-nested map of String srcType --> Map of String targetType -->
     * Transform
     */
    private static Map<String, Map<String, Transform>> transforms = new HashMap<String, Map<String, Transform>>(128);

    /** Utility to easily get Transform given srcType and targetType */
    private static Transform getTransformFromMaps(String srcType,
            String targetType) {
        Map<String, Transform> innerMap = transforms.get(srcType);
        boolean found = false;
        if (innerMap != null) {
            Transform result = innerMap.get(targetType);
            if (result != null) {
                return result;
            }
            found = true;
        }
        if (srcType.equals(targetType)) {
            return null;
        }
        if (DataTypeManager.DefaultDataTypes.OBJECT.equals(targetType)) {
            return AnyToObjectTransform.INSTANCE;
        }
        if (srcType.equals(DefaultDataTypes.NULL)) {
            return NullToAnyTransform.INSTANCE;
        }
        if (srcType.equals(DefaultDataTypes.OBJECT)) {
            return ObjectToAnyTransform.INSTANCE;
        }
        if (found) {
            //built-in type
            return null;
        }
        int sourceDims = 0;
        while (isArrayType(srcType)) {
            srcType = srcType.substring(0, srcType.length() - 2);
            sourceDims++;
        }
        int targetDims = 0;
        while(isArrayType(targetType)) {
            targetType = targetType.substring(0, targetType.length() - 2);
            targetDims++;
        }
        //go from typed[] to object[]
        if (DataTypeManager.DefaultDataTypes.OBJECT.equals(targetType) && targetDims <= sourceDims) {
            return AnyToObjectTransform.INSTANCE;
        }
        //go from object[] to typed[]
        if (DataTypeManager.DefaultDataTypes.OBJECT.equals(srcType) && targetDims >= sourceDims) {
            return ObjectToAnyTransform.INSTANCE;
        }
        //TODO: will eventually allow integer[] to long[], etc.
        return null;
    }

    /** Base data type names and classes, Type name --> Type class */
    private static Map<String, Class<?>> dataTypeNames = new LinkedHashMap<String, Class<?>>(128);

    /** Base data type names and classes, Type class --> Type name */
    private static Map<Class<?>, String> dataTypeClasses = new LinkedHashMap<Class<?>, String>(128);

    private static Map<Class<?>, Class<?>> arrayTypes = new HashMap<Class<?>, Class<?>>(128);
    private static Map<Class<?>, String> arrayTypeNames = new HashMap<Class<?>, String>(128);

    /** a set of all type names roughly ordered based upon data width */
    private static Set<String> DATA_TYPE_NAMES;

    private static Set<Class<?>> DATA_TYPE_CLASSES = Collections.unmodifiableSet(dataTypeClasses.keySet());

    // Static initializer - loads basic transforms types
    static {
        // Load default data types - not extensible yet
        loadDataTypes();

        // Load default transforms
        loadBasicTransforms();

        for (Map.Entry<String, Class<?>> entry : dataTypeNames.entrySet()) {
            Class<?> arrayType = getArrayType(entry.getValue());
            arrayTypes.put(entry.getValue(), arrayType);
            arrayTypeNames.put(arrayType, getDataTypeName(arrayType));
        }
    }

    /**
     * Constructor is private so instance creation is controlled by the class.
     */
    private DataTypeManager() {
    }

    /**
     * Add a new data type. For now this consists just of the Class - in the
     * future a data type will be a more complicated entity. This is
     * package-level for now as it is just used to add the default data types.
     *
     * @param dataType
     *         New data type defined by Class
     */
    static void addDataType(String typeName, Class<?> dataType) {
        dataTypeNames.put(typeName, dataType);
        dataTypeClasses.put(dataType, typeName);
    }

    /**
     * Get a set of all data type names.
     *
     * @return Set of data type names (String)
     */
    public static Set<String> getAllDataTypeNames() {
        return DATA_TYPE_NAMES;
    }

    public static Set<Class<?>> getAllDataTypeClasses() {
        return DATA_TYPE_CLASSES;
    }

    /**
     * Get data type class.
     * <br>IMPORTANT: only valid for default runtime types
     *
     * @param name
     *         Data type name
     * @return Data type class
     */
    public static Class<?> getDataTypeClass(String name) {
        if (name == null) {
            return DefaultDataClasses.NULL;
        }

        // Hope this is the correct case (as it will be if using the constants
        Class<?> dataTypeClass = dataTypeNames.get(name);

        // If that fails, do a lower case to make sure we match
        if (dataTypeClass == null) {
            dataTypeClass = dataTypeNames.get(name.toLowerCase());
        }

        if (dataTypeClass == null) {
            if (isArrayType(name)) {
                return getArrayType(getDataTypeClass(name.substring(0, name.length() - 2)));
            }
            dataTypeClass = DefaultDataClasses.OBJECT;
        }
        return dataTypeClass;
    }

    public static boolean isArrayType(String name) {
        return name.endsWith(ARRAY_SUFFIX);
    }

    public static String getDataTypeName(Class<?> typeClass) {
        if (typeClass == null) {
            return DefaultDataTypes.NULL;
        }
        String result = dataTypeClasses.get(typeClass);
        if (result == null) {
            if (typeClass.isArray() && !typeClass.getComponentType().isPrimitive()) {
                result = arrayTypeNames.get(typeClass);
                if (result == null) {
                    return getDataTypeName(typeClass.getComponentType()) + ARRAY_SUFFIX;
                }
                return result;
            }
            result = DefaultDataTypes.OBJECT;
        }
        return result;
    }

    /**
     * Take an object and determine the MetaMatrix data type. In most cases,
     * this is simply the class of the object. Some special cases are when the
     * value is of type Object or Null.
     */
    public static Class<?> determineDataTypeClass(Object value) {
        // Handle null case
        if (value == null) {
            return DefaultDataClasses.NULL;
        }
        Class<?> clazz = value.getClass();
        if (DATA_TYPE_CLASSES.contains(clazz)) {
            return clazz;
        }
        clazz = convertToRuntimeType(value, true).getClass();
        if (DATA_TYPE_CLASSES.contains(clazz)) {
            return clazz;
        }
        return DefaultDataClasses.OBJECT;
    }

    /**
     * Get a data value transformation between the sourceType and the
     * targetType.
     *
     * @param sourceType
     *         Incoming value type
     * @param targetType
     *         Outgoing value type
     * @return A transform if one exists, null otherwise
     */
    public static Transform getTransform(Class<?> sourceType, Class<?> targetType) {
        if (sourceType == null || targetType == null) {
            throw new IllegalArgumentException(CorePlugin.Util.getString(
                    "ERR.003.029.0002", sourceType, targetType)); //$NON-NLS-1$
        }
        return getTransformFromMaps(
                DataTypeManager.getDataTypeName(sourceType), DataTypeManager
                        .getDataTypeName(targetType));
    }

    /**
     * Get a data value transformation between the sourceType with given name
     * and the targetType of given name. The Class for source and target type
     * are not needed to do this lookup.
     *
     * @param sourceTypeName
     *         Incoming value type name
     * @param targetTypeName
     *         Outgoing value type name
     * @return A transform if one exists, null otherwise
     */
    public static Transform getTransform(String sourceTypeName,
            String targetTypeName) {
        if (sourceTypeName == null || targetTypeName == null) {
            throw new IllegalArgumentException(CorePlugin.Util.getString(
                    "ERR.003.029.0003", sourceTypeName, //$NON-NLS-1$
                    targetTypeName));
        }
        return getTransformFromMaps(sourceTypeName, targetTypeName);
    }

    /**
     * Does a transformation exist between the source and target type?
     *
     * @param sourceType
     *         Incoming value type
     * @param targetType
     *         Outgoing value type
     * @return True if a transform exists
     */
    public static boolean isTransformable(Class<?> sourceType, Class<?> targetType) {
        return getTransform(sourceType, targetType) != null;
    }

    /**
     * Does a transformation exist between the source and target type of given
     * names? The Class for source and target type are not needed to do this
     * lookup.
     *
     * @param sourceTypeName
     *         Incoming value type name
     * @param targetTypeName
     *         Outgoing value type name
     * @return True if a transform exists
     */
    public static boolean isTransformable(String sourceTypeName,
            String targetTypeName) {
        if (sourceTypeName == null || targetTypeName == null) {
            throw new IllegalArgumentException(CorePlugin.Util.getString(
                    "ERR.003.029.0003", sourceTypeName, //$NON-NLS-1$
                    targetTypeName));
        }
        return (getTransformFromMaps(sourceTypeName, targetTypeName) != null);
    }

    /**
     * Add a new transform to the known transform types.
     *
     * @param transform
     *         New transform to add
     */
    static void addTransform(Transform transform) {
        ArgCheck.isNotNull(transform);
        String sourceName = transform.getSourceTypeName();
        String targetName = transform.getTargetTypeName();

        Map<String, Transform> innerMap = transforms.get(sourceName);
        if (innerMap == null) {
            innerMap = new LinkedHashMap<String, Transform>();
            transforms.put(sourceName, innerMap);
        }
        innerMap.put(targetName, transform);
    }

    public static void getImplicitConversions(String type, Collection<String> result) {
        Map<String, Transform> innerMap = transforms.get(type);
        if (innerMap != null) {
            for (Map.Entry<String, Transform> entry : innerMap.entrySet()) {
                if (!entry.getValue().isExplicit()) {
                    result.add(entry.getKey());
                }
            }
            result.add(DefaultDataTypes.OBJECT);
            return;
        }
        String previous = DataTypeManager.DefaultDataTypes.OBJECT;
        result.add(previous);
        while (isArrayType(type)) {
            previous += ARRAY_SUFFIX;
            result.add(previous);
            type = getComponentType(type);
        }
    }

    public static boolean isImplicitConversion(String srcType, String tgtType) {
        Transform t = getTransform(srcType, tgtType);
        if (t != null) {
            return !t.isExplicit();
        }
        if (DefaultDataTypes.NULL.equals(srcType) && !DefaultDataTypes.NULL.equals(tgtType)) {
            return true;
        }
        if (DefaultDataTypes.OBJECT.equals(tgtType) && !DefaultDataTypes.OBJECT.equals(srcType)) {
            return true;
        }
        if (isArrayType(srcType) && isArrayType(tgtType)) {
            return isImplicitConversion(getComponentType(srcType), getComponentType(tgtType));
        }
        return false;
    }

    public static String getComponentType(String srcType) {
        return srcType.substring(0, srcType.length() - ARRAY_SUFFIX.length());
    }

    public static boolean isExplicitConversion(String srcType, String tgtType) {
        Transform t = getTransform(srcType, tgtType);
        if (t != null) {
            return t.isExplicit();
        }
        return false;
    }

    /**
     * Is the supplied class type a LOB based data type?
     *
     * @param type
     * @return true if yes; false otherwise
     */
    public static boolean isLOB(Class<?> type) {
        return type == DefaultDataClasses.BLOB
                || type == DefaultDataClasses.CLOB
                || type == DefaultDataClasses.XML
                || type == DefaultDataClasses.GEOMETRY
                || type == DefaultDataClasses.GEOGRAPHY
                || type == DefaultDataClasses.JSON;
    }

    public static boolean isLOB(String type) {
        return DataTypeManager.DefaultDataTypes.BLOB.equals(type)
                || DataTypeManager.DefaultDataTypes.CLOB.equals(type)
                || DataTypeManager.DefaultDataTypes.XML.equals(type)
                || DataTypeManager.DefaultDataTypes.GEOMETRY.equals(type)
                || DataTypeManager.DefaultDataTypes.GEOGRAPHY.equals(type)
                || DataTypeManager.DefaultDataTypes.JSON.equals(type);
    }

    /**
     * Load default data types.
     */
    static void loadDataTypes() {
        DataTypeManager.addDataType(DefaultDataTypes.BOOLEAN, DefaultDataClasses.BOOLEAN);
        DataTypeManager.addDataType(DefaultDataTypes.BYTE, DefaultDataClasses.BYTE);
        DataTypeManager.addDataType(DefaultDataTypes.SHORT,    DefaultDataClasses.SHORT);
        DataTypeManager.addDataType(DefaultDataTypes.CHAR, DefaultDataClasses.CHAR);
        DataTypeManager.addDataType(DefaultDataTypes.INTEGER, DefaultDataClasses.INTEGER);
        DataTypeManager.addDataType(DefaultDataTypes.LONG, DefaultDataClasses.LONG);
        DataTypeManager.addDataType(DefaultDataTypes.BIG_INTEGER, DefaultDataClasses.BIG_INTEGER);
        DataTypeManager.addDataType(DefaultDataTypes.FLOAT, DefaultDataClasses.FLOAT);
        DataTypeManager.addDataType(DefaultDataTypes.DOUBLE, DefaultDataClasses.DOUBLE);
        DataTypeManager.addDataType(DefaultDataTypes.BIG_DECIMAL, DefaultDataClasses.BIG_DECIMAL);
        DataTypeManager.addDataType(DefaultDataTypes.DATE, DefaultDataClasses.DATE);
        DataTypeManager.addDataType(DefaultDataTypes.TIME, DefaultDataClasses.TIME);
        DataTypeManager.addDataType(DefaultDataTypes.TIMESTAMP, DefaultDataClasses.TIMESTAMP);
        DataTypeManager.addDataType(DefaultDataTypes.STRING, DefaultDataClasses.STRING);
        DataTypeManager.addDataType(DefaultDataTypes.CLOB, DefaultDataClasses.CLOB);
        DataTypeManager.addDataType(DefaultDataTypes.XML, DefaultDataClasses.XML);
        DataTypeManager.addDataType(DefaultDataTypes.OBJECT, DefaultDataClasses.OBJECT);
        DataTypeManager.addDataType(DefaultDataTypes.NULL, DefaultDataClasses.NULL);
        DataTypeManager.addDataType(DefaultDataTypes.BLOB, DefaultDataClasses.BLOB);
        DataTypeManager.addDataType(DefaultDataTypes.VARBINARY, DefaultDataClasses.VARBINARY);
        DataTypeManager.addDataType(DefaultDataTypes.GEOMETRY, DefaultDataClasses.GEOMETRY);
        DataTypeManager.addDataType(DefaultDataTypes.GEOGRAPHY, DefaultDataClasses.GEOGRAPHY);
        DataTypeManager.addDataType(DefaultDataTypes.JSON, DefaultDataClasses.JSON);
        DATA_TYPE_NAMES = Collections.unmodifiableSet(new LinkedHashSet<String>(dataTypeNames.keySet()));
        dataTypeNames.put(DataTypeAliases.BIGINT, DefaultDataClasses.LONG);
        dataTypeNames.put(DataTypeAliases.DECIMAL, DefaultDataClasses.BIG_DECIMAL);
        dataTypeNames.put(DataTypeAliases.REAL, DefaultDataClasses.FLOAT);
        dataTypeNames.put(DataTypeAliases.SMALLINT, DefaultDataClasses.SHORT);
        dataTypeNames.put(DataTypeAliases.TINYINT, DefaultDataClasses.BYTE);
        dataTypeNames.put(DataTypeAliases.VARCHAR, DefaultDataClasses.STRING);

        valueMaps.put(DefaultDataClasses.BOOLEAN, new ValueCache<Boolean>() {
            @Override
            public Boolean getValue(Boolean value) {
                return Boolean.valueOf(value);
            }
        });
        valueMaps.put(DefaultDataClasses.BYTE, new ValueCache<Byte>() {
            @Override
            public Byte getValue(Byte value) {
                return Byte.valueOf(value);
            }
        });

        if (USE_VALUE_CACHE) {
            valueMaps.put(DefaultDataClasses.SHORT, new HashedValueCache<Short>(13));
            valueMaps.put(DefaultDataClasses.CHAR, new HashedValueCache<Character>(13));
            valueMaps.put(DefaultDataClasses.INTEGER, new HashedValueCache<Integer>(14));
            valueMaps.put(DefaultDataClasses.LONG, new HashedValueCache<Long>(14));
            valueMaps.put(DefaultDataClasses.BIG_INTEGER, new HashedValueCache<BigInteger>(15));
            valueMaps.put(DefaultDataClasses.FLOAT, new HashedValueCache<Float>(14));
            valueMaps.put(DefaultDataClasses.DOUBLE, new HashedValueCache<Double>(14));
            valueMaps.put(DefaultDataClasses.DATE, new HashedValueCache<Date>(14));
            valueMaps.put(DefaultDataClasses.TIME, new HashedValueCache<Time>(14));
            valueMaps.put(DefaultDataClasses.TIMESTAMP, new HashedValueCache<Timestamp>(14));
            valueMaps.put(DefaultDataClasses.BIG_DECIMAL, new WeakReferenceHashedValueCache<BigDecimal>(17));
            valueMaps.put(DefaultDataClasses.STRING, stringCache);
            valueMaps.put(DefaultDataClasses.VARBINARY, new WeakReferenceHashedValueCache<BinaryType>(17));
        }
    }

    /**
     * Load all basic {@link Transform}s into the DataTypeManager. This standard
     * set is always installed but may be overridden.
     */
    static void loadBasicTransforms() {
        DataTypeManager.addTransform(new BooleanToNumberTransform(Byte.valueOf((byte)1), Byte.valueOf((byte)0)));
        DataTypeManager.addTransform(new BooleanToNumberTransform(Short.valueOf((short)1), Short.valueOf((short)0)));
        DataTypeManager.addTransform(new BooleanToNumberTransform(Integer.valueOf(1), Integer.valueOf(0)));
        DataTypeManager.addTransform(new BooleanToNumberTransform(Long.valueOf(1), Long.valueOf(0)));
        DataTypeManager.addTransform(new BooleanToNumberTransform(BigInteger.valueOf(1), BigInteger.valueOf(0)));
        DataTypeManager.addTransform(new BooleanToNumberTransform(Float.valueOf(1), Float.valueOf(0)));
        DataTypeManager.addTransform(new BooleanToNumberTransform(Double.valueOf(1), Double.valueOf(0)));
        DataTypeManager.addTransform(new BooleanToNumberTransform(BigDecimal.valueOf(1), BigDecimal.valueOf(0)));
        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.BOOLEAN));

        DataTypeManager.addTransform(new NumberToBooleanTransform(Byte.valueOf((byte)0)));
        DataTypeManager.addTransform(new NumberToShortTransform(DefaultDataClasses.BYTE, false));
        DataTypeManager.addTransform(new NumberToIntegerTransform(DefaultDataClasses.BYTE, false));
        DataTypeManager.addTransform(new NumberToLongTransform(DefaultDataClasses.BYTE, false, false));
        DataTypeManager.addTransform(new FixedNumberToBigIntegerTransform(DefaultDataClasses.BYTE));
        DataTypeManager.addTransform(new NumberToFloatTransform(DefaultDataClasses.BYTE, false, false));
        DataTypeManager.addTransform(new NumberToDoubleTransform(DefaultDataClasses.BYTE, false, false));
        DataTypeManager.addTransform(new FixedNumberToBigDecimalTransform(DefaultDataClasses.BYTE));
        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.BYTE));

        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.CHAR));

        DataTypeManager.addTransform(new NumberToBooleanTransform(Short.valueOf((short)0)));
        DataTypeManager.addTransform(new NumberToByteTransform(DefaultDataClasses.SHORT));
        DataTypeManager.addTransform(new NumberToIntegerTransform(DefaultDataClasses.SHORT, false));
        DataTypeManager.addTransform(new NumberToLongTransform(DefaultDataClasses.SHORT, false, false));
        DataTypeManager.addTransform(new FixedNumberToBigIntegerTransform(DefaultDataClasses.SHORT));
        DataTypeManager.addTransform(new NumberToFloatTransform(DefaultDataClasses.SHORT, false, false));
        DataTypeManager.addTransform(new NumberToDoubleTransform(DefaultDataClasses.SHORT, false, false));
        DataTypeManager.addTransform(new FixedNumberToBigDecimalTransform(DefaultDataClasses.SHORT));
        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.SHORT));

        DataTypeManager.addTransform(new NumberToBooleanTransform(Integer.valueOf(0)));
        DataTypeManager.addTransform(new NumberToByteTransform(DefaultDataClasses.INTEGER));
        DataTypeManager.addTransform(new NumberToShortTransform(DefaultDataClasses.INTEGER, true));
        DataTypeManager.addTransform(new NumberToLongTransform(DefaultDataClasses.INTEGER, false, false));
        DataTypeManager.addTransform(new FixedNumberToBigIntegerTransform(DefaultDataClasses.INTEGER));
        DataTypeManager.addTransform(new NumberToFloatTransform(DefaultDataClasses.INTEGER, false, true)); //lossy, but not narrowing
        DataTypeManager.addTransform(new NumberToDoubleTransform(DefaultDataClasses.INTEGER, false, false));
        DataTypeManager.addTransform(new FixedNumberToBigDecimalTransform(DefaultDataClasses.INTEGER));
        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.INTEGER));

        DataTypeManager.addTransform(new NumberToBooleanTransform(Long.valueOf(0)));
        DataTypeManager.addTransform(new NumberToByteTransform(DefaultDataClasses.LONG));
        DataTypeManager.addTransform(new NumberToShortTransform(DefaultDataClasses.LONG, true));
        DataTypeManager.addTransform(new NumberToIntegerTransform(DefaultDataClasses.LONG, true));
        DataTypeManager.addTransform(new FixedNumberToBigIntegerTransform(DefaultDataClasses.LONG));
        DataTypeManager.addTransform(new NumberToFloatTransform(DefaultDataClasses.LONG, false, true)); //lossy, but not narrowing
        DataTypeManager.addTransform(new NumberToDoubleTransform(DefaultDataClasses.LONG, false, true)); //lossy, but not narrowing
        DataTypeManager.addTransform(new FixedNumberToBigDecimalTransform(DefaultDataClasses.LONG));
        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.LONG));

        DataTypeManager.addTransform(new NumberToBooleanTransform(BigInteger.valueOf(0)));
        DataTypeManager.addTransform(new NumberToByteTransform(DefaultDataClasses.BIG_INTEGER));
        DataTypeManager.addTransform(new NumberToShortTransform(DefaultDataClasses.BIG_INTEGER, true));
        DataTypeManager.addTransform(new NumberToIntegerTransform(DefaultDataClasses.BIG_INTEGER, true));
        DataTypeManager.addTransform(new NumberToLongTransform(DefaultDataClasses.BIG_INTEGER, true, false));
        DataTypeManager.addTransform(new NumberToFloatTransform(DefaultDataClasses.BIG_INTEGER, true, false));
        DataTypeManager.addTransform(new NumberToDoubleTransform(DefaultDataClasses.BIG_INTEGER, true, false));
        DataTypeManager.addTransform(new org.teiid.core.types.basic.BigIntegerToBigDecimalTransform());
        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.BIG_INTEGER));

        DataTypeManager.addTransform(new NumberToBooleanTransform(BigDecimal.valueOf(0)));
        DataTypeManager.addTransform(new NumberToByteTransform(DefaultDataClasses.BIG_DECIMAL));
        DataTypeManager.addTransform(new NumberToShortTransform(DefaultDataClasses.BIG_DECIMAL, true));
        DataTypeManager.addTransform(new NumberToIntegerTransform(DefaultDataClasses.BIG_DECIMAL, true));
        DataTypeManager.addTransform(new NumberToLongTransform(DefaultDataClasses.BIG_DECIMAL, true, false));
        DataTypeManager.addTransform(new org.teiid.core.types.basic.BigDecimalToBigIntegerTransform());
        DataTypeManager.addTransform(new NumberToFloatTransform(DefaultDataClasses.BIG_DECIMAL, true, false));
        DataTypeManager.addTransform(new NumberToDoubleTransform(DefaultDataClasses.BIG_DECIMAL, true, false));
        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.BIG_DECIMAL));

        DataTypeManager.addTransform(new NumberToBooleanTransform(Float.valueOf(0)));
        DataTypeManager.addTransform(new NumberToByteTransform(DefaultDataClasses.FLOAT));
        DataTypeManager.addTransform(new NumberToShortTransform(DefaultDataClasses.FLOAT, true));
        DataTypeManager.addTransform(new NumberToIntegerTransform(DefaultDataClasses.FLOAT, true));
        DataTypeManager.addTransform(new NumberToLongTransform(DefaultDataClasses.FLOAT, false, true)); //lossy, but not narrowing
        DataTypeManager.addTransform(new FloatingNumberToBigIntegerTransform(DefaultDataClasses.FLOAT));
        DataTypeManager.addTransform(new NumberToDoubleTransform(DefaultDataClasses.FLOAT, false, false));
        DataTypeManager.addTransform(new FloatingNumberToBigDecimalTransform(DefaultDataClasses.FLOAT));
        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.FLOAT));

        DataTypeManager.addTransform(new NumberToBooleanTransform(Double.valueOf(0)));
        DataTypeManager.addTransform(new NumberToByteTransform(DefaultDataClasses.DOUBLE));
        DataTypeManager.addTransform(new NumberToShortTransform(DefaultDataClasses.DOUBLE, true));
        DataTypeManager.addTransform(new NumberToIntegerTransform(DefaultDataClasses.DOUBLE, true));
        DataTypeManager.addTransform(new NumberToLongTransform(DefaultDataClasses.DOUBLE, false, true)); //lossy, but not narrowing
        DataTypeManager.addTransform(new FloatingNumberToBigIntegerTransform(DefaultDataClasses.DOUBLE));
        DataTypeManager.addTransform(new NumberToFloatTransform(DefaultDataClasses.DOUBLE, true, false));
        DataTypeManager.addTransform(new FloatingNumberToBigDecimalTransform(DefaultDataClasses.DOUBLE));
        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.DOUBLE));

        DataTypeManager.addTransform(new org.teiid.core.types.basic.DateToTimestampTransform());
        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.DATE));

        DataTypeManager.addTransform(new org.teiid.core.types.basic.TimeToTimestampTransform());
        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.TIME));

        DataTypeManager.addTransform(new org.teiid.core.types.basic.TimestampToTimeTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.TimestampToDateTransform());
        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.TIMESTAMP));

        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToBooleanTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToByteTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToShortTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToIntegerTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToLongTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToBigIntegerTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToFloatTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToDoubleTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToBigDecimalTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToTimeTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToDateTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToTimestampTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToCharacterTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToClobTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToSQLXMLTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.StringToJsonTransform());

        DataTypeManager.addTransform(new org.teiid.core.types.basic.BinaryToBlobTransform());

        DataTypeManager.addTransform(new org.teiid.core.types.basic.ClobToStringTransform());
        DataTypeManager.addTransform(new org.teiid.core.types.basic.ClobToStringTransform(DefaultDataClasses.JSON));

        DataTypeManager.addTransform(new org.teiid.core.types.basic.BlobToBinaryTransform());

        DataTypeManager.addTransform(new org.teiid.core.types.basic.SQLXMLToStringTransform());

        DataTypeManager.addTransform(new org.teiid.core.types.basic.GeographyToGeometryTransform());

        DataTypeManager.addTransform(new org.teiid.core.types.basic.JsonToClobTransform());

        DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.OBJECT) {
            @Override
            public boolean isExplicit() {
                return true;
            }
        });

    }

    /**
     * Convert the value to the probable runtime type.
     * @param allConversions if false only lob conversions will be used
     */
    public static Object convertToRuntimeType(Object value, boolean allConversions) {
        if (value == null) {
            return null;
        }
        Class<?> c = value.getClass();
        if (DATA_TYPE_CLASSES.contains(c)) {
            return value;
        }
        if (allConversions) {
            if (c == char[].class) {
                return new ClobType(ClobImpl.createClob((char[])value));
            }
            if (c == byte[].class) {
                return new BinaryType((byte[])value);
            }
            if (java.util.Date.class.isAssignableFrom(c)) {
                return new Timestamp(((java.util.Date)value).getTime());
            }
            if (Object[].class.isAssignableFrom(c)) {
                return new ArrayImpl((Object[])value);
            }
        }
        if (Clob.class.isAssignableFrom(c)) {
            return new ClobType((Clob)value);
        }
        if (Blob.class.isAssignableFrom(c)) {
            return new BlobType((Blob)value);
        }
        if (SQLXML.class.isAssignableFrom(c)) {
            return new XMLType((SQLXML)value);
        }
        return value; // "object type"
    }

    public static Class<?> getRuntimeType(Class<?> c) {
        if (c == null) {
            return DefaultDataClasses.NULL;
        }
        if (DATA_TYPE_CLASSES.contains(c)) {
            return c;
        }
        if (c == char[].class) {
            return DefaultDataClasses.CLOB;
        }
        if (c == byte[].class) {
            return DefaultDataClasses.VARBINARY;
        }
        if (java.util.Date.class.isAssignableFrom(c)) {
            return DefaultDataClasses.DATE;
        }
        if (Clob.class.isAssignableFrom(c)) {
            return DefaultDataClasses.CLOB;
        }
        if (Blob.class.isAssignableFrom(c)) {
            return DefaultDataClasses.BLOB;
        }
        if (SQLXML.class.isAssignableFrom(c)) {
            return DefaultDataClasses.XML;
        }
        if (c == ArrayImpl.class) {
            return getArrayType(DefaultDataClasses.OBJECT);
        }
        if (c.isArray()) {
            return getDataTypeClass(getDataTypeName(c));
        }
        return DefaultDataClasses.OBJECT; // "object type"
    }

    public static Object transformValue(Object value, Class<?> targetClass)
            throws TransformationException {
        if (value == null) {
            return value;
        }
        return transformValue(value, value.getClass(), targetClass);
    }

    public static Object transformValue(Object value, Class<?> sourceType,
            Class<?> targetClass) throws TransformationException {
        if (value == null || sourceType == targetClass || DefaultDataClasses.OBJECT == targetClass) {
            return value;
        }
        Transform transform = DataTypeManager.getTransform(sourceType,
                targetClass);
        if (transform == null) {
            Object[] params = new Object[] { sourceType, targetClass, value};
              throw new TransformationException(CorePlugin.Event.TEIID10076, CorePlugin.Util.gs(CorePlugin.Event.TEIID10076, params));
        }
        Object result = transform.transform(value, targetClass);
        return getCanonicalValue(result);
    }

    public static boolean isNonComparable(String type) {
        return (!COMPARABLE_OBJECT && DataTypeManager.DefaultDataTypes.OBJECT.equals(type))
            || (!COMPARABLE_LOBS && DataTypeManager.DefaultDataTypes.BLOB.equals(type))
            || (!COMPARABLE_LOBS && DataTypeManager.DefaultDataTypes.CLOB.equals(type))
            || DataTypeManager.DefaultDataTypes.JSON.equals(type)
            || DataTypeManager.DefaultDataTypes.GEOMETRY.equals(type)
            || DataTypeManager.DefaultDataTypes.GEOGRAPHY.equals(type)
            || DataTypeManager.DefaultDataTypes.XML.equals(type);
    }

    public static void setValueCacheEnabled(boolean enabled) {
        valueCacheEnabled = enabled;
    }

    public static final boolean isValueCacheEnabled() {
        return valueCacheEnabled;
    }

    @SuppressWarnings("unchecked")
    public static final <T> T getCanonicalValue(T value) {
        if (valueCacheEnabled) {
            if (value == null) {
                return null;
            }
            //TODO: this initial lookup is inefficient, since there are likely collisions
            ValueCache valueCache = valueMaps.get(value.getClass());
            if (valueCache != null) {
                value = (T)valueCache.getValue(value);
            }
        }
        return value;
    }

    public static final String getCanonicalString(String value) {
        if (value == null) {
            return null;
        }
        return stringCache.getValue(value);
    }

    public static boolean isHashable(Class<?> type) {
        return isHashable(type, PAD_SPACE, COLLATION_LOCALE);
    }

    static boolean isHashable(Class<?> type, boolean padSpace, String collationLocale) {
        if (type == null) {
            return true;
        }
        if (collationLocale != null && (type == DataTypeManager.DefaultDataClasses.STRING
                || type == DataTypeManager.DefaultDataClasses.CHAR
                || type == DataTypeManager.DefaultDataClasses.CLOB) ) {
            return false;
        }
        if (type == DataTypeManager.DefaultDataClasses.STRING
                || type == DataTypeManager.DefaultDataClasses.CLOB) {
            return !padSpace;
        }
        if (type.isArray() ) {
            return isHashable(type.getComponentType(), padSpace, collationLocale);
        }
        return !(type == DataTypeManager.DefaultDataClasses.BIG_DECIMAL
                || type == DataTypeManager.DefaultDataClasses.BLOB
                || type == DataTypeManager.DefaultDataClasses.OBJECT);
    }


    public static Class<?> getArrayType(Class<?> classType) {
        Class<?> result = arrayTypes.get(classType);
        if (result != null) {
            return result;
        }
        return Array.newInstance(classType, 0).getClass();
    }

    private static final HashSet<String> LENGTH_DATATYPES = new HashSet<String>(
            Arrays.asList(
                    DataTypeManager.DefaultDataTypes.CHAR,
                    DataTypeManager.DefaultDataTypes.CLOB,
                    DataTypeManager.DefaultDataTypes.JSON,
                    DataTypeManager.DefaultDataTypes.BLOB,
                    DataTypeManager.DefaultDataTypes.OBJECT,
                    DataTypeManager.DefaultDataTypes.XML,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.VARBINARY,
                    DataTypeManager.DefaultDataTypes.BIG_INTEGER));

    /**
     * Return true if the type may be defined with a length
     */
    public static boolean hasLength(String typeName) {
        return LENGTH_DATATYPES.contains(typeName);
    }

}
