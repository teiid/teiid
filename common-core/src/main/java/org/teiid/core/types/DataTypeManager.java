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

package org.teiid.core.types;

import java.lang.ref.WeakReference;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.core.CorePlugin;
import org.teiid.core.types.basic.AnyToObjectTransform;
import org.teiid.core.types.basic.AnyToStringTransform;
import org.teiid.core.types.basic.BooleanToNumberTransform;
import org.teiid.core.types.basic.FixedNumberToBigDecimalTransform;
import org.teiid.core.types.basic.FixedNumberToBigIntegerTransform;
import org.teiid.core.types.basic.FloatingNumberToBigDecimalTransform;
import org.teiid.core.types.basic.FloatingNumberToBigIntegerTransform;
import org.teiid.core.types.basic.NullToAnyTransform;
import org.teiid.core.types.basic.NumberToBooleanTransform;
import org.teiid.core.types.basic.NumberToByteTransform;
import org.teiid.core.types.basic.NumberToDoubleTransform;
import org.teiid.core.types.basic.NumberToFloatTransform;
import org.teiid.core.types.basic.NumberToIntegerTransform;
import org.teiid.core.types.basic.NumberToLongTransform;
import org.teiid.core.types.basic.NumberToShortTransform;
import org.teiid.core.types.basic.ObjectToAnyTransform;
import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.core.util.PropertiesUtils;


/**
 * <p>
 * This class manages data type, conversions between data types, and comparators
 * for data types. In the future other data type information may be managed
 * here.
 * </p>
 * 
 * <p>
 * In general, methods are provided to refer to types either by Class, or by
 * Class name. The benefit of the Class name option is that the user does not
 * need to load the Class object, which may not be in the classpath. The
 * advantage of the Class option is speed.
 * </p>
 */
public class DataTypeManager {
	
	private static final boolean USE_VALUE_CACHE = PropertiesUtils.getBooleanProperty(System.getProperties(), "org.teiid.useValueCache", false); //$NON-NLS-1$
	
	private static boolean valueCacheEnabled;
	
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
	
	private static Map<Class<?>, ValueCache<?>> valueMaps = new HashMap<Class<?>, ValueCache<?>>(128);
	private static HashedValueCache<String> stringCache = new HashedValueCache<String>(17) {
		
		@Override
		protected Object get(int index) {
			WeakReference<?> ref = (WeakReference<?>) cache[index];
			if (ref != null) {
				return ref.get();
			}
			return null;
		}
		
		@Override
		protected void set(int index, String value) {
			cache[index] = new WeakReference<Object>(value);
		}
		
		@Override
		protected int primaryHash(String value) {
			if (value.length() < 14) {
				return value.hashCode();
			}
			return HashCodeUtil.expHashCode(value);
		}
	};

	public static final int MAX_STRING_LENGTH = 4000;
	public static final int MAX_LOB_MEMORY_BYTES = 1 << 13;
	
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
		if (innerMap != null) {
			return innerMap.get(targetType);
		}
		return null;
	}

	/** Base data type names and classes, Type name --> Type class */
	private static Map<String, Class<?>> dataTypeNames = new LinkedHashMap<String, Class<?>>(128);

	/** Base data type names and classes, Type class --> Type name */
	private static Map<Class<?>, String> dataTypeClasses = new LinkedHashMap<Class<?>, String>(128);

	private static Set<String> DATA_TYPE_NAMES;

	private static Set<Class<?>> DATA_TYPE_CLASSES = Collections.unmodifiableSet(dataTypeClasses.keySet());

	private static Map<Class<?>, SourceTransform> sourceConverters = new HashMap<Class<?>, SourceTransform>();

	// Static initializer - loads basic transforms types
	static {
		// Load default data types - not extensible yet
		loadDataTypes();

		// Load default transforms
		loadBasicTransforms();
		
		loadSourceConversions();
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
	 * 		New data type defined by Class
	 */
	static void addDataType(String typeName, Class dataType) {
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
	 * 
	 * @param name
	 * 		Data type name
	 * @return Data type class
	 */
	public static Class getDataTypeClass(String name) {
		if (name == null) {
			return DefaultDataClasses.NULL;
		}

		// Hope this is the correct case (as it will be if using the constants
		Class dataTypeClass = dataTypeNames.get(name);

		// If that fails, do a lower case to make sure we match
		if (dataTypeClass == null) {
			dataTypeClass = dataTypeNames.get(name.toLowerCase());
		}

		if (dataTypeClass == null) {
			dataTypeClass = DefaultDataClasses.OBJECT;
		}
		return dataTypeClass;
	}

	public static String getDataTypeName(Class typeClass) {
		if (typeClass == null) {
			return DefaultDataTypes.NULL;
		}

		String result = dataTypeClasses.get(typeClass);
		if (result == null) {
			result = DefaultDataTypes.OBJECT;
		}

		return result;
	}

	/**
	 * Take an object and determine the MetaMatrix data type. In most cases,
	 * this is simply the class of the object. Some special cases are when the
	 * value is of type Object or Null.
	 */
	public static Class determineDataTypeClass(Object value) {
		// Handle null case
		if (value == null) {
			return DefaultDataClasses.NULL;
		}

		return getDataTypeClass(getDataTypeName(convertToRuntimeType(value)
				.getClass()));
	}

	/**
	 * Get a data value transformation between the sourceType and the
	 * targetType.
	 * 
	 * @param sourceType
	 * 		Incoming value type
	 * @param targetType
	 * 		Outgoing value type
	 * @return A transform if one exists, null otherwise
	 */
	public static Transform getTransform(Class sourceType, Class targetType) {
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
	 * 		Incoming value type name
	 * @param targetTypeName
	 * 		Outgoing value type name
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
	 * 		Incoming value type
	 * @param targetType
	 * 		Outgoing value type
	 * @return True if a transform exists
	 */
	public static boolean isTransformable(Class sourceType, Class targetType) {
		if (sourceType == null || targetType == null) {
			throw new IllegalArgumentException(CorePlugin.Util.getString(
					"ERR.003.029.0002", sourceType, targetType)); //$NON-NLS-1$
		}
		return (getTransformFromMaps(DataTypeManager
				.getDataTypeName(sourceType), DataTypeManager
				.getDataTypeName(targetType)) != null);
	}

	/**
	 * Does a transformation exist between the source and target type of given
	 * names? The Class for source and target type are not needed to do this
	 * lookup.
	 * 
	 * @param sourceTypeName
	 * 		Incoming value type name
	 * @param targetTypeName
	 * 		Outgoing value type name
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
	 * 		New transform to add
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

	public static List<String> getImplicitConversions(String type) {
		Map<String, Transform> innerMap = transforms.get(type);
		if (innerMap != null) {
			List<String> result = new ArrayList<String>(innerMap.size());
			for (Map.Entry<String, Transform> entry : innerMap.entrySet()) {
				if (!entry.getValue().isExplicit()) {
					result.add(entry.getKey());
				}
			}
			return result;
		}
		return Collections.emptyList();
	}

	public static boolean isImplicitConversion(String srcType, String tgtType) {
		Transform t = getTransform(srcType, tgtType);
		if (t != null) {
			return !t.isExplicit();
		}
		return false;
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
		return DataTypeManager.DefaultDataClasses.BLOB.equals(type)
				|| DataTypeManager.DefaultDataClasses.CLOB.equals(type)
				|| DataTypeManager.DefaultDataClasses.XML.equals(type);
	}

	public static boolean isLOB(String type) {
		return DataTypeManager.DefaultDataTypes.BLOB.equals(type)
				|| DataTypeManager.DefaultDataTypes.CLOB.equals(type)
				|| DataTypeManager.DefaultDataTypes.XML.equals(type);
	}

	/**
	 * Load default data types.
	 */
	static void loadDataTypes() {
		DataTypeManager.addDataType(DefaultDataTypes.BOOLEAN, DefaultDataClasses.BOOLEAN);
		DataTypeManager.addDataType(DefaultDataTypes.BYTE, DefaultDataClasses.BYTE);
		DataTypeManager.addDataType(DefaultDataTypes.SHORT,	DefaultDataClasses.SHORT);
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
			valueMaps.put(DefaultDataClasses.BIG_DECIMAL, new HashedValueCache<BigDecimal>(16) {
				@Override
				protected Object get(int index) {
					WeakReference<?> ref = (WeakReference<?>) cache[index];
					if (ref != null) {
						return ref.get();
					}
					return null;
				}
				
				@Override
				protected void set(int index, BigDecimal value) {
					cache[index] = new WeakReference<BigDecimal>(value);
				}
			});
			valueMaps.put(DefaultDataClasses.STRING, stringCache);
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
		DataTypeManager.addTransform(new NumberToLongTransform(	DefaultDataClasses.DOUBLE, false, true)); //lossy, but not narrowing
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
		
		DataTypeManager.addTransform(new org.teiid.core.types.basic.ClobToStringTransform());
		
		DataTypeManager.addTransform(new org.teiid.core.types.basic.SQLXMLToStringTransform());
		
		for (Class<?> type : getAllDataTypeClasses()) {
			if (type != DefaultDataClasses.OBJECT) {
				DataTypeManager.addTransform(new AnyToObjectTransform(type));
				DataTypeManager.addTransform(new ObjectToAnyTransform(type));
			} 
			if (type != DefaultDataClasses.NULL) {
				DataTypeManager.addTransform(new NullToAnyTransform(type));
			}
		}
		
		DataTypeManager.addTransform(new AnyToStringTransform(DefaultDataClasses.OBJECT) {
			@Override
			public boolean isExplicit() {
				return true;
			}
		});

	}

	static void loadSourceConversions() {
		sourceConverters.put(Clob.class, new SourceTransform<Clob, ClobType>() {
			@Override
			public ClobType transform(Clob value) {
				return new ClobType(value);
			}
		});
		sourceConverters.put(char[].class, new SourceTransform<char[], ClobType>() {
			@Override
			public ClobType transform(char[] value) {
				return new ClobType(ClobType.createClob(value));
			}
		});
		sourceConverters.put(Blob.class, new SourceTransform<Blob, BlobType>() {
			@Override
			public BlobType transform(Blob value) {
				return new BlobType(value);
			}
		});
		addSourceTransform(byte[].class, new SourceTransform<byte[], BlobType>() {
			@Override
			public BlobType transform(byte[] value) {
				return new BlobType(BlobType.createBlob(value));
			}
		});
		addSourceTransform(SQLXML.class, new SourceTransform<SQLXML, XMLType>() {
			@Override
			public XMLType transform(SQLXML value) {
				return new XMLType(value);
			}
		});
		addSourceTransform(Date.class, new SourceTransform<Date, Timestamp>() {
			@Override
			public Timestamp transform(Date value) {
				return new Timestamp(value.getTime());
			}
		});
	}
	
	public static Object convertToRuntimeType(Object value) {
		if (value == null) {
			return null;
		}
		Class<?> c = value.getClass();
		if (getAllDataTypeClasses().contains(c)) {
			return value;
		}
		SourceTransform t = sourceConverters.get(c);
		if (t != null) {
			return t.transform(value);
		}
		for (Map.Entry<Class<?>, SourceTransform> entry : sourceConverters.entrySet()) {
			if (entry.getKey().isAssignableFrom(c)) {
				return entry.getValue().transform(value);
			}
		}
		return value; // "object type"
	}

	@SuppressWarnings("unchecked")
	public static <T> T transformValue(Object value, Class<T> targetClass)
			throws TransformationException {
		if (value == null) {
			return (T)value;
		}
		return transformValue(value, value.getClass(), targetClass);
	}

	@SuppressWarnings("unchecked")
	public static <T> T transformValue(Object value, Class sourceType,
			Class<T> targetClass) throws TransformationException {
		if (value == null || sourceType == targetClass || DefaultDataClasses.OBJECT == targetClass) {
			return (T) value;
		}
		Transform transform = DataTypeManager.getTransform(sourceType,
				targetClass);
		if (transform == null) {
            Object[] params = new Object[] { sourceType, targetClass, value};
            throw new TransformationException(CorePlugin.Util.getString("ObjectToAnyTransform.Invalid_value", params)); //$NON-NLS-1$
		}
		T result = (T) transform.transform(value);
		return getCanonicalValue(result);
	}
	
    public static boolean isNonComparable(String type) {
        return DataTypeManager.DefaultDataTypes.OBJECT.equals(type)
            || DataTypeManager.DefaultDataTypes.BLOB.equals(type)
            || DataTypeManager.DefaultDataTypes.CLOB.equals(type)
            || DataTypeManager.DefaultDataTypes.XML.equals(type);
    }
    
    public static <S> void addSourceTransform(Class<S> sourceClass, SourceTransform<S, ?> transform) {
    	sourceConverters.put(sourceClass, transform);
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
}
