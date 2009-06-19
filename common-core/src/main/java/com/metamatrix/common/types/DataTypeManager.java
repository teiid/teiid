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

package com.metamatrix.common.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamSource;

import com.metamatrix.common.types.basic.AnyToObjectTransform;
import com.metamatrix.common.types.basic.BooleanToNumberTransform;
import com.metamatrix.common.types.basic.NullToAnyTransform;
import com.metamatrix.common.types.basic.NumberToBooleanTransform;
import com.metamatrix.common.types.basic.NumberToByteTransform;
import com.metamatrix.common.types.basic.NumberToDoubleTransform;
import com.metamatrix.common.types.basic.NumberToFloatTransform;
import com.metamatrix.common.types.basic.NumberToIntegerTransform;
import com.metamatrix.common.types.basic.NumberToLongTransform;
import com.metamatrix.common.types.basic.NumberToShortTransform;
import com.metamatrix.common.types.basic.ObjectToAnyTransform;
import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.ErrorMessageKeys;
import com.metamatrix.core.util.ArgCheck;

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

	public static final int MAX_STRING_LENGTH = 4000;

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
		/*
		 * Here we explicitly instantiate the classes, so that JRE loads the
		 * class files using the default class loader and initializes the static
		 * modifiers correctly.
		 * 
		 * Using JDK 1.4, referring to Class as "STRING = String.class" was
		 * yeilding a null, rather than a String class, however using as below
		 * did not show that side effect.
		 */
		public static final Class STRING = "".getClass(); //$NON-NLS-1$
		public static final Class BOOLEAN = (Boolean.TRUE).getClass();
		public static final Class BYTE = (new Byte((byte) 0)).getClass();
		public static final Class SHORT = (new Short((short) 0)).getClass();
		public static final Class CHAR = (new Character('a')).getClass();
		public static final Class INTEGER = (new Integer(0)).getClass();
		public static final Class LONG = (new Long(0)).getClass();
		public static final Class BIG_INTEGER = (new java.math.BigInteger("0")).getClass(); //$NON-NLS-1$
		public static final Class FLOAT = (new Float(0)).getClass();
		public static final Class DOUBLE = (new Double(0)).getClass();
		public static final Class BIG_DECIMAL = (new java.math.BigDecimal("0")).getClass(); //$NON-NLS-1$
		public static final Class DATE = (new java.sql.Date(System
				.currentTimeMillis())).getClass();
		public static final Class TIME = (new java.sql.Time(System
				.currentTimeMillis())).getClass();
		public static final Class TIMESTAMP = (new java.sql.Timestamp(System
				.currentTimeMillis())).getClass();
		public static final Class OBJECT = (new Object()).getClass();
		public static final Class NULL = (new NullType()).getClass();
		public static final Class BLOB = (new BlobType()).getClass();
		public static final Class CLOB = (new ClobType()).getClass();
		public static final Class XML = (new XMLType()).getClass();
	}

	/**
	 * Doubly-nested map of String srcType --> Map of String targetType -->
	 * Transform
	 */
	private static Map<String, Map<String, Transform>> transforms = new HashMap<String, Map<String, Transform>>();

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
	private static Map<String, Class> dataTypeNames = new LinkedHashMap<String, Class>();

	/** Base data type names and classes, Type class --> Type name */
	private static Map<Class, String> dataTypeClasses = new LinkedHashMap<Class, String>();

	private static Set<String> DATA_TYPE_NAMES = Collections
			.unmodifiableSet(dataTypeNames.keySet());

	private static Set<Class> DATA_TYPE_CLASSES = Collections
			.unmodifiableSet(dataTypeClasses.keySet());

	/**
	 * Map of a type to the list of types that type may be converted to
	 * implicitly
	 */
	private static Map<String, List<String>> implicitConversions = new HashMap<String, List<String>>();

	/**
	 * Map of a type to the list of types that type may be converted to
	 * implicitly OR explicitly
	 */
	private static Map<String, List<String>> explicitConversions = new HashMap<String, List<String>>();
	
	private static Map<Class<?>, SourceTransform> sourceConverters = new HashMap<Class<?>, SourceTransform>();

	// Static initializer - loads basic transforms types
	static {
		// Load default data types - not extensible yet
		loadDataTypes();

		// Load default transforms
		loadBasicTransforms();

		// Load implicit conversions
		loadImplicitConversions();

		// Load allowed conversions
		loadExplicitConversions();
		
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

	public static Set<Class> getAllDataTypeClasses() {
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
					ErrorMessageKeys.TYPES_ERR_0002, sourceType, targetType));
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
					ErrorMessageKeys.TYPES_ERR_0003, sourceTypeName,
					targetTypeName));
		}
		return getTransformFromMaps(sourceTypeName, targetTypeName);
	}

	/**
	 * Get the preferred data value transformation between the firstType and the
	 * secondType. The transform returned may be either from the firstType to
	 * secondType or from secondType to firstType.
	 * 
	 * @param firstType
	 * 		First value type
	 * @param secondType
	 * 		Second value type
	 * @return The preferred transform if one exists, null otherwise
	 */
	public static Transform getPreferredTransform(Class firstType,
			Class secondType) {
		if (firstType == null || secondType == null) {
			throw new IllegalArgumentException(CorePlugin.Util.getString(
					ErrorMessageKeys.TYPES_ERR_0002, firstType, secondType));
		}

		// Get transforms
		String firstName = DataTypeManager.getDataTypeName(firstType);
		String secondName = DataTypeManager.getDataTypeName(secondType);

		return DataTypeManager.getPreferredTransformHelper(firstName,
				secondName);
	}

	/**
	 * Get the preferred data value transformation between the firstType and the
	 * secondType. The transform returned may be either from the firstType to
	 * secondType or from secondType to firstType.
	 * 
	 * @param firstTypeName
	 * 		First value type
	 * @param secondTypeName
	 * 		Second value type
	 * @return The preferred transform if one exists, null otherwise
	 */
	public static Transform getPreferredTransform(String firstTypeName,
			String secondTypeName) {
		if (firstTypeName == null || secondTypeName == null) {
			throw new IllegalArgumentException(CorePlugin.Util.getString(
					ErrorMessageKeys.TYPES_ERR_0003, firstTypeName,
					secondTypeName));
		}

		return DataTypeManager.getPreferredTransformHelper(firstTypeName,
				secondTypeName);
	}

	/**
	 * Get the preferred data value transformation between the firstType and the
	 * secondType. The transform returned may be either from the firstType to
	 * secondType or from secondType to firstType.
	 * 
	 * @param firstName
	 * 		First value type name
	 * @param secondName
	 * 		Second value type name
	 * @return The preferred transform if one exists, null otherwise
	 */
	private static Transform getPreferredTransformHelper(String firstTypeName,
			String secondTypeName) {
		// Return null for identity transform
		if (firstTypeName.equals(secondTypeName)) {
			return null;
		}

		Transform t1 = getTransformFromMaps(firstTypeName, secondTypeName);
		Transform t2 = getTransformFromMaps(secondTypeName, firstTypeName);

		// Check for null transforms
		if (t1 == null) {
			// Can't choose t1 so return t2, which may be null, which is correct
			return t2;
		} else if (t2 == null) {
			// Both transforms are null
			return null;
		}

		// We know both transforms are non-null now
		// Rules for choosing:
		// 1) Cast away from a string (string to float instead of float to
		// string)
		// 2) Choose a non-narrowing over narrowing
		// 3) Choose t1 arbitrarily
		if (firstTypeName.equals(DataTypeManager.DefaultDataClasses.STRING.getName())) {
			return t1;
		} else if (secondTypeName.equals(DataTypeManager.DefaultDataClasses.STRING.getName())) {
			return t2;
		} else if (!t1.isNarrowing() && t2.isNarrowing()) {
			return t1;
		} else if (t1.isNarrowing() && !t2.isNarrowing()) {
			return t2;
		} else {
			return t1;
		}
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
					ErrorMessageKeys.TYPES_ERR_0002, sourceType, targetType));
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
					ErrorMessageKeys.TYPES_ERR_0003, sourceTypeName,
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
			innerMap = new HashMap<String, Transform>();
			transforms.put(sourceName, innerMap);
		}
		innerMap.put(targetName, transform);
	}

	static void setImplicitConversions(String type, List<String> conversions) {
		implicitConversions.put(type, conversions);
	}

	public static List<String> getImplicitConversions(String type) {
		return implicitConversions.get(type);
	}

	public static boolean isImplicitConversion(String srcType, String tgtType) {
		List<String> conversions = implicitConversions.get(srcType);
		if (conversions != null) {
			return conversions.contains(tgtType);
		}
		return false;
	}

	static void setExplicitConversions(String type, List<String> conversions) {
		explicitConversions.put(type, conversions);
	}

	public static List<String> getExplicitConversions(String type) {
		return explicitConversions.get(type);
	}

	public static boolean isExplicitConversion(String srcType, String tgtType) {
		List<String> conversions = explicitConversions.get(srcType);
		if (conversions != null) {
			return conversions.contains(tgtType);
		}
		return false;
	}

	/**
	 * Is the supplied class type a LOB based data type?
	 * 
	 * @param type
	 * @return true if yes; false otherwise
	 */
	public static boolean isLOB(Class type) {
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
		DataTypeManager.addDataType(DefaultDataTypes.BOOLEAN,
				DefaultDataClasses.BOOLEAN);
		DataTypeManager.addDataType(DefaultDataTypes.BYTE,
				DefaultDataClasses.BYTE);
		DataTypeManager.addDataType(DefaultDataTypes.SHORT,
				DefaultDataClasses.SHORT);
		DataTypeManager.addDataType(DefaultDataTypes.CHAR,
				DefaultDataClasses.CHAR);
		DataTypeManager.addDataType(DefaultDataTypes.INTEGER,
				DefaultDataClasses.INTEGER);
		DataTypeManager.addDataType(DefaultDataTypes.LONG,
				DefaultDataClasses.LONG);
		DataTypeManager.addDataType(DefaultDataTypes.BIG_INTEGER,
				DefaultDataClasses.BIG_INTEGER);
		DataTypeManager.addDataType(DefaultDataTypes.FLOAT,
				DefaultDataClasses.FLOAT);
		DataTypeManager.addDataType(DefaultDataTypes.DOUBLE,
				DefaultDataClasses.DOUBLE);
		DataTypeManager.addDataType(DefaultDataTypes.BIG_DECIMAL,
				DefaultDataClasses.BIG_DECIMAL);
		DataTypeManager.addDataType(DefaultDataTypes.DATE,
				DefaultDataClasses.DATE);
		DataTypeManager.addDataType(DefaultDataTypes.TIME,
				DefaultDataClasses.TIME);
		DataTypeManager.addDataType(DefaultDataTypes.TIMESTAMP,
				DefaultDataClasses.TIMESTAMP);
		DataTypeManager.addDataType(DefaultDataTypes.STRING,
				DefaultDataClasses.STRING);
		DataTypeManager.addDataType(DefaultDataTypes.CLOB,
				DefaultDataClasses.CLOB);
		DataTypeManager.addDataType(DefaultDataTypes.XML,
				DefaultDataClasses.XML);
		DataTypeManager.addDataType(DefaultDataTypes.OBJECT,
				DefaultDataClasses.OBJECT);
		DataTypeManager.addDataType(DefaultDataTypes.NULL,
				DefaultDataClasses.NULL);
		DataTypeManager.addDataType(DefaultDataTypes.BLOB,
				DefaultDataClasses.BLOB);
	}

	/**
	 * Load all basic {@link Transform}s into the DataTypeManager. This standard
	 * set is always installed but may be overridden.
	 */
	static void loadBasicTransforms() {
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.BigDecimalToBigIntegerTransform());
		DataTypeManager.addTransform(new NumberToBooleanTransform(BigDecimal.valueOf(1), BigDecimal.valueOf(0)));
		DataTypeManager.addTransform(new NumberToByteTransform(
				DefaultDataClasses.BIG_DECIMAL));
		DataTypeManager.addTransform(new NumberToDoubleTransform(
				DefaultDataClasses.BIG_DECIMAL, true));
		DataTypeManager.addTransform(new NumberToFloatTransform(
				DefaultDataClasses.BIG_DECIMAL, true));
		DataTypeManager.addTransform(new NumberToIntegerTransform(
				DefaultDataClasses.BIG_DECIMAL, true));
		DataTypeManager.addTransform(new NumberToLongTransform(
				DefaultDataClasses.BIG_DECIMAL, true));
		DataTypeManager.addTransform(new NumberToShortTransform(
				DefaultDataClasses.BIG_DECIMAL, true));
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.BigDecimalToStringTransform());
		
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.BigIntegerToBigDecimalTransform());
		DataTypeManager.addTransform(new NumberToBooleanTransform(BigInteger.valueOf(1), BigInteger.valueOf(0)));
		DataTypeManager.addTransform(new NumberToByteTransform(
				DefaultDataClasses.BIG_INTEGER));
		DataTypeManager.addTransform(new NumberToDoubleTransform(
				DefaultDataClasses.BIG_INTEGER, true));
		DataTypeManager.addTransform(new NumberToFloatTransform(
				DefaultDataClasses.BIG_INTEGER, true));
		DataTypeManager.addTransform(new NumberToIntegerTransform(
				DefaultDataClasses.BIG_INTEGER, true));
		DataTypeManager.addTransform(new NumberToLongTransform(
				DefaultDataClasses.BIG_INTEGER, true));
		DataTypeManager.addTransform(new NumberToShortTransform(
				DefaultDataClasses.BIG_INTEGER, true));
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.BigIntegerToStringTransform());

		DataTypeManager
				.addTransform(new BooleanToNumberTransform(BigDecimal.valueOf(1), BigDecimal.valueOf(0)));
		DataTypeManager
				.addTransform(new BooleanToNumberTransform(BigInteger.valueOf(1), BigInteger.valueOf(0)));
		DataTypeManager
				.addTransform(new BooleanToNumberTransform(Double.valueOf(1), Double.valueOf(0)));
		DataTypeManager
				.addTransform(new BooleanToNumberTransform(Float.valueOf(1), Float.valueOf(0)));
		DataTypeManager
				.addTransform(new BooleanToNumberTransform(Long.valueOf(1), Long.valueOf(0)));
		DataTypeManager
				.addTransform(new BooleanToNumberTransform(Integer.valueOf(1), Integer.valueOf(0)));
		DataTypeManager
				.addTransform(new BooleanToNumberTransform(Short.valueOf((short)1), Short.valueOf((short)0)));
		DataTypeManager
				.addTransform(new BooleanToNumberTransform(Byte.valueOf((byte)1), Byte.valueOf((byte)0)));
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.BooleanToStringTransform());

		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.ByteToBigDecimalTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.ByteToBigIntegerTransform());
		DataTypeManager.addTransform(new NumberToBooleanTransform(Byte.valueOf((byte)1), Byte.valueOf((byte)0)));
		DataTypeManager.addTransform(new NumberToDoubleTransform(
				DefaultDataClasses.BYTE, false));
		DataTypeManager.addTransform(new NumberToFloatTransform(
				DefaultDataClasses.BYTE, false));
		DataTypeManager.addTransform(new NumberToIntegerTransform(
				DefaultDataClasses.BYTE, false));
		DataTypeManager.addTransform(new NumberToLongTransform(
				DefaultDataClasses.BYTE, false));
		DataTypeManager.addTransform(new NumberToShortTransform(
				DefaultDataClasses.BYTE, false));
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.ByteToStringTransform());

		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.CharacterToStringTransform());

		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.ClobToStringTransform());

		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.DateToStringTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.DateToTimestampTransform());

		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.DoubleToBigDecimalTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.DoubleToBigIntegerTransform());
		DataTypeManager.addTransform(new NumberToBooleanTransform(Double.valueOf(1), Double.valueOf(0)));
		DataTypeManager.addTransform(new NumberToByteTransform(
				DefaultDataClasses.DOUBLE));
		DataTypeManager.addTransform(new NumberToFloatTransform(
				DefaultDataClasses.DOUBLE, true));
		DataTypeManager.addTransform(new NumberToIntegerTransform(
				DefaultDataClasses.DOUBLE, true));
		DataTypeManager.addTransform(new NumberToLongTransform(
				DefaultDataClasses.DOUBLE, true));
		DataTypeManager.addTransform(new NumberToShortTransform(
				DefaultDataClasses.DOUBLE, true));
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.DoubleToStringTransform());

		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.FloatToBigDecimalTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.FloatToBigIntegerTransform());
		DataTypeManager.addTransform(new NumberToBooleanTransform(Float.valueOf(1), Float.valueOf(0)));
		DataTypeManager.addTransform(new NumberToByteTransform(
				DefaultDataClasses.FLOAT));
		DataTypeManager.addTransform(new NumberToDoubleTransform(
				DefaultDataClasses.FLOAT, false));
		DataTypeManager.addTransform(new NumberToIntegerTransform(
				DefaultDataClasses.FLOAT, true));
		DataTypeManager.addTransform(new NumberToLongTransform(
				DefaultDataClasses.FLOAT, true));
		DataTypeManager.addTransform(new NumberToShortTransform(
				DefaultDataClasses.FLOAT, true));
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.FloatToStringTransform());

		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.IntegerToBigDecimalTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.IntegerToBigIntegerTransform());
		DataTypeManager.addTransform(new NumberToBooleanTransform(Integer.valueOf(1), Integer.valueOf(0)));
		DataTypeManager.addTransform(new NumberToByteTransform(
				DefaultDataClasses.INTEGER));
		DataTypeManager.addTransform(new NumberToDoubleTransform(
				DefaultDataClasses.INTEGER, false));
		DataTypeManager.addTransform(new NumberToFloatTransform(
				DefaultDataClasses.INTEGER, false));
		DataTypeManager.addTransform(new NumberToLongTransform(
				DefaultDataClasses.INTEGER, false));
		DataTypeManager.addTransform(new NumberToShortTransform(
				DefaultDataClasses.INTEGER, true));
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.IntegerToStringTransform());

		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.LongToBigDecimalTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.LongToBigIntegerTransform());
		DataTypeManager.addTransform(new NumberToBooleanTransform(Long.valueOf(1), Long.valueOf(0)));
		DataTypeManager.addTransform(new NumberToByteTransform(
				DefaultDataClasses.LONG));
		DataTypeManager.addTransform(new NumberToDoubleTransform(
				DefaultDataClasses.LONG, true));
		DataTypeManager.addTransform(new NumberToFloatTransform(
				DefaultDataClasses.LONG, true));
		DataTypeManager.addTransform(new NumberToIntegerTransform(
				DefaultDataClasses.LONG, true));
		DataTypeManager.addTransform(new NumberToShortTransform(
				DefaultDataClasses.LONG, true));
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.LongToStringTransform());
				
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.ShortToBigDecimalTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.ShortToBigIntegerTransform());
		DataTypeManager.addTransform(new NumberToBooleanTransform(Short.valueOf((short)1), Short.valueOf((short)0)));
		DataTypeManager.addTransform(new NumberToByteTransform(
				DefaultDataClasses.SHORT));
		DataTypeManager.addTransform(new NumberToDoubleTransform(
				DefaultDataClasses.SHORT, false));
		DataTypeManager.addTransform(new NumberToFloatTransform(
				DefaultDataClasses.SHORT, false));
		DataTypeManager.addTransform(new NumberToIntegerTransform(
				DefaultDataClasses.SHORT, false));
		DataTypeManager.addTransform(new NumberToLongTransform(
				DefaultDataClasses.SHORT, false));
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.ShortToStringTransform());

		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToBigDecimalTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToBigIntegerTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToBooleanTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToByteTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToCharacterTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToClobTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToDateTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToDoubleTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToFloatTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToIntegerTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToLongTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToShortTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToTimestampTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToTimeTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.StringToSQLXMLTransform());
		
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.SQLXMLToStringTransform());

		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.TimestampToDateTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.TimestampToStringTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.TimestampToTimeTransform());

		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.TimeToTimestampTransform());
		DataTypeManager
				.addTransform(new com.metamatrix.common.types.basic.TimeToStringTransform());
		
		for (Class<?> type : getAllDataTypeClasses()) {
			if (type != DefaultDataClasses.OBJECT) {
				DataTypeManager.addTransform(new AnyToObjectTransform(type));
				DataTypeManager.addTransform(new ObjectToAnyTransform(type));
			} 
			if (type != DefaultDataClasses.NULL) {
				DataTypeManager.addTransform(new NullToAnyTransform(type));
			}
		}

	}

	static void loadImplicitConversions() {
		DataTypeManager.setImplicitConversions(DefaultDataTypes.STRING, Arrays
				.asList(new String[] { DefaultDataTypes.CLOB,
						DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.CHAR, Arrays
				.asList(new String[] { DefaultDataTypes.STRING,
						DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.BOOLEAN, Arrays
				.asList(new String[] { DefaultDataTypes.STRING,
						DefaultDataTypes.BYTE, DefaultDataTypes.SHORT,
						DefaultDataTypes.INTEGER, DefaultDataTypes.LONG,
						DefaultDataTypes.BIG_INTEGER, DefaultDataTypes.FLOAT,
						DefaultDataTypes.DOUBLE, DefaultDataTypes.BIG_DECIMAL,
						DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.BYTE,
				Arrays
						.asList(new String[] { DefaultDataTypes.STRING,
								DefaultDataTypes.SHORT,
								DefaultDataTypes.INTEGER,
								DefaultDataTypes.LONG,
								DefaultDataTypes.BIG_INTEGER,
								DefaultDataTypes.FLOAT,
								DefaultDataTypes.DOUBLE,
								DefaultDataTypes.BIG_DECIMAL,
								DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.SHORT, Arrays
				.asList(new String[] { DefaultDataTypes.STRING,
						DefaultDataTypes.INTEGER, DefaultDataTypes.LONG,
						DefaultDataTypes.BIG_INTEGER, DefaultDataTypes.FLOAT,
						DefaultDataTypes.DOUBLE, DefaultDataTypes.BIG_DECIMAL,
						DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.INTEGER,
				Arrays
						.asList(new String[] { DefaultDataTypes.STRING,
								DefaultDataTypes.LONG,
								DefaultDataTypes.BIG_INTEGER,
								DefaultDataTypes.FLOAT,
								DefaultDataTypes.DOUBLE,
								DefaultDataTypes.BIG_DECIMAL,
								DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.LONG, Arrays
				.asList(new String[] { DefaultDataTypes.STRING,
						DefaultDataTypes.BIG_INTEGER, DefaultDataTypes.BIG_DECIMAL,
						DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.BIG_INTEGER,
				Arrays
						.asList(new String[] { DefaultDataTypes.STRING,
								DefaultDataTypes.BIG_DECIMAL,
								DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.FLOAT, Arrays
				.asList(new String[] { DefaultDataTypes.STRING,
						DefaultDataTypes.DOUBLE, DefaultDataTypes.BIG_DECIMAL,
						DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.DOUBLE,
				Arrays
						.asList(new String[] { DefaultDataTypes.STRING,
								DefaultDataTypes.BIG_DECIMAL,
								DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.BIG_DECIMAL,
				Arrays.asList(new String[] { DefaultDataTypes.STRING,
						DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.DATE, Arrays
				.asList(new String[] { DefaultDataTypes.STRING,
						DefaultDataTypes.TIMESTAMP, DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.TIME, Arrays
				.asList(new String[] { DefaultDataTypes.STRING,
						DefaultDataTypes.TIMESTAMP,
						DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.TIMESTAMP,
				Arrays.asList(new String[] { DefaultDataTypes.STRING,
						DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.OBJECT, Arrays
				.asList(new String[] {}));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.NULL, Arrays
				.asList(new String[] { DefaultDataTypes.STRING,
						DefaultDataTypes.CHAR, DefaultDataTypes.BOOLEAN,
						DefaultDataTypes.BYTE, DefaultDataTypes.SHORT,
						DefaultDataTypes.INTEGER, DefaultDataTypes.LONG,
						DefaultDataTypes.BIG_INTEGER, DefaultDataTypes.FLOAT,
						DefaultDataTypes.DOUBLE, DefaultDataTypes.BIG_DECIMAL,
						DefaultDataTypes.DATE, DefaultDataTypes.TIME,
						DefaultDataTypes.TIMESTAMP, DefaultDataTypes.OBJECT,
						DefaultDataTypes.BLOB, DefaultDataTypes.CLOB,
						DefaultDataTypes.XML }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.BLOB, Arrays
				.asList(new String[] { DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.CLOB, Arrays
				.asList(new String[] { DefaultDataTypes.OBJECT }));
		DataTypeManager.setImplicitConversions(DefaultDataTypes.XML, Arrays
				.asList(new String[] { DefaultDataTypes.OBJECT }));
	}

	static void loadExplicitConversions() {
		DataTypeManager.setExplicitConversions(DefaultDataTypes.STRING, Arrays
				.asList(new String[] { DefaultDataTypes.CHAR,
						DefaultDataTypes.BOOLEAN, DefaultDataTypes.BYTE,
						DefaultDataTypes.SHORT, DefaultDataTypes.INTEGER,
						DefaultDataTypes.LONG, DefaultDataTypes.BIG_INTEGER,
						DefaultDataTypes.FLOAT, DefaultDataTypes.DOUBLE,
						DefaultDataTypes.BIG_DECIMAL, DefaultDataTypes.DATE,
						DefaultDataTypes.TIME, DefaultDataTypes.TIMESTAMP,
						DefaultDataTypes.XML }));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.CHAR, Arrays
				.asList(new String[] {}));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.BOOLEAN, Arrays
				.asList(new String[] {}));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.BYTE, Arrays
				.asList(new String[] { DefaultDataTypes.BOOLEAN }));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.SHORT, Arrays
				.asList(new String[] { DefaultDataTypes.BOOLEAN,
						DefaultDataTypes.BYTE }));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.INTEGER, Arrays
				.asList(new String[] { DefaultDataTypes.BOOLEAN,
						DefaultDataTypes.BYTE, DefaultDataTypes.SHORT }));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.LONG, Arrays
				.asList(new String[] { DefaultDataTypes.BOOLEAN,
						DefaultDataTypes.BYTE, DefaultDataTypes.SHORT,
						DefaultDataTypes.INTEGER, DefaultDataTypes.FLOAT, 
						DefaultDataTypes.DOUBLE}));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.BIG_INTEGER,
				Arrays.asList(new String[] { DefaultDataTypes.BOOLEAN,
						DefaultDataTypes.BYTE, DefaultDataTypes.SHORT,
						DefaultDataTypes.INTEGER, DefaultDataTypes.LONG,
						DefaultDataTypes.FLOAT, DefaultDataTypes.DOUBLE }));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.FLOAT, Arrays
				.asList(new String[] { DefaultDataTypes.BOOLEAN,
						DefaultDataTypes.BYTE, DefaultDataTypes.SHORT,
						DefaultDataTypes.INTEGER, DefaultDataTypes.LONG,
						DefaultDataTypes.BIG_INTEGER }));
		DataTypeManager
				.setExplicitConversions(DefaultDataTypes.DOUBLE, Arrays
						.asList(new String[] { DefaultDataTypes.BOOLEAN,
								DefaultDataTypes.BYTE, DefaultDataTypes.SHORT,
								DefaultDataTypes.INTEGER,
								DefaultDataTypes.LONG,
								DefaultDataTypes.BIG_INTEGER,
								DefaultDataTypes.FLOAT }));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.BIG_DECIMAL,
				Arrays.asList(new String[] { DefaultDataTypes.BOOLEAN,
						DefaultDataTypes.BYTE, DefaultDataTypes.SHORT,
						DefaultDataTypes.INTEGER, DefaultDataTypes.LONG,
						DefaultDataTypes.BIG_INTEGER, DefaultDataTypes.FLOAT,
						DefaultDataTypes.DOUBLE }));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.DATE, Arrays
				.asList(new String[] {}));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.TIME, Arrays
				.asList(new String[] {}));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.TIMESTAMP,
				Arrays.asList(new String[] { DefaultDataTypes.DATE,
						DefaultDataTypes.TIME }));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.OBJECT, Arrays
				.asList(new String[] { DefaultDataTypes.STRING,
						DefaultDataTypes.CHAR, DefaultDataTypes.BOOLEAN,
						DefaultDataTypes.BYTE, DefaultDataTypes.SHORT,
						DefaultDataTypes.INTEGER, DefaultDataTypes.LONG,
						DefaultDataTypes.BIG_INTEGER, DefaultDataTypes.FLOAT,
						DefaultDataTypes.DOUBLE, DefaultDataTypes.BIG_DECIMAL,
						DefaultDataTypes.DATE, DefaultDataTypes.TIME,
						DefaultDataTypes.TIMESTAMP, DefaultDataTypes.BLOB,
						DefaultDataTypes.CLOB, DefaultDataTypes.XML }));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.NULL, Arrays
				.asList(new String[] {}));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.BLOB, Arrays
				.asList(new String[] {}));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.CLOB, Arrays
				.asList(new String[] { DefaultDataTypes.STRING }));
		DataTypeManager.setExplicitConversions(DefaultDataTypes.XML, Arrays
				.asList(new String[] { DefaultDataTypes.STRING }));
	}
	
	static void loadSourceConversions() {
		sourceConverters.put(Clob.class, new SourceTransform<Clob, ClobType>() {
			//## JDBC4.0-begin ##
			@Override
			//## JDBC4.0-end ##
			public ClobType transform(Clob value) {
				return new ClobType(value);
			}
		});
		sourceConverters.put(char[].class, new SourceTransform<char[], ClobType>() {
			//## JDBC4.0-begin ##
			@Override
			//## JDBC4.0-end ##
			public ClobType transform(char[] value) {
				return new ClobType(ClobType.createClob(value));
			}
		});
		sourceConverters.put(Blob.class, new SourceTransform<Blob, BlobType>() {
			//## JDBC4.0-begin ##
			@Override
			//## JDBC4.0-end ##
			public BlobType transform(Blob value) {
				return new BlobType(value);
			}
		});
		sourceConverters.put(byte[].class, new SourceTransform<byte[], BlobType>() {
			//## JDBC4.0-begin ##
			@Override
			//## JDBC4.0-end ##
			public BlobType transform(byte[] value) {
				return new BlobType(BlobType.createBlob(value));
			}
		});
		sourceConverters.put(SQLXML.class, new SourceTransform<SQLXML, XMLType>() {
			//## JDBC4.0-begin ##
			@Override
			//## JDBC4.0-end ##
			public XMLType transform(SQLXML value) {
				return new XMLType(value);
			}
		});
		sourceConverters.put(DOMSource.class, new SourceTransform<DOMSource, XMLType>() {
			//## JDBC4.0-begin ##
			@Override
			//## JDBC4.0-end ##
			public XMLType transform(DOMSource value) {
				return new XMLType(new SQLXMLImpl(value));
			}
		});
		sourceConverters.put(SAXSource.class, new SourceTransform<SAXSource, XMLType>() {
			//## JDBC4.0-begin ##
			@Override
			//## JDBC4.0-end ##
			public XMLType transform(SAXSource value) {
				return new XMLType(new SQLXMLImpl(value));
			}
		});
		sourceConverters.put(StreamSource.class, new SourceTransform<StreamSource, XMLType>() {
			//## JDBC4.0-begin ##
			@Override
			//## JDBC4.0-end ##
			public XMLType transform(StreamSource value) {
				return new XMLType(new SQLXMLImpl(value));
			}
		});
		sourceConverters.put(Date.class, new SourceTransform<Date, Timestamp>() {
			//## JDBC4.0-begin ##
			@Override
			//## JDBC4.0-end ##
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
		if (value == null || sourceType == targetClass) {
			return (T) value;
		}
		Transform transform = DataTypeManager.getTransform(sourceType,
				targetClass);
		if (transform == null) {
            Object[] params = new Object[] { sourceType, targetClass, value};
            throw new TransformationException(CorePlugin.Util.getString("ObjectToAnyTransform.Invalid_value", params)); //$NON-NLS-1$
		}
		return (T) transform.transform(value);
	}
	
    public static boolean isNonComparable(String type) {
        return DataTypeManager.DefaultDataTypes.OBJECT.equals(type)
            || DataTypeManager.DefaultDataTypes.BLOB.equals(type)
            || DataTypeManager.DefaultDataTypes.CLOB.equals(type)
            || DataTypeManager.DefaultDataTypes.XML.equals(type);
    }
}
