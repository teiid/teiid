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

package com.metamatrix.common.properties;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;

import bsh.EvalError;
import bsh.Interpreter;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.core.util.Assertion;

/**
 * ObjectPropertyManager adds to TextManager the ability to apply a Java interpreter to evaluate property values at runtime.  This
 * means that all property values evaluate to Objects, which provides an easy means to specify complex property values.
 * <p>
 * Since the values within the resource bundles are interpreted, they must be specified using Java syntax (although trailing
 * semicolons are optional).  A special variable, "mgr", can be used within property values to reference the instance of
 * ObjectPropertyManager retrieving the value, and is intended for accessing the getter methods of that instance.
 * </p><p>
 * Notes:<ul>
 * <li>
 * Since property values are parsed by the ResourceBundle class as well as the interpreter, escape characters within a value must
 * be preceded by an additional backslash ('\').
 * </li>
 * </ul>
 * </p><p>
 * <strong>Example:</strong>
 * </p><dl>
 *     <dd><p><u>Resource Bundle (com/metamatrix/toolbox/uiDefaults.properties)</u></p></dd>
 *     <code><dl>
 *         <dd>message.key = "line1\\nline2"</dd>
 *         <dd>other.key = 3</dd>
 *         <dd>integer.property.key = mgr.get("other.key")</dd>
 *     </dl></code>
 *     <dd><p><u>Code</u></p></dd>
 *     <code><dl>
 *         <dd>public static void main() {<dl></dd>
 *             <dd>final ObjectPropertyManager propMgr = new ObjectPropertyManager("com/metamatrix/toolbox/uiDefaults");</dd>
 *             <dd>final int intProp = propMgr.getInt("integer.property.key");</dd>
 *         </dl><dd>}</dd>
 *     </dl></code>
 * </dl>
 * @see java.util.ResourceBundle
 * @since 2.1
 */
public class ObjectPropertyManager extends TextManager {
    
    //############################################################################################################################
	//# Constants                                                                                                                #
	//############################################################################################################################

    private static final String VALUE_PHRASE                        = "propertyValuePhrase"; //$NON-NLS-1$
    private static final String REGISTERED_NAME_SPACES_LIST_PHRASE  = "propertyRegisteredNamespaceListPhrase"; //$NON-NLS-1$
    private static final String KEYED_VALUE_NOT_FOUND_MESSAGE       = "keyedValueNotFoundMessage"; //$NON-NLS-1$

    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################

    /**
    Verifies that the specified value is an instance of the specified class.
    @param key              The key used to retrieve the value
    @param value            The value to verify
    @param expectedClass   The class of which the value must be an instance
    @throws ClassCastException If the value is not an instance of the specified class.
    @since 2.1
    */
    protected static void assertClass(final String key, final Object value, final Class expectedClass) {
        Assertion.isInstanceOf(value, expectedClass, CommonPlugin.Util.getString(VALUE_PHRASE, key));
    }

    /**
    Throws the specified exception as a RuntimeException.
    @param error An exception
    @since 2.1
    */
    protected static void throwRuntimeException(final Exception error) {
        if (error instanceof RuntimeException) {
            throw (RuntimeException)error;
        }
        throw new RuntimeException(error.getMessage());
    }

    //############################################################################################################################
    //# Variables                                                                                                                #
    //############################################################################################################################

    private static Interpreter interpreter = new Interpreter();

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    /**
    Creates an instance of ObjectPropertyManager with no namespaces.
    @since 2.1
    */
    public ObjectPropertyManager() {
        constructObjectPropertyManager(null, null);
    }

    /**
    Creates an instance of ObjectPropertyManager that will retrieve property values from the specified namespace.
    @param namespace The namespace identifier
    @since 2.1
    */
    public ObjectPropertyManager(final String namespace) {
        this(namespace, null);
    }

    /**
    Creates an instance of ObjectPropertyManager that will retrieve property values from the specified list of namespaces.  The name-
    spaces will be searched in ascending order, starting with the first namespace registered.
    @param namespaces The list of namespace identifiers
    @since 2.1
    */
    public ObjectPropertyManager(final String[] namespaces) {
        this(Arrays.asList(namespaces));
    }

    /**
    Creates an instance of ObjectPropertyManager that will retrieve property values from the specified list of namespaces.  The name-
    spaces will be searched in ascending order, starting with the first namespace registered.
    @param namespaces The list of namespace identifiers
    @since 2.1
    */
    public ObjectPropertyManager(final List namespaces) {
        super(namespaces);
        constructObjectPropertyManager(namespaces, null);
    }

    /**
    Creates an instance of ObjectPropertyManager that will retrieve property values from the specified namespace.  Properties from the
    resource bundle identified by the namespace will be loaded into the specified property map.
    @param namespace    The namespace identifier
    @param propertyMap  The map into which the property values are loaded
    @since 2.1
    */
    public ObjectPropertyManager(final String namespace, final Map propertyMap) {
        super(namespace, propertyMap);
        if (namespace == null) {
            Assertion.isNotNull(namespace, "Namespace"); //$NON-NLS-1$
        }
        constructObjectPropertyManager(Arrays.asList(new String[] {namespace}), propertyMap);
    }

    //############################################################################################################################
    //# Methods                                                                                                                  #
    //############################################################################################################################

    /**
    Called by each constructor to initialize the namespaces list and register any namespaces passed in the constructor.
    @param namespaces The list of namespace identifiers passed in the constructor
    @param propertyMap  The map into which the property values are loaded if a single namespace is specified
    @since 2.1
    */
    protected void constructObjectPropertyManager(final List namespaces, final Map propertyMap) {
        //interpreter = new Interpreter();
        try {
            interpreter.set("mgr", this); //$NON-NLS-1$
        } catch (final Exception err) {
            throwRuntimeException(err);
        }
    }

    /**
    @since 2.1
    */
    private boolean convertValueToBoolean(final String key, final Object value) {
        assertClass(key, value, Boolean.class);
        return ((Boolean)value).booleanValue();
    }

    /**
    @since 2.1
    */
    private byte convertValueToByte(final String key, final Object value) {
        assertClass(key, value, Number.class);
        return ((Number)value).byteValue();
    }

    /**
    @since 2.1
    */
    private char convertValueToChar(final String key, final Object value) {
        assertClass(key, value, Character.class);
        return ((Character)value).charValue();
    }

    /**
    @since 2.1
    */
    private double convertValueToDouble(final String key, final Object value) {
        assertClass(key, value, Number.class);
        return ((Number)value).doubleValue();
    }

    /**
    @since 2.1
    */
    private float convertValueToFloat(final String key, final Object value) {
        assertClass(key, value, Number.class);
        return ((Number)value).floatValue();
    }

    /**
    @since 2.1
    */
    private int convertValueToInt(final String key, final Object value) {
        assertClass(key, value, Number.class);
        return ((Number)value).intValue();
    }

    /**
    @since 2.1
    */
    private long convertValueToLong(final String key, final Object value) {
        assertClass(key, value, Number.class);
        return ((Number)value).longValue();
    }

    /**
    @since 2.1
    */
    private short convertValueToShort(final String key, final Object value) {
        assertClass(key, value, Number.class);
        return ((Number)value).shortValue();
    }

    /**
    @since 2.1
    */
    private String convertValueToString(final String key, final Object value) {
        assertClass(key, value, String.class);
        return (String)value;
    }

    /* (non-Javadoc)
	 * @see com.metamatrix.common.properties.PropertyManager#evaluateValue(java.lang.String, java.lang.String)
	 */
	protected Object evaluateValue(final String key, final String value) {
        try {
    		return interpreter.eval(value);
        } catch (final EvalError err) {
            throw new RuntimeException(CommonPlugin.Util.getString(VALUE_PHRASE, key) + ": " + err.getMessage()); //$NON-NLS-1$
        }
	}

    /**
    Retrieves the boolean value of the property identified by the specified key.
    @param key The property key
    @return The property value
    @throws MissingResourceException If the value could not be found
    @since 2.1
    */
    public boolean getBoolean(final String key) {
        return convertValueToBoolean(key, getNonNullObject(key));
    }

    /**
    Retrieves the boolean value of the property identified by the specified key, or if not found, returns the specified default
    value.
    @param key          The property key
    @param defaultValue The default value
    @return The property value, or the default value if not found
    @since 2.1
    */
    public boolean getBoolean(final String key, final boolean defaultValue) {
        final Object val = get(key);
        if (val == null) {
            return defaultValue;
        }
        return convertValueToBoolean(key, val);
    }

    /**
    Retrieves the byte value of the property identified by the specified key.
    @param key The property key
    @return The property value
    @throws MissingResourceException If the value could not be found
    @since 2.1
    */
    public byte getByte(final String key) {
        return convertValueToByte(key, getNonNullObject(key));
    }

    /**
    Retrieves the byte value of the property identified by the specified key, or if not found, returns the specified default
    value.
    @param key          The property key
    @param defaultValue The default value
    @return The property value, or the default value if not found
    @since 2.1
    */
    public byte getByte(final String key, final byte defaultValue) {
        final Object val = get(key);
        if (val == null) {
            return defaultValue;
        }
        return convertValueToByte(key, val);
    }

    /**
    Retrieves the char value of the property identified by the specified key.
    @param key The property key
    @return The property value
    @throws MissingResourceException If the value could not be found
    @since 2.1
    */
    public char getChar(final String key) {
        return convertValueToChar(key, getNonNullObject(key));
    }

    /**
    Retrieves the char value of the property identified by the specified key, or if not found, returns the specified default
    value.
    @param key          The property key
    @param defaultValue The default value
    @return The property value, or the default value if not found
    @since 2.1
    */
    public char getChar(final String key, final char defaultValue) {
        final Object val = get(key);
        if (val == null) {
            return defaultValue;
        }
        return convertValueToChar(key, val);
    }

    /**
    Retrieves the double value of the property identified by the specified key.
    @param key The property key
    @return The property value
    @throws MissingResourceException If the value could not be found
    @since 2.1
    */
    public double getDouble(final String key) {
        return convertValueToDouble(key, getNonNullObject(key));
    }

    /**
    Retrieves the double value of the property identified by the specified key, or if not found, returns the specified default
    value.
    @param key          The property key
    @param defaultValue The default value
    @return The property value, or the default value if not found
    @since 2.1
    */
    public double getDouble(final String key, final double defaultValue) {
        final Object val = get(key);
        if (val == null) {
            return defaultValue;
        }
        return convertValueToDouble(key, val);
    }

    /**
    Retrieves the float value of the property identified by the specified key.
    @param key The property key
    @return The property value
    @throws MissingResourceException If the value could not be found
    @since 2.1
    */
    public float getFloat(final String key) {
        return convertValueToFloat(key, getNonNullObject(key));
    }

    /**
    Retrieves the float value of the property identified by the specified key, or if not found, returns the specified default
    value.
    @param key          The property key
    @param defaultValue The default value
    @return The property value, or the default value if not found
    @since 2.1
    */
    public float getFloat(final String key, final float defaultValue) {
        final Object val = get(key);
        if (val == null) {
            return defaultValue;
        }
        return convertValueToFloat(key, val);
    }

    /**
    Retrieves the int value of the property identified by the specified key.
    @param key The property key
    @return The property value
    @throws MissingResourceException If the value could not be found
    @since 2.1
    */
    public int getInt(final String key) {
        return convertValueToInt(key, getNonNullObject(key));
    }

    /**
    Retrieves the int value of the property identified by the specified key, or if not found, returns the specified default
    value.
    @param key          The property key
    @param defaultValue The default value
    @return The property value, or the default value if not found
    @since 2.1
    */
    public int getInt(final String key, final int defaultValue) {
        final Object val = get(key);
        if (val == null) {
            return defaultValue;
        }
        return convertValueToInt(key, val);
    }

    /**
    Returns the Java interpreter used to evaluate poroperty values.  The interpreter can be customized, for instance, to include
    additional import statements so package names do not need to be specified in property values, or to define methods that may be
    called within a property value.
    @return The interpreter
    @since 2.1
    */
    public Interpreter getInterpreter() {
        return interpreter;
    }

    /**
    Retrieves the long value of the property identified by the specified key.
    @param key The property key
    @return The property value
    @throws MissingResourceException If the value could not be found
    @since 2.1
    */
    public long getLong(final String key) {
        return convertValueToLong(key, getNonNullObject(key));
    }

    /**
    Retrieves the long value of the property identified by the specified key, or if not found, returns the specified default
    value.
    @param key          The property key
    @param defaultValue The default value
    @return The property value, or the default value if not found
    @since 2.1
    */
    public long getLong(final String key, final long defaultValue) {
        final Object val = get(key);
        if (val == null) {
            return defaultValue;
        }
        return convertValueToLong(key, val);
    }

    /**
    Retrieves the value of the property identified by the specified key.
    @param key The property key
    @return The property value
    @throws MissingResourceException If the value could not be found
    @since 2.1
    */
    protected Object getNonNullObject(final String key) {
        final Object val = get(key);
        if (val == null) {
            final String msg =
                CommonPlugin.Util.getString(KEYED_VALUE_NOT_FOUND_MESSAGE, key, REGISTERED_NAME_SPACES_LIST_PHRASE);
            throw new MissingResourceException(msg, REGISTERED_NAME_SPACES_LIST_PHRASE, key);
        }
        return val;
    }

    /**
    Retrieves the short value of the property identified by the specified key.
    @param key The property key
    @return The property value
    @throws MissingResourceException If the value could not be found
    @since 2.1
    */
    public short getShort(final String key) {
        return convertValueToShort(key, getNonNullObject(key));
    }

    /**
    Retrieves the short value of the property identified by the specified key, or if not found, returns the specified default
    value.
    @param key          The property key
    @param defaultValue The default value
    @return The property value, or the default value if not found
    @since 2.1
    */
    public short getShort(final String key, final short defaultValue) {
        final Object val = get(key);
        if (val == null) {
            return defaultValue;
        }
        return convertValueToShort(key, val);
    }

    /**
    Retrieves the String value of the property identified by the specified key.
    @param key The property key
    @return The property value, or null if not found
    @since 2.1
    */
    public String getString(final String key) {
        return convertValueToString(key, get(key));
    }

    /**
    Retrieves the String value of the property identified by the specified key, or if not found, returns the specified default
    value.
    @param key          The property key
    @param defaultValue The default value
    @return The property value, or the default value if not found
    @since 1.0
    */
    public String getString(final String key, final String defaultValue) {
        final Object val = get(key);
        if (val == null) {
          return defaultValue;
        }
        return convertValueToString(key, val);
    }
}
