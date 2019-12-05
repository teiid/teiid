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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.teiid.core.BundleUtil;
import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidRuntimeException;



/**
 * Static utility methods for common tasks having to do with
 * java.util.Properties.
 */
public final class PropertiesUtils {

    public static class InvalidPropertyException extends TeiidRuntimeException {
        private static final long serialVersionUID = 1586068295007497776L;

        public InvalidPropertyException(BundleUtil.Event event, String propertyName, Object value, Class<?> expectedType, Throwable cause) {
            super(event, cause, CorePlugin.Util.getString("InvalidPropertyException.message", propertyName, value, expectedType.getSimpleName())); //$NON-NLS-1$
        }

    }

    /**
     * Performs a correct deep clone of the properties object by capturing
     * all properties in the default(s) and placing them directly into the
     * new Properties object.
     */
    public static Properties clone( Properties props ) {
        return clone(props, null, false);
    }

    /**
     * Performs a correct deep clone of the properties object by capturing
     * all properties in the default(s) and placing them directly into the
     * new Properties object.
     */
    public static Properties clone( Properties props, Properties defaults, boolean deepClone ) {
        Properties result = null;
        if ( defaults != null ) {
            if ( deepClone ) {
                defaults = clone(defaults);
            }
            result = new Properties(defaults);
        } else {
            result = new Properties();
        }

        putAll(result, props);

        return result;
    }

    /**
     * <p>This method is intended to replace the use of the <code>putAll</code>
     * method of <code>Properties</code> inherited from <code>java.util.Hashtable</code>.
     * The problem with that method is that, since it is inherited from
     * <code>Hashtable</code>, <i>default</i> properties are lost.
     *
     * <p>For example, the following code
     * <pre><code>
     * Properties a;
     * Properties b;
     * //initialize ...
     * a.putAll(b);
     * </code></pre>
     * will fail <i>if</i> <code>b</code> had been constructed with a default
     * <code>Properties</code> object.  Those defaults would be lost and
     * not added to <code>a</code>.
     *
     * <p>The above code could be correctly performed with this method,
     * like this:
     * <pre><code>
     * Properties a;
     * Properties b;
     * //initialize ...
     * PropertiesUtils.putAll(a,b);
     * </code></pre>
     * In the above example, <code>a</code> is modified - properties are added to
     * it (note that if <code>a</code> has defaults they will remain unaffected.)
     * The properties from <code>b</code>, <i>including defaults</i>, will be
     * added to <code>a</code> using its <code>setProperty</code> method -
     * these new properties will overwrite any pre-existing ones of the same
     * name.
     *
     *
     * @param addToThis This Properties object is modified; the properties
     * of the other parameter are added to this.  The added property values
     * will replace any current property values of the same names.
     * @param withThese The properties (including defaults) of this
     * object are added to the "addToThis" parameter.
     */
    public static void putAll(Properties addToThis,
                              Properties withThese) {
        if ( withThese != null && addToThis != null ) {
            Enumeration<?> enumeration = withThese.propertyNames();
            while ( enumeration.hasMoreElements() ) {
                String propName = (String) enumeration.nextElement();
                Object propValue = withThese.get(propName);
                if ( propValue == null ) {
                    //defaults can only be retrieved as strings
                    propValue = withThese.getProperty(propName);
                }
                if ( propValue != null ) {
                    addToThis.put(propName, propValue);
                }
            }
        }
    }

    public static int getIntProperty(Properties props, String propName, int defaultValue) throws InvalidPropertyException {
        String stringVal = props.getProperty(propName);
        if(stringVal == null) {
            return defaultValue;
        }
        stringVal = stringVal.trim();
        if (stringVal.length() == 0) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(stringVal);
        } catch(NumberFormatException e) {
              throw new InvalidPropertyException(CorePlugin.Event.TEIID10037, propName, stringVal, Integer.class, e);
        }
    }

    public static long getLongProperty(Properties props, String propName, long defaultValue) {
        String stringVal = props.getProperty(propName);
        if(stringVal == null) {
            return defaultValue;
        }
        stringVal = stringVal.trim();
        if (stringVal.length() == 0) {
            return defaultValue;
        }
        try {
            return Long.parseLong(stringVal);
        } catch(NumberFormatException e) {
              throw new InvalidPropertyException( CorePlugin.Event.TEIID10038, propName, stringVal, Long.class, e);
        }
    }

    public static float getFloatProperty(Properties props, String propName, float defaultValue) {
        String stringVal = props.getProperty(propName);
        if(stringVal == null) {
            return defaultValue;
        }
        stringVal = stringVal.trim();
        if (stringVal.length() == 0) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(stringVal);
        } catch(NumberFormatException e) {
              throw new InvalidPropertyException(CorePlugin.Event.TEIID10039, propName, stringVal, Float.class, e);
        }
    }

    public static double getDoubleProperty(Properties props, String propName, double defaultValue) {
        String stringVal = props.getProperty(propName);
        if(stringVal == null) {
            return defaultValue;
        }
        stringVal = stringVal.trim();
        if (stringVal.length() == 0) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(stringVal);
        } catch(NumberFormatException e) {
              throw new InvalidPropertyException(CorePlugin.Event.TEIID10040, propName, stringVal, Double.class, e);
        }
    }

    public static boolean getBooleanProperty(Properties props, String propName, boolean defaultValue) {
        String stringVal = props.getProperty(propName);
        if(stringVal == null) {
            return defaultValue;
        }
        stringVal = stringVal.trim();
        if (stringVal.length() == 0) {
            return defaultValue;
        }
        return Boolean.valueOf(stringVal);
    }

    public static Properties load(String fileName) throws IOException {
        InputStream is = null;
        try {
            Properties props = new Properties();
            is = new FileInputStream(fileName);
            props.load(is);
            return props;
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    /**
     * Convert a nibble to a hex character
     * @param   nibble  the nibble to convert.
     */
    public static char toHex(int nibble) {
    return hexDigit[(nibble & 0xF)];
    }

    /** A table of hex digits */
    private static final char[] hexDigit = {
    '0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'
    };

    public static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(toHex(b >>> 4));
            sb.append(toHex(b));
        }
        return sb.toString();
    }

    /**
     * Return the bytes for a given hex string, or throw an {@link IllegalArgumentException}
     * @param hex
     * @return
     */
    public static byte[] fromHex(String hex) {
        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException();
        }
        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < hex.length(); i++) {
            int charValue = hex.charAt(i);
            //48-57 0-9
            //65-70 A-F
            //97-102 a-f
            if (charValue >= 48 && charValue <= 57) {
                charValue -= 48;
            } else if (charValue >= 65 && charValue <= 70) {
                charValue -= 55;
            } else if (charValue >= 97 && charValue <= 102) {
                charValue -= 87;
            } else {
                throw new IllegalArgumentException();
            }
            if (i % 2 == 0) {
                //high nibble
                result[i/2] |= (charValue << 4);
            } else {
                result[i/2] |= charValue;
            }
        }
        return result;
    }

    public static void toHex(StringBuilder sb, InputStream is) throws IOException {
        int i = 0;
        while ((i = is.read()) != -1) {
            byte b = (byte)i;
            sb.append(toHex(b >>> 4));
            sb.append(toHex(b));
        }
    }

    public static void setBeanProperties(Object bean, Properties props, String prefix) {
        setBeanProperties(bean, props, prefix, false);
    }

    public static void setBeanProperties(Object bean, Properties props, String prefix, boolean includeEnv) {
        Map<String, String> map = caseInsensitiveProps(props);
        final Method[] methods = bean.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            final Method method = methods[i];
            final String methodName = method.getName();
            // If setter ...
            if (! methodName.startsWith("set") || method.getParameterTypes().length != 1 ) { //$NON-NLS-1$
                continue;
            }
            // Get the property name
            String propertyName = methodName.substring(3);    // remove the "set"
            if (prefix != null) {
                propertyName = prefix + "." + propertyName; //$NON-NLS-1$
            }
            String propertyValue = null;
            if (includeEnv) {
                propertyValue = getValue(propertyName, map, System.getenv());
            } else {
                propertyValue = map.get(propertyName);
            }
            if (propertyValue != null || map.containsKey(propertyName)) {
                setProperty(bean, propertyValue, method, propertyName);
            }
        }
    }

    public static void setBeanProperty(Object bean, String name, Object value) {
        final Method[] methods = bean.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            final Method method = methods[i];
            final String methodName = method.getName();
            // If setter ...
            if (! methodName.startsWith("set") || method.getParameterTypes().length != 1 || !StringUtil.endsWithIgnoreCase(methodName, name)) { //$NON-NLS-1$
                continue;
            }
            // Get the property name
            final String propertyName = methodName.substring(3);    // remove the "set"
            setProperty(bean, value, method, propertyName);
        }
    }

    private static Class<?> setProperty(Object bean, Object value,
            final Method method, final String propertyName) {
        final Class<?> argType = method.getParameterTypes()[0];
        try {
            Object[] params = new Object[] {value};
            if (value != null && !argType.isAssignableFrom(value.getClass())) {
                params = new Object[] {StringUtil.valueOf(value.toString(), argType)};
            }
            method.invoke(bean, params);
        } catch (Throwable e) {
              throw new InvalidPropertyException(CorePlugin.Event.TEIID10044, propertyName, value, argType, e);
        }
        return argType;
    }

    private static Map<String, String> caseInsensitiveProps(final Properties props) {
        TreeMap<String, String> newProps = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Enumeration<?> itr = props.propertyNames();
        while ( itr.hasMoreElements() ) {
            final String name = (String) itr.nextElement();
            String propValue = props.getProperty(name);
            newProps.put(name, propValue);
        }
        return newProps;
    }

    /**
     * Search for the property first in the environment, then in the system properties
     * @param key
     * @param defaultValue
     * @return
     */
    public static String getHierarchicalProperty(String key, String defaultValue) {
        return getHierarchicalProperty(key, defaultValue, String.class);
    }

    /**
     * Search for the property first in the environment, then in the system properties
     * @param key
     * @param defaultValue
     * @param clazz
     * @return
     */
    public static <T> T getHierarchicalProperty(String key, T defaultValue, Class<T> clazz) {
        Map sysProps = System.getProperties();
        Map<String, String> env = System.getenv();
        String stringVal = getValue(key, sysProps, env);
        if(stringVal != null) {
            stringVal = stringVal.trim();
            if (stringVal.length() != 0) {
                return StringUtil.valueOf(stringVal, clazz);
            }
        }
        return defaultValue;
    }

    static String getValue(String key, Map sysProps,
            Map<String, String> env) {
        Object val = sysProps.get(key);
        String stringVal = null;
        if (val instanceof String) {
            stringVal = val.toString();
        } else {
            stringVal = env.get(key);
            if (stringVal == null) {
                //convert to upper snake case
                String newKey = key.replaceAll("[.-]", "_"); //$NON-NLS-1$ //$NON-NLS-2$
                newKey = newKey.replaceAll("([A-Z]|[a-z]+)([A-Z])", "$1_$2"); //$NON-NLS-1$ //$NON-NLS-2$
                newKey = newKey.replaceAll("([A-Z]|[a-z]+)([A-Z])", "$1_$2"); //$NON-NLS-1$ //$NON-NLS-2$
                stringVal = env.get(newKey.toUpperCase());
            }
        }
        return stringVal;
    }

    public static Properties getDefaultProperties() {
        Properties properties = new Properties();
        properties.putAll(System.getProperties());
        return properties;
    }

}
