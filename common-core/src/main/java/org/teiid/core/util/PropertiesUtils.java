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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
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
     * </p>
     * <p>For example, the following code
     * <pre><code>
     * Properties a;
     * Properties b;
     * //initialize ...
     * a.putAll(b);
     * </code></pre>
     * will fail <i>if</i> <code>b</code> had been constructed with a default
     * <code>Properties</code> object.  Those defaults would be lost and
     * not added to <code>a</code>.</p>
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
     * </p>
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
            Enumeration enumeration = withThese.propertyNames();
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

    /**
     * Read the header part of a properties file into a String. 
     * @param fileName
     * @return
     * @throws IOException
     * @since 4.3
     */
    public static String loadHeader(String fileName) throws IOException {
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(fileName);
            br = new BufferedReader(fr);
            String header = br.readLine();
            if (header != null && header.indexOf('#') == 0) {
                header = header.substring(1);
            }
            return header;
        } finally {
            if (br != null) {
                br.close();
            }
            if (fr != null) {
                fr.close();
            }
        }
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
    
    public static Properties loadFromURL(URL url) throws MalformedURLException, IOException {
        Properties result = new Properties();
        InputStream is = null;
        try {
	        is = url.openStream();
        	result.load(is);
        } finally {
        	if (is != null) {
        		is.close();
        	}
        }
        return result;
    }

    public static Properties loadAsResource(Class clazz, String resourceName) throws IOException { 
        InputStream is = null;
        Properties configProps = new Properties();
        try {
            is = clazz.getResourceAsStream(resourceName);
            ArgCheck.isNotNull(is);
            if (is != null) {
                   configProps.load(is);
            }
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception ce) {
                    
                }
            }
        }
        return configProps;
    }
    
    /**
     * Write the specified properties to the specified file,
     * with the specified header.  
     * Results may not be sorted.
     * @param fileName
     * @param props
     * @param header
     * @throws IOException
     * @since 4.3
     */
    public static void print(String fileName, Properties props, String header) throws IOException {
        FileOutputStream stream = null;
        try {
            stream = new FileOutputStream(fileName);
            props.store(stream, header);
            stream.flush();
        } finally {
            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (Exception e) {
            }
        }
    }

    
    
    /**
     * Write the specified properties to the specified file,
     * with the specified header.  
     * Results are sorted by property name.
     */    
    public static void print( String fileName, Properties props ) throws IOException {

        FileOutputStream stream = null;
        PrintStream writer = null;

        try {

    		stream = new FileOutputStream(fileName);
    	  	writer = new PrintStream(stream);
    
            List names = new ArrayList();
            Enumeration enumeration = props.propertyNames();
            while ( enumeration.hasMoreElements() ) {
                String name = (String) enumeration.nextElement();
                names.add(name);
            }
            Collections.sort(names);
    
            StringBuffer sb;
    
            for (Iterator nIt=names.iterator(); nIt.hasNext(); ) {
              String name = (String) nIt.next();
    
              String value = props.getProperty(name);
    
              sb = new StringBuffer();
    
              sb.append(name);
              sb.append("="); //$NON-NLS-1$
              sb.append(value);
    
              writer.println(sb.toString());
            }
            writer.flush();
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (Exception e){
                                
            }
            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (Exception e){
                                
            }            
        }


    }

    public static void print( java.io.PrintStream stream, Properties props ) {
        if (props != null) {
            Collection sorted = sortPropertiesForPrinting(props);
            for (Iterator it=sorted.iterator(); it.hasNext(); ) {
                String value = (String) it.next();
                stream.println(value);
            }
        }
    }

    private static final String NEWLINE = "\n"; //$NON-NLS-1$
    /**
     * Sorts the properties and returns a collection of entries
     * where each entry can be printed.  Each entry will print in the
     * format of: Property: <code>name</code> = <code>value</code>
     */

    private static final String APREFIX = "Property '"; //$NON-NLS-1$
    private static final String AEQUAL = "'='"; //$NON-NLS-1$
    private static final String ASUFFIX = "'"; //$NON-NLS-1$
    private static final String TAB = "\t"; //$NON-NLS-1$

    public static Collection sortPropertiesForPrinting(Properties props) {

        Collection sortedProps = new ArrayList(props.size());

        List names = new ArrayList();
        Enumeration enumeration = props.propertyNames();
        while ( enumeration.hasMoreElements() ) {
            String name = (String) enumeration.nextElement();
            names.add(name);
        }
        Collections.sort(names);

        StringBuffer sb;

        for (Iterator nIt=names.iterator(); nIt.hasNext(); ) {
          String name = (String) nIt.next();

          String value = null;
          if (PasswordMaskUtil.doesNameEndWithPasswordSuffix(name)){
                value = PasswordMaskUtil.MASK_STRING;
          } else {
                value = props.getProperty(name);
                value= saveConvert(value, false);
          }

          name = saveConvert(name, true);

          sb = new StringBuffer(APREFIX);

          sb.append(name);
          sb.append(TAB);
          sb.append(AEQUAL);
          sb.append(value);
          sb.append(ASUFFIX);

//          sortedProps.add(APREFIX + name + TAB + AEQUAL + value + ASUFFIX);
          sortedProps.add(sb.toString());
        }

        return sortedProps;

    }

    private static final String specialSaveChars = "=: \t\r\n\f#!"; //$NON-NLS-1$

    /*
     * Converts unicodes to encoded &#92;uxxxx
     * and writes out any of the characters in specialSaveChars
     * with a preceding slash
     */
    public static String saveConvert(String theString, boolean escapeSpace) {
        if ( theString == null ) {
            return ""; //$NON-NLS-1$
        }
        int len = theString.length();
        StringBuffer outBuffer = new StringBuffer(len*2);

        for(int x=0; x<len; x++) {
            char aChar = theString.charAt(x);
            switch(aChar) {
        case ' ':
            if (x == 0 || escapeSpace)
            outBuffer.append('\\');

            outBuffer.append(' ');
            break;
                case '\\':outBuffer.append('\\'); outBuffer.append('\\');
                          break;
                case '\t':outBuffer.append('\\'); outBuffer.append('t');
                          break;
                case '\n':outBuffer.append('\\'); outBuffer.append('n');
                          break;
                case '\r':outBuffer.append('\\'); outBuffer.append('r');
                          break;
                case '\f':outBuffer.append('\\'); outBuffer.append('f');
                          break;
                default:
                    if ((aChar < 0x0020) || (aChar > 0x007e)) {
                        outBuffer.append('\\');
                        outBuffer.append('u');
                        outBuffer.append(toHex((aChar >> 12) & 0xF));
                        outBuffer.append(toHex((aChar >>  8) & 0xF));
                        outBuffer.append(toHex((aChar >>  4) & 0xF));
                        outBuffer.append(toHex( aChar        & 0xF));
                    } else {
                        if (specialSaveChars.indexOf(aChar) != -1)
                            outBuffer.append('\\');
                        outBuffer.append(aChar);
                    }
            }
        }
        return outBuffer.toString();
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
    
    public static void toHex(StringBuilder sb, InputStream is) throws IOException {
    	int i = 0;
    	while ((i = is.read()) != -1) {
    		byte b = (byte)i;
			sb.append(toHex(b >>> 4));
			sb.append(toHex(b));
		}
    }

    /**
     * The specialty of nested properties is, in a given property file 
     * there can be values with pattern like "${...}"
     * <code>
     *  key1=value1
     *  key2=${key1}/value2
     * </code> 
     * where the value of the <code>key2</code> should resolve to <code>value1/value2</code>
     * also if the property in the pattern <code>${..}</code> is not found in the loaded 
     * properties, an exception is thrown. Multiple nesting is OK, however recursive nested is not supported.
     * @param original - Original properties to be resolved
     * @return resolved properties object.
     * @since 4.4
     */
    public static Properties resolveNestedProperties(Properties original) {
        
        for(Enumeration e = original.propertyNames(); e.hasMoreElements();) {
            String key = (String)e.nextElement();
            String value = original.getProperty(key);

            // this will take care of the if there are any non-string properties, 
            // no nesting allowed on these.
            if (value == null) {
                continue;
            }

            boolean matched = true;
            boolean modified = false;
            
            while(matched) {
                // now match the pattern, then extract and find the value
                int start = value.indexOf("${"); //$NON-NLS-1$
                int end = start;
                if (start != -1) {
                    end = value.indexOf('}', start);
                }
                matched = ((start != -1) && (end != -1)); 
                if (matched) {
                    String nestedkey = value.substring(start+2, end);
                    String nestedvalue = original.getProperty(nestedkey);
                    
                    // in cases where the key and the nestedkey are the same, this has to be bypassed
                    // because it will cause an infinite loop, and because there will be no value
                    // for the nestedkey that doesnt contain ${..} in the value
                    if (key.equals(nestedkey)) {
                        matched = false;

                    } else {

                    
                        // this will handle case where we did not resolve, mark it blank
                        if (nestedvalue == null) {
                        	  throw new TeiidRuntimeException(CorePlugin.Event.TEIID10042, CorePlugin.Util.gs(CorePlugin.Event.TEIID10042, nestedkey));
                        }                    
                        value = value.substring(0,start)+nestedvalue+value.substring(end+1);
                        modified = true;
                   }
                }
            }
            if(modified) {
            	original.setProperty(key, value);
            }
        }
        return original;
    }
    
    public static void setBeanProperties(Object bean, Properties props, String prefix) {
    	setBeanProperties(bean, props, prefix, false);
    }
    
    public static void setBeanProperties(Object bean, Properties props, String prefix, boolean caseSensitive) {
		// Move all prop names to lower case so we can use reflection to get
	    // method names and look them up in the connection props.
    	Map<?, ?> map = props;
    	if (!caseSensitive) {
    		map = caseInsensitiveProps(props);    		
    	}
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
            	if (caseSensitive) {
            		propertyName = prefix + "." + Character.toLowerCase(propertyName.charAt(0)) + propertyName.substring(1, propertyName.length()); //$NON-NLS-1$
            	} else {
            		propertyName = prefix + "." + propertyName; //$NON-NLS-1$
            	}
            }
            Object propertyValue = map.get(propertyName);
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

	private static TreeMap<String, String> caseInsensitiveProps(final Properties connectionProps) {
	    final TreeMap<String, String> caseInsensitive = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
	    final Enumeration<?> itr = connectionProps.propertyNames();
	    while ( itr.hasMoreElements() ) {
	        final String name = (String) itr.nextElement();
	        String propValue = connectionProps.getProperty(name);
	        if (propValue != null || connectionProps.containsKey(name)) {
	        	caseInsensitive.put(name, propValue);
	        } 
	    }
	    return caseInsensitive;
	}
	
	/**
     * Search for the property first in the environment, then in the system properties
     * @param key
     * @param defaultValue
     * @param clazz
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
	    String stringVal = System.getenv(key);
        if(stringVal != null) {
            stringVal = stringVal.trim();
            if (stringVal.length() != 0) {
                return StringUtil.valueOf(stringVal, clazz);
            }
        }
        stringVal = System.getProperty(key);
        if(stringVal != null) {
            stringVal = stringVal.trim();
            if (stringVal.length() != 0) {
                return StringUtil.valueOf(stringVal, clazz);
            }
        }
        return defaultValue;
	}
	
	public static Properties getCombinedProperties() {
	    Properties properties = new Properties();
        properties.putAll(System.getProperties());
        properties.putAll(System.getenv());
        return properties;
	}

}
