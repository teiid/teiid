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

import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidRuntimeException;



/**
 * Static utility methods for common tasks having to do with
 * java.util.Properties.
 */
public final class PropertiesUtils {
	
	public static class InvalidPropertyException extends TeiidRuntimeException {
		
		public InvalidPropertyException(String propertyName, String value, Class<?> expectedType, Throwable cause) {
			super(cause, CorePlugin.Util.getString("InvalidPropertyException.message", propertyName, value, expectedType.getSimpleName())); //$NON-NLS-1$
		}

	}

    /**
     * Returns a list of property names matching the given pattern. A '*' may be
     * given to match the pattern exactly up to the '*', then anything after.
     * Note: Should also implement matching with '?' which means match any single
     * char.
     * @param pattern The property name to match which may include a '*'.
     * @param props The properties to search.
     * @return The List of property names matching given pattern - may be empty
     * but never null.
     */
    public static List filter( String pattern, Properties props ) {
        boolean addAll = false;
        String searchStr = null;
        List propNames = new ArrayList();
        int globIndex = pattern.indexOf('*');
        if ( globIndex == -1 ) {
            searchStr = pattern;
        } else if ( globIndex == 0 ) {
            addAll = true;
        } else {
            searchStr = pattern.substring(0, globIndex);
        }

        Enumeration propNameEnum = props.propertyNames();
        while ( propNameEnum.hasMoreElements() ) {
            String name = (String) propNameEnum.nextElement();
            if ( name.startsWith(searchStr) || addAll ) {
                propNames.add(name);
            }
        }

        return propNames;
    }


    /**
     * Obtains from this source the list of all properties that match the pattern specified by the filter.
     * <p>
     * The filter is a string that may contain the '*' character as a wildcard one or more times
     * in the string.  For example, the following filter:
     * <p>
     * <pre>       *metamatrix.*</pre>
     * <p>
     * finds all properties that contain somewhere in the property name the string "metamatrix.".
     * @param filterPattern the string filter pattern that will be used to narrow the set of properties returned.
     * @param props The properties to search.
     * @return the enumeration of all of the property names of the primary source;
     * the enumeration may be empty if there is an error connecting to the property sources.
     */
    public static Properties getProperties(String filterPattern, Properties props)  {
        Properties results = new Properties();

        boolean addAll = false;
        String searchStr = null;
        int globIndex = filterPattern.indexOf('*');
        if ( globIndex == -1 ) {
            searchStr = filterPattern;
        } else if ( globIndex == 0 ) {
            addAll = true;
        } else {
            searchStr = filterPattern.substring(0, globIndex);
        }

        Enumeration propNameEnum = props.propertyNames();
        while ( propNameEnum.hasMoreElements() ) {
            String name = (String) propNameEnum.nextElement();
            if ( name.startsWith(searchStr)) {
            	Object value = props.get(name);
            	if (value != null) {
            		results.put(name.substring(searchStr.length()), value);
            	}
            }
            else if (addAll) {
            	results.put(name, props.get(name));
            }
        }

        return results;
    }


    /**
     * Performs a correct deep clone of the properties object by capturing
     * all properties in the default(s) and placing them directly into the
     * new Properties object.  If the input is an instance of
     * <code>UnmodifiableProperties</code>, this method returns an
     * <code>UnmodifiableProperties</code> instance around a new (flattened)
     * copy of the underlying Properties object.
     */
    public static Properties clone( Properties props ) {
        return clone(props, null, false);
    }

    /**
     * Performs a correct deep clone of the properties object by capturing
     * all properties in the default(s) and placing them directly into the
     * new Properties object.  If the input is an instance of
     * <code>UnmodifiableProperties</code>, this method returns an
     * <code>UnmodifiableProperties</code> instance around a new (flattened)
     * copy of the underlying Properties object.
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
     * This method implements a 'compareTo' logic for two Properties objects,
     * equivalent to calling <code>p1.compareTo(p2)</code> if the
     * {@link java.util.Properties Properties} class implemented
     * {@link java.lang.Comparable Comparable} (which it does not).
     * @param p1 the first Properties instance to compare; may be null
     * @param p2 the second Properties instance to compare; may be null
     * @return a negative integer, zero, or a positive integer as <code>p1</code>
     *      is less than, equal to, or greater than <code>p2</code>, respectively.
     */
    public static int compare( Properties p1, Properties p2 ) {
        if ( p1 != null ) {
            if ( p2 == null ) {
            	return 1;
            }
        } else {
        	if ( p2 != null ) {
        		return -1;
        	}
        	return 0;
        }


        // Compare the number of property values ...
        int diff = p1.size() - p2.size();
        if ( diff != 0 ) {
            return diff;
        }

        // Iterate through the properties and compare values ...
        Map.Entry entry = null;
        Object p1Value = null;
        Object p2Value = null;
        Iterator iter = p1.entrySet().iterator();
        while ( iter.hasNext() ) {
            entry = (Map.Entry) iter.next();
            p1Value = entry.getValue();
            p2Value = p2.get(entry.getKey());
            if ( p1Value != null ) {
                if ( p2Value == null ) {
                    return 1;
                }
                if ( p1Value instanceof Comparable ) {
                    diff = ((Comparable)p1Value).compareTo(p2Value);
                } else {
                    diff = p1Value.toString().compareTo(p2Value.toString());
                }
                if ( diff != 0 ) {
                    return diff;
                }
            } else {
                if ( p2Value != null ) {
                    return -1;
                }
            }
        }
        return 0;
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
    
    public static void setOverrideProperies(Properties base, Properties override) {
        Enumeration overrideEnum = override.propertyNames();
        while (overrideEnum.hasMoreElements()) {
            String key = (String)overrideEnum.nextElement();
            String value = base.getProperty(key);
            String overRideValue = override.getProperty(key);
            if (value != null && !value.equals(overRideValue)) {
                base.setProperty(key, overRideValue);
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
            throw new InvalidPropertyException(propName, stringVal, Integer.class, e);
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
        	throw new InvalidPropertyException(propName, stringVal, Long.class, e);
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
        	throw new InvalidPropertyException(propName, stringVal, Float.class, e);
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
        	throw new InvalidPropertyException(propName, stringVal, Double.class, e);
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
        try {
            return Boolean.valueOf(stringVal);
        } catch(NumberFormatException e) {
        	throw new InvalidPropertyException(propName, stringVal, Float.class, e);
        }
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
    
    public static Properties sort(Properties props) {

        List names = new ArrayList();
        Enumeration enumeration = props.propertyNames();
        while ( enumeration.hasMoreElements() ) {
            String name = (String) enumeration.nextElement();
            names.add(name);
        }
        Collections.sort(names);

        Properties newProps = new Properties();
        Iterator iter = names.iterator();
        while ( iter.hasNext() ) {
            String name = (String) iter.next();
            String propValue = props.getProperty(name);
            if ( propValue != null ) {
                newProps.setProperty(name, propValue);
            }
        }
        return newProps;
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
    public static String prettyPrint( Properties props ) {
        if (props != null) {
            Collection sorted = sortPropertiesForPrinting(props);
    
            StringBuffer outBuf = new StringBuffer();
    
            for (Iterator it=sorted.iterator(); it.hasNext(); ) {
                String value = (String) it.next();
                outBuf.append(value);
                outBuf.append(NEWLINE);
            }
    
            return outBuf.toString();
        }
        return ""; //$NON-NLS-1$
    }

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

//    private static final String keyValueSeparators = "=: \t\r\n\f";

//    private static final String strictKeyValueSeparators = "=:";

    private static final String specialSaveChars = "=: \t\r\n\f#!"; //$NON-NLS-1$

//    private static final String whiteSpaceChars = " \t\r\n\f";


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


    public static final void copyProperty(Properties srcProperties, String srcPropName, Properties tgtProperties, String tgtPropName) {
        if(srcProperties == null || srcPropName == null || tgtProperties == null || tgtPropName == null) {
            return;
        }
        
        String value = srcProperties.getProperty(srcPropName);
        if(value != null) {
            tgtProperties.setProperty(tgtPropName, value);
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
                        	throw new TeiidRuntimeException(CorePlugin.Util.getString("PropertiesUtils.failed_to_resolve_property", nestedkey)); //$NON-NLS-1$
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
    
 // ======================================================
    /**
     * Returns a boolean indicating whether the string matched the given pattern.
     * A '*' may be
     * given to match the pattern exactly up to the '*', then anything after.
     * We will also support a leading star, and match on anything that ends with
     * the string specified after the star.
     * Note: Should also implement matching with '?' which means match any single
     * char.
     * @param pattern The property name to match which may include a '*'.
     * @param props The properties to search.
     * @return The boolean - passed or failed
     * but never null.
     */
    public static boolean filterTest( String pattern, String sCandidate ) {

        // Vars for match strategy
        char   chStar               = '*';

        // Match rule booleans.  Please note that 'bLeading'
        //  and 'bTrailing' refer to the string we are searching for.
        //  For example, if the strategy is bLeading, and the Match frag
        //  is "wet", and the candidate string is "wetrust", it will pass.
        //  Likewise if the strategy is bTrailing and the Match frag is
        //  "rust" and the candidate string is "wetrust", it will pass.
        boolean bLeading            = false;
        boolean bTrailing           = false;
        boolean bFullMatch          = false;
        boolean bAnywhere           = false;
        boolean bAllMatch           = false;

        boolean bPass               = false;

        String sMatchFrag           = ""; //$NON-NLS-1$
//        List propNames              = new ArrayList();


        // 1. Analyze pattern to resolve match strategy

        //  First ensure the pattern is safe to work with.
        //  If the pattern is an empty string, set it to '*',
        //  which means anything passes.
        pattern = pattern.trim();
        if ( pattern.length() == 0 )
            pattern = "*"; //$NON-NLS-1$

        int iFirstStar  =   pattern.indexOf( chStar );
        int iLastStar   =   pattern.lastIndexOf( chStar );

        // If there are any stars:
        if( (iFirstStar > -1) && ( iLastStar > -1 ) )
        {
            // and their positions are the same (same star, silly)
            if( iFirstStar == iLastStar )
            {
                // and this star is at the front:
                if( iFirstStar == 0 )
                {
                    // and the pattern is only one byte long:
                    if( pattern.length() == 1 )
                    {
                        // Then the pattern is a single '*',
                        // and all will pass the match:
                        bAllMatch  = true;
                    }
                    else
                    {
                        // Or the pattern is a leading star followed
                        //  by a string:
                        bTrailing   = true;
                        sMatchFrag  = pattern.substring( 1 );
                    }
                }
                else
                {
                    // OR the star is NOT at the front, so the
                    //  pattern is a trailing star preceded by a string:
                    bLeading    = true;
                    sMatchFrag  = pattern.substring( 0, iLastStar );

                }
            }
            else
            {
                // They are not equal
                //sMatchStrategy  = ANYWHERE;
                bAnywhere   = true;
                sMatchFrag  = pattern.substring( iFirstStar + 1, iLastStar );
            }
        }
        else
        {
            // there are no stars at all
            //sMatchStrategy  = FULL_MATCH;
            bFullMatch  = true;
            sMatchFrag  = pattern;
        }

        // Now test the string
        String name     = sCandidate;
        bPass           = false;

        // force the match fragment and the test string to UPPER case
        String sMatchFragUpper =   sMatchFrag.toUpperCase();
        String sNameUpper      =   name.toUpperCase();

        // Test all of the booleans.  Only one should be true.
        if( bAllMatch ) {
            bPass = true;
        }
        else
        if( bAnywhere ) {
            if( sNameUpper.indexOf( sMatchFragUpper ) > -1 )
                bPass = true;
        }
        else
        if( bFullMatch ) {
            if( sNameUpper.equals( sMatchFragUpper ) )
                bPass = true;
        }
        else
        if( bLeading ) {
            if( sNameUpper.startsWith( sMatchFragUpper ) )
                bPass = true;
        }
        else
        if( bTrailing ) {
            if( sNameUpper.endsWith( sMatchFragUpper ) )
                bPass = true;
        }

        return bPass;
    }

    public static void setBeanProperties(Object bean, Properties props, String prefix) {
		// Move all prop names to lower case so we can use reflection to get
	    // method names and look them up in the connection props.
	    final Properties connProps = lowerCaseAllPropNames(props);
	    final Method[] methods = bean.getClass().getMethods();
	    for (int i = 0; i < methods.length; i++) {
	        final Method method = methods[i];
	        final String methodName = method.getName();
	        // If setter ...
	        if ( methodName.startsWith("set") && method.getParameterTypes().length == 1 ) { //$NON-NLS-1$
	            // Get the property name
	            final String propertyName = methodName.substring(3);    // remove the "set"
	            String shortName = propertyName.toLowerCase();
	            String propertyValue = null;
	            if (prefix != null) {
	            	propertyValue = connProps.getProperty(prefix + "." + shortName); //$NON-NLS-1$
	            } else {
	            	propertyValue = connProps.getProperty(shortName);
	            }
	            if (propertyValue == null) {
	            	continue;
	            }
                final Class<?> argType = method.getParameterTypes()[0];
                try {
                    final Object[] params = new Object[] {StringUtil.valueOf(propertyValue, argType)};
                    method.invoke(bean, params);
                } catch (Throwable e) {
                	throw new InvalidPropertyException(propertyName, propertyValue, argType, e);
                }
	        }
	    }
	}
    
    public static void setBeanProperty(Object bean, String name, Object value) {
    	if (value == null) {
    		return;
    	}
    	name = name.toLowerCase();
	    final Method[] methods = bean.getClass().getMethods();
	    for (int i = 0; i < methods.length; i++) {
	        final Method method = methods[i];
	        final String methodName = method.getName();
	        // If setter ...
	        if ( methodName.startsWith("set") && method.getParameterTypes().length == 1 ) { //$NON-NLS-1$
	            // Get the property name
	            final String propertyName = methodName.substring(3);    // remove the "set"
	            String shortName = propertyName.toLowerCase();
	            if (!shortName.equals(name)) {
	            	continue;
	            }
                final Class<?> argType = method.getParameterTypes()[0];
                try {
                	Object[] params = new Object[] {value};
                	if (!argType.isAssignableFrom(value.getClass())) {
                		params = new Object[] {StringUtil.valueOf(value.toString(), argType)};
                	}
                    method.invoke(bean, params);
                } catch (Throwable e) {
                	throw new InvalidPropertyException(propertyName, value.toString(), argType, e);
                }
	        }
	    }
	}    

	private static Properties lowerCaseAllPropNames(final Properties connectionProps) {
	    final Properties lcProps = new Properties();
	    final Enumeration<?> itr = connectionProps.propertyNames();
	    while ( itr.hasMoreElements() ) {
	        final String name = (String) itr.nextElement();
	        String propValue = connectionProps.getProperty(name);
	        if (propValue != null) {
	        	lcProps.setProperty(name.toLowerCase(), propValue);
	        } 
	    }
	    return lcProps;
	}

}
