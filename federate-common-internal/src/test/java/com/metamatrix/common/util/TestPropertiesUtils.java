/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.properties.UnmodifiableProperties;

/**
 * Tests primarily the various cloning scenarios available with PropertiesUtils
 */
public class TestPropertiesUtils extends TestCase {

    private final static String TEMP_FILE = "temp.properties";  //$NON-NLS-1$
    
    
    
	// ################################## FRAMEWORK ################################

    public TestPropertiesUtils(String name) {
        super(name);
    }
    
	public void tearDown() throws Exception{
        try {
            File temp = new File(TEMP_FILE);
            temp.delete();
        } catch (Exception e) {
            //ignore
        }
	}
	

    //===================================================================
    //ACTUAL TESTS
    //===================================================================

    
    /**
     * Tests {@link com.metamatrix.common.utils.PropertiesUtils#print(String, Properties, String header)}
     * and {@link com.metamatrix.common.utils.PropertiesUtils#load(String)}
     * and {@link com.metamatrix.common.utils.PropertiesUtils#loadHeader(String)}
     */
    public void testPrintLoadWithHeader() throws Exception {
        Properties props1 = make(MAP_C, null, !UNMODIFIABLE);
        
        //print to file
        PropertiesUtils.print(TEMP_FILE, props1, "header"); //$NON-NLS-1$
        
        //load from file
        Properties props2 = PropertiesUtils.load(TEMP_FILE);
        assertEquals("Expected props1 to be equal to props2", 0, PropertiesUtils.compare(props1, props2)); //$NON-NLS-1$
        
        String header = PropertiesUtils.loadHeader(TEMP_FILE);
        assertEquals("header", header); //$NON-NLS-1$
    }
    
    
    /**
     * Tests {@link com.metamatrix.common.utils.PropertiesUtils#print(String, Properties)}
     * and {@link com.metamatrix.common.utils.PropertiesUtils#load(String)}
     * and {@link com.metamatrix.common.utils.PropertiesUtils#loadHeader(String)}
     */
    public void testPrintLoadWithoutHeader() throws Exception {
        Properties props1 = make(MAP_C, null, !UNMODIFIABLE);
        
        //print to file
        PropertiesUtils.print(TEMP_FILE, props1); 
        
        //load from file
        Properties props2 = PropertiesUtils.load(TEMP_FILE);
        assertEquals("Expected props1 to be equal to props2", 0, PropertiesUtils.compare(props1, props2)); //$NON-NLS-1$
    }
    
    
    
    
	// ################ putAll(Properties, Properties) ###########################

    public void testPutAllWithDefaults(){
	    Properties c = make(MAP_C, null, !UNMODIFIABLE);
        Properties ab = make(MAP_A, make(MAP_B, null, UNMODIFIABLE), UNMODIFIABLE);
	    PropertiesUtils.putAll(c, ab);
	    assertTrue(verifyProps(c, LIST_ABC));
    }

	// ##################### clone(Properties) ###################################

	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties)}
	 */
	public void testSimpleUnmodifiableClone(){
	    Properties a = make(MAP_A, null, UNMODIFIABLE);
	    a = PropertiesUtils.clone(a);
	    assertTrue(verifyProps(a, LIST_A));
	    assertTrue(a instanceof UnmodifiableProperties);
	}

	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties)}
	 */
	public void testSimpleModifiableClone(){
	    Properties a = make(MAP_A, null, !UNMODIFIABLE);
	    a = PropertiesUtils.clone(a);
	    assertTrue(verifyProps(a, LIST_A));
	    assertTrue(!(a instanceof UnmodifiableProperties));
	}

	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties)}
	 */
	public void testSimpleUnmodifiableCloneWithUnmodifiableDefaults(){
	    Properties ab = make(MAP_A, make(MAP_B, null, UNMODIFIABLE), UNMODIFIABLE);
	    ab = PropertiesUtils.clone(ab);
	    assertTrue(verifyProps(ab, LIST_AB));
	    assertTrue(ab instanceof UnmodifiableProperties);
	}

	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties)}
	 */
	public void testSimpleModifiableCloneWithUnmodifiableDefaults(){
	    Properties ab = make(MAP_A, make(MAP_B, null, UNMODIFIABLE), !UNMODIFIABLE);
	    ab = PropertiesUtils.clone(ab);
	    assertTrue(verifyProps(ab, LIST_AB));
	    assertTrue(!(ab instanceof UnmodifiableProperties));
	}

	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties)}
	 */
	public void testSimpleUnmodifiableCloneWithModifiableDefaults(){
	    Properties ab = make(MAP_A, make(MAP_B, null, !UNMODIFIABLE), UNMODIFIABLE);
	    ab = PropertiesUtils.clone(ab);
	    assertTrue(verifyProps(ab, LIST_AB));
	    assertTrue(ab instanceof UnmodifiableProperties);
	}

	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties)}
	 */
	public void testSimpleModifiableCloneWithModifiableDefaults(){
	    Properties ab = make(MAP_A, make(MAP_B, null, !UNMODIFIABLE), !UNMODIFIABLE);
	    ab = PropertiesUtils.clone(ab);
	    assertTrue(verifyProps(ab, LIST_AB));
	    assertTrue(!(ab instanceof UnmodifiableProperties));
	}









	// ##################### clone(Properties, boolean) ##########################


	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, boolean)}
	 */
	public void testCloneModifiableAsModifiable(){
	    Properties a = make(MAP_A, null, !UNMODIFIABLE);
	    a = PropertiesUtils.clone(a, !UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_A));
	    assertTrue(!(a instanceof UnmodifiableProperties));
	}

	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, boolean)}
	 */
	public void testCloneModifiableAsUnmodifiable(){
	    Properties a = make(MAP_A, null, !UNMODIFIABLE);
	    a = PropertiesUtils.clone(a, UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_A));
	    assertTrue(a instanceof UnmodifiableProperties);
	}

	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, boolean)}
	 */
	public void testCloneUnmodifiableAsModifiable(){
	    Properties a = make(MAP_A, null, UNMODIFIABLE);
	    a = PropertiesUtils.clone(a, !UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_A));
	    assertTrue(!(a instanceof UnmodifiableProperties));
	}

	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, boolean)}
	 */
	public void testCloneUnmodifiableAsUnmodifiable(){
	    Properties a = make(MAP_A, null, UNMODIFIABLE);
	    a = PropertiesUtils.clone(a, UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_A));
	    assertTrue(a instanceof UnmodifiableProperties);
	}


	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, boolean)}
	 */
	public void testCloneModifiableWithModifiableAsModifiable(){
	    Properties ab = make(MAP_A, make(MAP_B, null, !UNMODIFIABLE), !UNMODIFIABLE);
	    ab = PropertiesUtils.clone(ab, !UNMODIFIABLE);
	    assertTrue(verifyProps(ab, LIST_AB));
	    assertTrue(!(ab instanceof UnmodifiableProperties));
	}
	


	// ######## clone(Properties, Properties, boolean, boolean) ##################
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testCloneModAndModAsMod(){
	    Properties a = make(MAP_A, null, !UNMODIFIABLE);
		Properties b = make(MAP_B, null, !UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, !DEEP_CLONE, !UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(!(a instanceof UnmodifiableProperties));
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testDeepcloneModAndModAsMod(){
	    Properties a = make(MAP_A, null, !UNMODIFIABLE);
		Properties b = make(MAP_B, null, !UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, DEEP_CLONE, !UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(!(a instanceof UnmodifiableProperties));
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testCloneModAndModAsUnmod(){
	    Properties a = make(MAP_A, null, !UNMODIFIABLE);
		Properties b = make(MAP_B, null, !UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, !DEEP_CLONE, UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(a instanceof UnmodifiableProperties);
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testDeepcloneModAndModAsUnmod(){
	    Properties a = make(MAP_A, null, !UNMODIFIABLE);
		Properties b = make(MAP_B, null, !UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, DEEP_CLONE, UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(a instanceof UnmodifiableProperties);
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testCloneModAndUnmodAsMod(){
	    Properties a = make(MAP_A, null, !UNMODIFIABLE);
		Properties b = make(MAP_B, null, UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, !DEEP_CLONE, !UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(!(a instanceof UnmodifiableProperties));
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testDeepcloneModAndUnmodAsMod(){
	    Properties a = make(MAP_A, null, !UNMODIFIABLE);
		Properties b = make(MAP_B, null, UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, DEEP_CLONE, !UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(!(a instanceof UnmodifiableProperties));
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testCloneModAndUnmodAsUnmod(){
	    Properties a = make(MAP_A, null, !UNMODIFIABLE);
		Properties b = make(MAP_B, null, UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, !DEEP_CLONE, UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(a instanceof UnmodifiableProperties);
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testDeepcloneModAndUnmodAsUnmod(){
	    Properties a = make(MAP_A, null, !UNMODIFIABLE);
		Properties b = make(MAP_B, null, UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, DEEP_CLONE, UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(a instanceof UnmodifiableProperties);
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testCloneUnmodAndModAsMod(){
	    Properties a = make(MAP_A, null, UNMODIFIABLE);
		Properties b = make(MAP_B, null, !UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, !DEEP_CLONE, !UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(!(a instanceof UnmodifiableProperties));
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testDeepcloneUnmodAndModAsMod(){
	    Properties a = make(MAP_A, null, UNMODIFIABLE);
		Properties b = make(MAP_B, null, !UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, DEEP_CLONE, !UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(!(a instanceof UnmodifiableProperties));
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testCloneUnmodAndModAsUnmod(){
	    Properties a = make(MAP_A, null, UNMODIFIABLE);
		Properties b = make(MAP_B, null, !UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, !DEEP_CLONE, UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(a instanceof UnmodifiableProperties);
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testDeepcloneUnmodAndModAsUnmod(){
	    Properties a = make(MAP_A, null, UNMODIFIABLE);
		Properties b = make(MAP_B, null, !UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, DEEP_CLONE, UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(a instanceof UnmodifiableProperties);
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testCloneUnmodAndUnmodAsMod(){
	    Properties a = make(MAP_A, null, UNMODIFIABLE);
		Properties b = make(MAP_B, null, UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, !DEEP_CLONE, !UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(!(a instanceof UnmodifiableProperties));
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testDeepcloneUnmodAndUnmodAsMod(){
	    Properties a = make(MAP_A, null, UNMODIFIABLE);
		Properties b = make(MAP_B, null, UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, DEEP_CLONE, !UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(!(a instanceof UnmodifiableProperties));
	}

	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testCloneUnmodAndUnmodAsUnmod(){
	    Properties a = make(MAP_A, null, UNMODIFIABLE);
		Properties b = make(MAP_B, null, UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, !DEEP_CLONE, UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(a instanceof UnmodifiableProperties);
	}

	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testDeepcloneUnmodAndUnmodAsUnmod(){
	    Properties a = make(MAP_A, null, UNMODIFIABLE);
		Properties b = make(MAP_B, null, UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, b, DEEP_CLONE, UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_AB));
	    assertTrue(a instanceof UnmodifiableProperties);
	}


	// ########################## ADVANCED #######################################

	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testAdvanced(){
	    Properties a = make(MAP_A, null, UNMODIFIABLE);
		Properties bc = make(MAP_B, make(MAP_C, null, UNMODIFIABLE), UNMODIFIABLE); 
	    a = PropertiesUtils.clone(a, bc, DEEP_CLONE, UNMODIFIABLE);
	    assertTrue(verifyProps(a, LIST_ABC));
	    assertTrue(a instanceof UnmodifiableProperties);
	}
	
	/**
	 * Tests {@link com.metamatrix.common.utils.PropertiesUtils#clone(Properties, Properties, boolean, boolean)}
	 */
	public void testAdvanced2(){
	    Properties ab = make(MAP_A, make(MAP_B, null, UNMODIFIABLE), UNMODIFIABLE);
		Properties c = make(MAP_C, null, UNMODIFIABLE); 
	    ab = PropertiesUtils.clone(ab, c, DEEP_CLONE, UNMODIFIABLE);
	    assertTrue(verifyProps(ab, LIST_ABC));
	    assertTrue(ab instanceof UnmodifiableProperties);
	}

    
    

    //===================================================================
    //TESTS HELPERS
    //===================================================================

	/**
	 * Checks the Properties against the static test data defined in this Class. 
	 * @param props Properties to check
	 * @param chainOfMappings a List of Map objects in order of defaults.  That is,
	 * the first Map should represent the properties itself, the second Map the internal
	 * defaults of the properties, the third Map the defaults of the second Map, and so on... 
	 * @return true or false for pass or fail
	 */
	private static final boolean verifyProps(Properties props, List chainOfMappings){
	    boolean result = verifyAllPropsPresent(props, chainOfMappings);
	    if (result){
	    	result = verifyCorrectMappings(props, chainOfMappings);   
	    }
	    return result; 
	}
	
	/**
	 * Check that the Set of all keys in the List<Map> chainOfMappings is present in props.
	 * @param props Properties to check
	 * @param chainOfMappings a List of Map objects in order of defaults.  That is,
	 * the first Map should represent the properties itself, the second Map the internal
	 * defaults of the properties, the third Map the defaults of the second Map, and so on... 
	 * @return true all keys are present, false otherwise
	 */
    private static final boolean verifyAllPropsPresent(Properties props, List chainOfMappings){
	    Enumeration e = props.propertyNames();
		HashSet propNames = new HashSet();
	    while (e.hasMoreElements()) {
            propNames.add( e.nextElement());
	    }
	    
	    HashSet testNames = new HashSet();
        Iterator i = chainOfMappings.iterator();
        while (i.hasNext()) {
            Map aMapping = (Map) i.next();
			testNames.addAll(aMapping.keySet());
        }	    
		return propNames.containsAll(testNames);
	}

	/**
	 * Verify that the Properties props correctly reflects the chain of mappings (which
	 * simulate an arbitrary chain of Properties and default Properties).  For each
	 * property name, look in order through each Map in the List chainOfMappings to
	 * see if (a) that property name is there, and (b) that it is mapped to the same
	 * property.  There are two conditions that will cause this method to returns
	 * false: (1) if a property name maps to an incorrect, non-null value the first
	 * time a mapping for that property name is found; (2) if no mapping is found at
	 * for a property name.
	 * @param props Properties to check
	 * @param chainOfMappings a List of Map objects in order of defaults.  That is,
	 * the first Map should represent the properties itself, the second Map the internal
	 * defaults of the properties, the third Map the defaults of the second Map, and so on... 
	 * @return true if props correctly reflects the chainOfMappings, false otherwise
	 */
	private static final boolean verifyCorrectMappings(Properties props, List chainOfMappings){
	    Enumeration e = props.propertyNames();
		boolean allGood = true;
	    while (e.hasMoreElements() && allGood) {
			boolean foundKey = false;
            String propName = (String) e.nextElement();
			String propValue = props.getProperty(propName);
            Iterator i = chainOfMappings.iterator();
            while (i.hasNext() && !foundKey) {
                Map aMapping = (Map) i.next();
                Object value = aMapping.get(propName);
				if (value != null){
				    foundKey = true;
					allGood = propValue.equals(value);
				}
            }
        }
		return allGood;
    }

	/**
	 * Constructs a Properties object from the supplied Map of properties,
	 * the supplied defaults, and optionally wraps the returned Properties
	 * in an UnmodifiableProperties instance
	 * @param mappings Map of String propName to String propValue
	 * @param defaults optional default Properties; may be null
	 * @param makeUnmodifiable If true, the returned Properties object will be
	 * an instance of UnmodifiableProperties wrapping a Properties object
	 */
	private static final Properties make(Map mappings, Properties defaults, boolean makeUnmodifiable){
	    Properties props = null;
	    if (defaults != null){
	    	props = new Properties(defaults);    
	    } else {
	        props = new Properties();
	    }
	    Iterator i = mappings.entrySet().iterator();
	    while (i.hasNext()) {
            Map.Entry anEntry = (Map.Entry) i.next();
            props.setProperty((String)anEntry.getKey(),(String)anEntry.getValue());
        }
	    if (makeUnmodifiable){
	        props = new UnmodifiableProperties(props);
	    }
	    return props;
	}

	private static final boolean UNMODIFIABLE = true;
	private static final boolean DEEP_CLONE = true;
	
	private static final String PROP_NAME_1 = "prop1"; //$NON-NLS-1$
	private static final String PROP_NAME_2 = "prop2"; //$NON-NLS-1$
	private static final String PROP_NAME_3 = "prop3"; //$NON-NLS-1$
	private static final String PROP_NAME_4 = "prop4"; //$NON-NLS-1$
	private static final String PROP_NAME_5 = "prop5"; //$NON-NLS-1$
	private static final String PROP_NAME_6 = "prop6"; //$NON-NLS-1$

	//"a", "b", or "c" designates which of the test Properties
	//the values will go in	
	private static final String PROP_VALUE_1A = "value1a"; //$NON-NLS-1$
	private static final String PROP_VALUE_1B = "value1b"; //$NON-NLS-1$
	private static final String PROP_VALUE_2A = "value2a"; //$NON-NLS-1$
	private static final String PROP_VALUE_2C = "value2c"; //$NON-NLS-1$
	private static final String PROP_VALUE_3A = "value3a"; //$NON-NLS-1$
	private static final String PROP_VALUE_4B = "value4b"; //$NON-NLS-1$
	private static final String PROP_VALUE_4C = "value4c"; //$NON-NLS-1$
	private static final String PROP_VALUE_5B = "value5b"; //$NON-NLS-1$
	private static final String PROP_VALUE_6C = "value6c"; //$NON-NLS-1$
	
	private static final Map MAP_A;
	private static final Map MAP_B;
	private static final Map MAP_C;
	private static final List LIST_A;
//	private static final List LIST_B;
	private static final List LIST_AB;
	private static final List LIST_ABC;
	static{
		//A
	    Map temp = new HashMap();
	    temp.put(PROP_NAME_1, PROP_VALUE_1A);
	    temp.put(PROP_NAME_2, PROP_VALUE_2A);
	    temp.put(PROP_NAME_3, PROP_VALUE_3A);
		MAP_A = Collections.unmodifiableMap(temp);
		//B
	    temp = new HashMap();
	    temp.put(PROP_NAME_1, PROP_VALUE_1B);
	    temp.put(PROP_NAME_4, PROP_VALUE_4B);
	    temp.put(PROP_NAME_5, PROP_VALUE_5B);
		MAP_B = Collections.unmodifiableMap(temp);
		//C
	    temp = new HashMap();
	    temp.put(PROP_NAME_2, PROP_VALUE_2C);
	    temp.put(PROP_NAME_4, PROP_VALUE_4C);
	    temp.put(PROP_NAME_6, PROP_VALUE_6C);
		MAP_C = Collections.unmodifiableMap(temp);
		//LISTS OF BINDINGS		
		List tempList = new ArrayList(1);
		tempList.add(MAP_A);
		LIST_A = Collections.unmodifiableList(tempList);
		tempList = new ArrayList(1);
		tempList.add(MAP_B);
//		LIST_B = Collections.unmodifiableList(tempList);
		tempList = new ArrayList(2);
		tempList.add(MAP_A);
		tempList.add(MAP_B);
		LIST_AB = Collections.unmodifiableList(tempList);
		tempList = new ArrayList(3);
		tempList.add(MAP_A);
		tempList.add(MAP_B);
		tempList.add(MAP_C);
		LIST_ABC = Collections.unmodifiableList(tempList);
	}
    
    public void testNestedProperties() throws Exception {
        System.setProperty("testdirectory", "c:/metamatrix/testdirectory"); //$NON-NLS-1$ //$NON-NLS-2$ 
        
        Properties p = new Properties();
        p.setProperty("key1", "value1"); //$NON-NLS-1$ //$NON-NLS-2$
        p.setProperty("key2", "${key1}/value2"); //$NON-NLS-1$ //$NON-NLS-2$
        p.put("key3", new Integer(-234)); //$NON-NLS-1$
        p.setProperty("key4", "${key2}/value4"); //$NON-NLS-1$ //$NON-NLS-2$
        p.setProperty("key5", "${testdirectory}/testdata"); //$NON-NLS-1$ //$NON-NLS-2$
        p.setProperty("key6", "${foo}"); //$NON-NLS-1$ //$NON-NLS-2$
        p.setProperty("key7", "anotherdir/${testdirectory}/${key1}"); //$NON-NLS-1$ //$NON-NLS-2$
        
        Properties m = PropertiesUtils.resolveNestedProperties(p);
        assertEquals("value1/value2", m.getProperty("key2")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(new Integer(-234), m.get("key3")); //$NON-NLS-1$ 
        assertEquals("value1/value2/value4", m.getProperty("key4")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("c:/metamatrix/testdirectory/testdata", m.getProperty("key5")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(null, m.getProperty("key6")); //$NON-NLS-1$ 
        assertEquals("anotherdir/c:/metamatrix/testdirectory/value1", m.getProperty("key7")); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testOverrideProperties() {
        Properties p = new Properties();
        
        p.setProperty("foo", "bar"); 
        p.setProperty("foo1", "bar1");
        p.setProperty("foo2", "bar2");
        
        Properties p1 = new Properties(p);
        
        p1.setProperty("foo", "x");
        
        PropertiesUtils.setOverrideProperies(p1, p);
        
        assertEquals("bar", p1.getProperty("foo"));
        
        assertEquals(1, p1.size());
    }
}
