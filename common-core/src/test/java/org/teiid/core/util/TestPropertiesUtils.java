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

import static org.junit.Assert.*;

import java.io.File;
import java.util.*;

import org.junit.After;
import org.junit.Test;
import org.teiid.core.util.PropertiesUtils.InvalidPropertyException;


/**
 * Tests primarily the various cloning scenarios available with PropertiesUtils
 */
@SuppressWarnings({"nls","unchecked"})
public class TestPropertiesUtils {

    private final static String TEMP_FILE = UnitTestUtil.getTestScratchPath() + "/temp.properties";  //$NON-NLS-1$

    @After public void tearDown() throws Exception{
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


    // ################ putAll(Properties, Properties) ###########################

    @Test public void testPutAllWithDefaults(){
        Properties c = make(MAP_C, null, !UNMODIFIABLE);
        Properties ab = make(MAP_A, make(MAP_B, null, UNMODIFIABLE), UNMODIFIABLE);
        PropertiesUtils.putAll(c, ab);
        assertTrue(verifyProps(c, LIST_ABC));
    }

    // ##################### clone(Properties) ###################################

    @Test public void testSimpleModifiableClone(){
        Properties a = make(MAP_A, null, !UNMODIFIABLE);
        a = PropertiesUtils.clone(a);
        assertTrue(verifyProps(a, LIST_A));
    }

    @Test public void testSimpleModifiableCloneWithUnmodifiableDefaults(){
        Properties ab = make(MAP_A, make(MAP_B, null, UNMODIFIABLE), !UNMODIFIABLE);
        ab = PropertiesUtils.clone(ab);
        assertTrue(verifyProps(ab, LIST_AB));
    }

    @Test public void testSimpleModifiableCloneWithModifiableDefaults(){
        Properties ab = make(MAP_A, make(MAP_B, null, !UNMODIFIABLE), !UNMODIFIABLE);
        ab = PropertiesUtils.clone(ab);
        assertTrue(verifyProps(ab, LIST_AB));
    }

    // ##################### clone(Properties, boolean) ##########################

    @Test public void testCloneModifiableAsModifiable(){
        Properties a = make(MAP_A, null, !UNMODIFIABLE);
        a = PropertiesUtils.clone(a);
        assertTrue(verifyProps(a, LIST_A));
    }

    @Test public void testCloneUnmodifiableAsModifiable(){
        Properties a = make(MAP_A, null, UNMODIFIABLE);
        a = PropertiesUtils.clone(a);
        assertTrue(verifyProps(a, LIST_A));
    }

    @Test public void testCloneModifiableWithModifiableAsModifiable(){
        Properties ab = make(MAP_A, make(MAP_B, null, !UNMODIFIABLE), !UNMODIFIABLE);
        ab = PropertiesUtils.clone(ab);
        assertTrue(verifyProps(ab, LIST_AB));
    }

    // ######## clone(Properties, Properties, boolean, boolean) ##################

    @Test public void testCloneModAndModAsMod(){
        Properties a = make(MAP_A, null, !UNMODIFIABLE);
        Properties b = make(MAP_B, null, !UNMODIFIABLE);
        a = PropertiesUtils.clone(a, b, !DEEP_CLONE);
        assertTrue(verifyProps(a, LIST_AB));
    }

    @Test public void testDeepcloneModAndModAsMod(){
        Properties a = make(MAP_A, null, !UNMODIFIABLE);
        Properties b = make(MAP_B, null, !UNMODIFIABLE);
        a = PropertiesUtils.clone(a, b, DEEP_CLONE);
        assertTrue(verifyProps(a, LIST_AB));
    }

    @Test public void testCloneModAndUnmodAsMod(){
        Properties a = make(MAP_A, null, !UNMODIFIABLE);
        Properties b = make(MAP_B, null, UNMODIFIABLE);
        a = PropertiesUtils.clone(a, b, !DEEP_CLONE);
        assertTrue(verifyProps(a, LIST_AB));
    }

    @Test public void testDeepcloneModAndUnmodAsMod(){
        Properties a = make(MAP_A, null, !UNMODIFIABLE);
        Properties b = make(MAP_B, null, UNMODIFIABLE);
        a = PropertiesUtils.clone(a, b, DEEP_CLONE);
        assertTrue(verifyProps(a, LIST_AB));
    }

    @Test public void testCloneUnmodAndModAsMod(){
        Properties a = make(MAP_A, null, UNMODIFIABLE);
        Properties b = make(MAP_B, null, !UNMODIFIABLE);
        a = PropertiesUtils.clone(a, b, !DEEP_CLONE);
        assertTrue(verifyProps(a, LIST_AB));
    }

    @Test public void testDeepcloneUnmodAndModAsMod(){
        Properties a = make(MAP_A, null, UNMODIFIABLE);
        Properties b = make(MAP_B, null, !UNMODIFIABLE);
        a = PropertiesUtils.clone(a, b, DEEP_CLONE);
        assertTrue(verifyProps(a, LIST_AB));
    }

    @Test public void testCloneUnmodAndUnmodAsMod(){
        Properties a = make(MAP_A, null, UNMODIFIABLE);
        Properties b = make(MAP_B, null, UNMODIFIABLE);
        a = PropertiesUtils.clone(a, b, !DEEP_CLONE);
        assertTrue(verifyProps(a, LIST_AB));
    }

    @Test public void testDeepcloneUnmodAndUnmodAsMod(){
        Properties a = make(MAP_A, null, UNMODIFIABLE);
        Properties b = make(MAP_B, null, UNMODIFIABLE);
        a = PropertiesUtils.clone(a, b, DEEP_CLONE);
        assertTrue(verifyProps(a, LIST_AB));
    }

    // ########################## ADVANCED #######################################

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

    private static final Map<String, String> MAP_A;
    private static final Map<String, String> MAP_B;
    private static final Map<String, String> MAP_C;
    private static final List<Map<String, String>> LIST_A;
//    private static final List LIST_B;
    private static final List LIST_AB;
    private static final List LIST_ABC;
    static{
        //A
        Map<String, String> temp = new HashMap<String, String>();
        temp.put(PROP_NAME_1, PROP_VALUE_1A);
        temp.put(PROP_NAME_2, PROP_VALUE_2A);
        temp.put(PROP_NAME_3, PROP_VALUE_3A);
        MAP_A = Collections.unmodifiableMap(temp);
        //B
        temp = new HashMap<String, String>();
        temp.put(PROP_NAME_1, PROP_VALUE_1B);
        temp.put(PROP_NAME_4, PROP_VALUE_4B);
        temp.put(PROP_NAME_5, PROP_VALUE_5B);
        MAP_B = Collections.unmodifiableMap(temp);
        //C
        temp = new HashMap<String, String>();
        temp.put(PROP_NAME_2, PROP_VALUE_2C);
        temp.put(PROP_NAME_4, PROP_VALUE_4C);
        temp.put(PROP_NAME_6, PROP_VALUE_6C);
        MAP_C = Collections.unmodifiableMap(temp);
        //LISTS OF BINDINGS
        List<Map<String, String>> tempList = new ArrayList<Map<String, String>>(1);
        tempList.add(MAP_A);
        LIST_A = Collections.unmodifiableList(tempList);
        tempList = new ArrayList<Map<String, String>>(1);
        tempList.add(MAP_B);
//        LIST_B = Collections.unmodifiableList(tempList);
        tempList = new ArrayList<Map<String, String>>(2);
        tempList.add(MAP_A);
        tempList.add(MAP_B);
        LIST_AB = Collections.unmodifiableList(tempList);
        tempList = new ArrayList<Map<String, String>>(3);
        tempList.add(MAP_A);
        tempList.add(MAP_B);
        tempList.add(MAP_C);
        LIST_ABC = Collections.unmodifiableList(tempList);
    }

    @Test public void testGetInvalidInt() {
        Properties p = new Properties();
        p.setProperty("x", "y"); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            PropertiesUtils.getIntProperty(p, "x", 1); //$NON-NLS-1$
            fail("expected exception"); //$NON-NLS-1$
        } catch (InvalidPropertyException e) {
            assertEquals("TEIID10037 Property 'x' with value 'y' is not a valid Integer.", e.getMessage()); //$NON-NLS-1$
        }
    }

    static class Bean {
        private int prop;
        private String prop1;
        private double prop2;
        private List<String> prop3;

        public int getProp() {
            return prop;
        }
        public void setProp(int prop) {
            this.prop = prop;
        }
        public String getProp1() {
            return prop1;
        }
        public void setProp1(String prop1) {
            this.prop1 = prop1;
        }
        public double getProp2() {
            return prop2;
        }
        public void setProp2(double prop2) {
            this.prop2 = prop2;
        }
        public List<String> getProp3() {
            return prop3;
        }
        public void setProp3(List<String> prop3) {
            this.prop3 = prop3;
        }
    }

    @Test public void testSetBeanProperties() {
        Bean bean = new Bean();
        Properties p = new Properties();
        p.setProperty("prop", "0");  //$NON-NLS-1$//$NON-NLS-2$
        p.setProperty("prop1", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        p.setProperty("prop2", "2"); //$NON-NLS-1$ //$NON-NLS-2$
        p.setProperty("prop3", "3"); //$NON-NLS-1$ //$NON-NLS-2$

        p = new Properties(p);
        p.put("object", new Object()); //$NON-NLS-1$

        PropertiesUtils.setBeanProperties(bean, p, null);

        assertEquals(0, bean.getProp());
        assertEquals("1", bean.getProp1()); //$NON-NLS-1$
        assertEquals(2d, bean.getProp2(), 0);
        assertEquals(Arrays.asList("3"), bean.getProp3()); //$NON-NLS-1$

        p.setProperty("prop", "?"); //$NON-NLS-1$ //$NON-NLS-2$

        try {
            PropertiesUtils.setBeanProperties(bean, p, null);
            fail("expected exception"); //$NON-NLS-1$
        } catch (InvalidPropertyException e) {

        }
    }

    @Test public void testGetInt() {
        Properties p = new Properties();
        p.setProperty("prop", "0  "); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(PropertiesUtils.getIntProperty(p, "prop", -1), 0); //$NON-NLS-1$
    }

    public static class MyBean {
        int val;

        public int getVal() {
            return val;
        }

        public void setVal(int val) {
            this.val = val;
        }
    }

    @Test public void testCaseSensitive() {
        Properties p = new Properties();
        p.setProperty("org.teiid.val", "100"); //$NON-NLS-1$ //$NON-NLS-2$
        MyBean test = new MyBean();
        PropertiesUtils.setBeanProperties(test, p, "org.teiid");
        assertEquals(100, test.getVal());
    }

    @Test public void testSystemProperty() {
        String old = System.setProperty("org.teiid.val", "200");
        try {
            MyBean test = new MyBean();
            PropertiesUtils.setBeanProperties(test, System.getProperties(), "org.teiid");
            assertEquals(200, test.getVal());
        } finally {
            if (old != null) {
                System.setProperty("org.teiid.val", old);
            } else {
                System.clearProperty("org.teiid.val");
            }
        }
    }

    @Test public void testGetEnvValue() {
        Map<String, String> env = new HashMap<>();
        env.put("ORG_TEIID_SOME_LONG_VAL", "val");
        assertEquals("val", PropertiesUtils.getValue("org.teiid.someLongVal", Collections.EMPTY_MAP, env));
    }

}
