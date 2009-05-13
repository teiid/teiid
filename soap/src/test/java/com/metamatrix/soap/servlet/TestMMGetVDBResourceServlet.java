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

package com.metamatrix.soap.servlet;

import java.util.Map;
import java.util.TreeMap;

import com.metamatrix.common.util.WSDLServletUtil;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestMMGetVDBResourceServlet extends TestCase {
    
    private final MMGetVDBResourceServlet servlet = new MMGetVDBResourceServlet();
    
    private static String KEY_1 = "key1"; //$NON-NLS-1$
    private static String KEY_2 = "key2"; //$NON-NLS-1$
    private static String KEY_3 = "key3"; //$NON-NLS-1$
    private static String KEY_4 = "key4";//$NON-NLS-1$
    private static String KEY_5 = "key5";//$NON-NLS-1$
    
    
    private static String[] VALUE_1 = new String[] {"value1=new"}; //$NON-NLS-1$
    private static String[] VALUE_1_ENCODED = new String[] {"value1%3Dnew"}; //$NON-NLS-1$
    private static String[] VALUE_2 = new String[] {"value2"};//$NON-NLS-1$
    private static String[] VALUE_3 = new String[] {"value3"};//$NON-NLS-1$
    private static String[] VALUE_4 = new String[] {"value4"};//$NON-NLS-1$
    private static String[] VALUE_5 = new String[] {"value5"};//$NON-NLS-1$
    
    private static String EXPECTED_SUFFIX_STRING = KEY_1 + "=" + VALUE_1_ENCODED[0] + "&" + KEY_2 + "=" + VALUE_2[0] + "&" + KEY_3 + "=" + VALUE_3[0] + "&" + KEY_4 + "=" + VALUE_4[0] + "&" + KEY_5 + "=" + VALUE_5[0]; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ 
    
    private static String EXPECTED_HTTP_SUFFIX_STRING = "httptype=http&" + KEY_1 + "=" + VALUE_1_ENCODED[0] + "&" + KEY_2 + "=" + VALUE_2[0] + "&" + KEY_3 + "=" + VALUE_3[0] + "&" + KEY_4 + "=" + VALUE_4[0] + "&" + KEY_5 + "=" + VALUE_5[0]; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ //$NON-NLS-10$ 
    
    public void testBuildEndpointSuffixStringNoHttpTypeParam() {
        Map map = new TreeMap();
        map.put(KEY_1, VALUE_1);
        map.put(KEY_2, VALUE_2);
        map.put(KEY_3, VALUE_3);
        map.put(KEY_4, VALUE_4);
        map.put(KEY_5, VALUE_5);
        assertEquals(servlet.buildEndpointSuffixString(map), EXPECTED_SUFFIX_STRING);
        
    }
        
    public void testBuildEndpointSuffixStringWithHttpTypeParam() {
        Map map = new TreeMap();
        map.put(KEY_1, VALUE_1);
        map.put(KEY_2, VALUE_2);
        map.put(KEY_3, VALUE_3);
        map.put(KEY_4, VALUE_4);
        map.put(KEY_5, VALUE_5);
        map.put(WSDLServletUtil.HTTP_TYPE_PARAMETER_KEY, new String[] {WSDLServletUtil.HTTP_PARAMETER_VALUE}); 
        assertEquals(servlet.buildEndpointSuffixString(map), EXPECTED_HTTP_SUFFIX_STRING);
        
    }
    
}
