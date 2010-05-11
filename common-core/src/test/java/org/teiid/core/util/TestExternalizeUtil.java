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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.util.ExternalizeUtil;

import junit.framework.TestCase;

public class TestExternalizeUtil extends TestCase {

    private ByteArrayOutputStream bout;
    private ObjectOutputStream oout;   
    
    /**
     * Constructor for TestExternalizeUtil.
     * @param name
     */
    public TestExternalizeUtil(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        bout = new ByteArrayOutputStream(4096);
        oout = new ObjectOutputStream(bout);
    }
    
    public void testEmptyCollection() throws Exception {
    	ExternalizeUtil.writeCollection(oout, Arrays.asList(new Object[0]));
    	oout.flush();        
        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        ObjectInputStream oin = new ObjectInputStream(bin);
        
        List<?> result = ExternalizeUtil.readList(oin);
        assertEquals(0, result.size());
    }

}
