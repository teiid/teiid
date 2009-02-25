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

package org.teiid.connector.jdbc;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.metamatrix.connector.api.ConnectorCapabilities;

public class TestJDBCConnector {

    public void helpTestMaxIn(int setting, int expected) throws Exception {
        Properties connProps = new Properties();
        connProps.setProperty(JDBCPropertyNames.SET_CRITERIA_BATCH_SIZE, String.valueOf(setting)); 
        connProps.setProperty(JDBCPropertyNames.EXT_CAPABILITY_CLASS, SimpleCapabilities.class.getName()); 
        ConnectorCapabilities caps = JDBCConnector.createCapabilities(connProps, this.getClass().getClassLoader());
        int maxIn = caps.getMaxInCriteriaSize();
        Assert.assertEquals(expected, maxIn);
    }

    @Test
    public void test1() throws Exception {
        helpTestMaxIn(-1, 250);
    }

    @Test
    public void test2() throws Exception {
        helpTestMaxIn(0, 250);
    }

    @Test
    public void test3() throws Exception {
        helpTestMaxIn(1, 1);
    }	
}
