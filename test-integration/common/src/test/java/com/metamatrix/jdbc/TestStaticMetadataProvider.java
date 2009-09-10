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

package com.metamatrix.jdbc;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.teiid.dqp.internal.process.MetaDataProcessor;

import com.metamatrix.dqp.metadata.ResultsMetadataConstants;

/**
 */
public class TestStaticMetadataProvider extends TestCase {

    /**
     * Constructor for TestStaticMetadataProvider.
     * @param name
     */
    public TestStaticMetadataProvider(String name) {
        super(name);
    }

    private StaticMetadataProvider example1() throws Exception {
        MetaDataProcessor processor = new MetaDataProcessor(null, null, null, null, null);
        Map[] columnMetadata = new Map[] { 
            processor.getDefaultColumn("vdb", "1", "table", "c1", String.class), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            processor.getDefaultColumn("vdb", "1", "table", "c2", Integer.class) //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        };               
        
        return StaticMetadataProvider.createWithData(columnMetadata, 0);
    }
    
    public void testMetadata() throws Exception {
        StaticMetadataProvider provider = example1();
        assertEquals(2, provider.getColumnCount());
        
        for(int i=0; i<provider.getColumnCount(); i++) {
            assertNotNull(provider.getValue(i, ResultsMetadataConstants.VIRTUAL_DATABASE_NAME));
            assertNotNull(provider.getValue(i, ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION));
            assertNotNull(provider.getValue(i, ResultsMetadataConstants.GROUP_NAME));
            assertNotNull(provider.getValue(i, ResultsMetadataConstants.ELEMENT_NAME));
            
        }
    }
    
    public void testGetStringValue() throws Exception {
        Integer property = ResultsMetadataConstants.VIRTUAL_DATABASE_NAME;
        String value = "vdb"; //$NON-NLS-1$
        
        Map columnMetadata = new HashMap();
        columnMetadata.put(property, value); 

        StaticMetadataProvider md = StaticMetadataProvider.createWithData(new Map[] {columnMetadata}, 0);
        
        String actualValue = md.getStringValue(0, property);
        assertEquals(value, actualValue);               
    }

    public void testGetIntValue() throws Exception {
        Integer property = ResultsMetadataConstants.VIRTUAL_DATABASE_NAME;
        Integer value = new Integer(10); //$NON-NLS-1$
        
        Map columnMetadata = new HashMap();
        columnMetadata.put(property, value); 

        StaticMetadataProvider md = StaticMetadataProvider.createWithData(new Map[] {columnMetadata}, 0);
        
        int actualValue = md.getIntValue(0, property);
        assertEquals(10, actualValue);               
    }

    public void testGetBooleanValue() throws Exception {
        Integer property = ResultsMetadataConstants.VIRTUAL_DATABASE_NAME;
        Boolean value = Boolean.TRUE; //$NON-NLS-1$
        
        Map columnMetadata = new HashMap();
        columnMetadata.put(property, value); 

        StaticMetadataProvider md = StaticMetadataProvider.createWithData(new Map[] {columnMetadata}, 0);
        
        boolean actualValue = md.getBooleanValue(0, property);
        assertEquals(true, actualValue);               
    }

}
