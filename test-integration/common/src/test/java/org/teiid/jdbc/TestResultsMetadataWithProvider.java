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

package org.teiid.jdbc;

import java.util.Map;

import org.teiid.dqp.internal.process.MetaDataProcessor;
import org.teiid.jdbc.ResultSetMetaDataImpl;
import org.teiid.jdbc.MetadataProvider;

import junit.framework.TestCase;


/**
 */
public class TestResultsMetadataWithProvider extends TestCase {

    /**
     * Constructor for TestResultsMetadataWithProvider.
     * @param name
     */
    public TestResultsMetadataWithProvider(String name) {
        super(name);
    }

    public MetadataProvider exampleProvider() throws Exception {
        MetaDataProcessor processor = new MetaDataProcessor(null, null, "vdb", 1); //$NON-NLS-1$  
        Map col1 = processor.getDefaultColumn("table", "col1", "col1Label", String.class); //$NON-NLS-1$ //$NON-NLS-2$ 
        Map col2 = processor.getDefaultColumn("table", "col2", "col2Label", Integer.class); //$NON-NLS-1$ //$NON-NLS-2$ 
        
        Map[] columnMetadata = new Map[] {
            col1, col2
        };
                
        MetadataProvider provider = new MetadataProvider(columnMetadata);                      
        return provider;        
    }
    
    public void test1() throws Exception {        
        ResultSetMetaDataImpl rmd = new ResultSetMetaDataImpl(exampleProvider(), null);
        
        assertEquals(false, rmd.isAutoIncrement(1));
        assertEquals(false, rmd.isCaseSensitive(1));
        assertEquals(false, rmd.isCurrency(1));
        assertEquals(true, rmd.isDefinitelyWritable(1));
        assertEquals(false, rmd.isReadOnly(1));
        assertEquals(true, rmd.isSearchable(1));
        assertEquals(true, rmd.isSigned(1));
        assertEquals(true, rmd.isWritable(1));
        assertEquals("vdb", rmd.getCatalogName(1)); //$NON-NLS-1$
        assertEquals(null, rmd.getSchemaName(1)); 
        assertEquals("table", rmd.getTableName(1)); //$NON-NLS-1$
        assertEquals("col1", rmd.getColumnName(1)); //$NON-NLS-1$
        assertEquals("col1Label", rmd.getColumnLabel(1)); //$NON-NLS-1$
        assertEquals("string", rmd.getColumnTypeName(1)); //$NON-NLS-1$
    }
    
    public void test2BackwardCompatibilityTest() throws Exception {        
        ResultSetMetaDataImpl rmd = new ResultSetMetaDataImpl(exampleProvider(), "false");
        
        assertEquals(false, rmd.isAutoIncrement(1));
        assertEquals(false, rmd.isCaseSensitive(1));
        assertEquals(false, rmd.isCurrency(1));
        assertEquals(true, rmd.isDefinitelyWritable(1));
        assertEquals(false, rmd.isReadOnly(1));
        assertEquals(true, rmd.isSearchable(1));
        assertEquals(true, rmd.isSigned(1));
        assertEquals(true, rmd.isWritable(1));
        assertEquals("vdb", rmd.getCatalogName(1)); //$NON-NLS-1$
        assertEquals(null, rmd.getSchemaName(1)); 
        assertEquals("table", rmd.getTableName(1)); //$NON-NLS-1$
        assertEquals("col1Label", rmd.getColumnName(1)); //$NON-NLS-1$
        assertEquals("col1Label", rmd.getColumnLabel(1)); //$NON-NLS-1$
        assertEquals("string", rmd.getColumnTypeName(1)); //$NON-NLS-1$
    }    
}
